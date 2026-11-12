// PSI source tests (T-162): the pressure file parser (some avg10/60/300,
// a full CPU file without full, extra lines, broken values),
// degradations (a kernel without PSI — errNoPSI, other read errors),
// ctx cancellation, the metrics wrapper.
package monitor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"crawler/internal/metrics"
)

// TestParsePSI — parsing a some line: three averages, total is skipped;
// a full line (memory/IO have it; CPU does not) does not pass; a missing
// some or broken fields are errors.
func TestParsePSI(t *testing.T) {
	st, err := parsePSI("some avg10=1.25 avg60=2.50 avg300=3.75 total=562500\n")
	if err != nil {
		t.Fatalf("parsePSI: %v", err)
	}
	if st.Avg10 != 1.25 || st.Avg60 != 2.5 || st.Avg300 != 3.75 {
		t.Errorf("the averages = %+v, want 1.25/2.5/3.75", st)
	}

	// A file with a full line (the kernel returns it for memory/IO —
	// the CPU file parser must skip it).
	st, err = parsePSI("full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n" +
		"some avg10=91.02 avg60=92.10 avg300=93.45 total=16815000000\n")
	if err != nil || st.Avg300 != 93.45 {
		t.Errorf("full+some: (%+v, %v), want some avg300=93.45", st, err)
	}

	// An empty file / only full / garbage — there is no some line.
	for _, bad := range []string{"", "full avg10=0 avg60=0 avg300=0 total=0\n", "garbage\n"} {
		if _, err := parsePSI(bad); err == nil || !strings.Contains(err.Error(), "some") {
			t.Errorf("%q: error = %v, want \"the some line is missing\"", bad, err)
		}
	}

	// Broken values: not a number, negative, >100, a missing field —
	// the format is fixed by the kernel, a malformed file must not be
	// silently ignored.
	for _, bad := range []string{
		"some avg10=x avg60=1 avg300=1 total=0\n",
		"some avg10=-1 avg60=1 avg300=1 total=0\n",
		"some avg10=101 avg60=1 avg300=1 total=0\n",
		"some avg60=1 avg300=1 total=0\n",
	} {
		if _, err := parsePSI(bad); err == nil {
			t.Errorf("%q: no error, want a format error", bad)
		}
	}
}

// TestOSPISource — reading by the given path; a missing file is errNoPSI
// (the quiet degradation class of the alert), other errors — as is; a ctx
// cancellation — before the file access.
func TestOSPISource(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "cpu")
	if err := os.WriteFile(file, []byte("some avg10=4.00 avg60=4.00 avg300=4.00 total=6000000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := (OSPSI{Path: file}).PSI(context.Background())
	if err != nil || st.Avg300 != 4 {
		t.Errorf("PSI over the file: (%+v, %v), want avg300=4", st, err)
	}

	// No file — a kernel without PSI: the quiet class (the alert degrades without a Warn).
	_, err = (OSPSI{Path: filepath.Join(dir, "missing")}).PSI(context.Background())
	if !errors.Is(err, errNoPSI) {
		t.Errorf("no file: error = %v, want errNoPSI", err)
	}

	// A directory instead of a file — another error (not errNoPSI).
	_, err = (OSPSI{Path: dir}).PSI(context.Background())
	if err == nil || errors.Is(err, errNoPSI) {
		t.Errorf("a directory instead of the file: error = %v, want a regular read error", err)
	}

	// A ctx cancellation — before the file access.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (OSPSI{Path: file}).PSI(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("a ctx cancellation: error = %v, want context.Canceled", err)
	}
}

// TestOSPSIDefaultPath — an empty Path reads the production
// /proc/pressure/cpu: on a CI host with PSI in the kernel the answer is
// valid (0–100, the values are not pinned — the host pressure changes
// between runs), without PSI — errNoPSI; the default convention is
// checked, not the values.
func TestOSPSIDefaultPath(t *testing.T) {
	st, err := (OSPSI{}).PSI(context.Background())
	if err != nil {
		if !errors.Is(err, errNoPSI) {
			t.Errorf("the default path: error = %v, want errNoPSI or a valid snapshot", err)
		}
		return
	}
	if st.Avg10 < 0 || st.Avg10 > 100 || st.Avg300 < 0 || st.Avg300 > 100 {
		t.Errorf("a snapshot outside 0–100: %+v", st)
	}
}

// TestPSIMetrics — the wrapper mirrors the averages into
// crawler_psi_cpu_some_percent {kind=avg10/60/300}, a source error — a
// clean error transit, the gauge keeps the last value (hold-last).
func TestPSIMetrics(t *testing.T) {
	reg := metrics.New("monitor")
	src := &fakePSI{psi: PSIStat{Avg10: 10, Avg60: 20, Avg300: 30}}
	m := NewPSIMetrics(reg, src)

	st, err := m.PSI(context.Background())
	if err != nil || st.Avg300 != 30 {
		t.Fatalf("PSI: (%+v, %v), want the snapshot transit", st, err)
	}
	for kind, want := range map[string]float64{"avg10": 10, "avg60": 20, "avg300": 30} {
		v, ok := gaugeLabels(t, reg, "crawler_psi_cpu_some_percent", map[string]string{"kind": kind})
		if !ok || v != want {
			t.Errorf("crawler_psi_cpu_some_percent{kind=%s} = (%v, %v), want %v", kind, v, ok, want)
		}
	}

	// A source error: the metrics are untouched, the error is returned.
	src.err = errors.New("failure")
	if _, err := m.PSI(context.Background()); err == nil {
		t.Error("the source error was swallowed")
	}
	v, ok := gaugeLabels(t, reg, "crawler_psi_cpu_some_percent", map[string]string{"kind": "avg300"})
	if !ok || v != 30 {
		t.Errorf("after the error avg300 = (%v, %v), want 30 (hold-last)", v, ok)
	}

	// A nil registry — pure transit.
	src.err = nil
	plain := NewPSIMetrics(nil, src)
	if st, err := plain.PSI(context.Background()); err != nil || st.Avg10 != 10 {
		t.Errorf("transit without a registry = (%+v, %v), want the source as is", st, err)
	}
}

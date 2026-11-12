// Unit tests for crawlerctl zones collapse (T-196): routing, flags and
// usage diagnostics without infrastructure — a valid invocation reaches
// the configuration and fails deterministically (exit code 1, the
// TestRunZonesSyncExecution style). The SQL and the move logic are
// covered by internal/collapse package tests (a fake connection) and
// test/integration (the live schema).
package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

// TestRunZonesCollapseExecution — the execution path: flag parsing up to
// the DB connection; a configuration failure — exit code 1; --skip-reconcile
// without --apply and extra arguments — exit code 2.
func TestRunZonesCollapseExecution(t *testing.T) {
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	cases := []runCase{
		{name: "collapse — no configuration", args: []string{"zones", "collapse"},
			code: exitError, inErr: "zones collapse: config:"},
		{name: "collapse --apply — no configuration", args: []string{"zones", "collapse", "--apply"},
			code: exitError, inErr: "zones collapse: config:"},
		{name: "collapse --force — no configuration", args: []string{"zones", "collapse", "--force"},
			code: exitError, inErr: "zones collapse: config:"},
		{name: "collapse --skip-reconcile without --apply", args: []string{"zones", "collapse", "--skip-reconcile"},
			code: exitUsage, inErr: "--skip-reconcile complements --apply"},
		{name: "collapse extra argument", args: []string{"zones", "collapse", "5g.in"},
			code: exitUsage, inErr: "extra arguments"},
		{name: "collapse unknown flag", args: []string{"zones", "collapse", "--nope"},
			code: exitUsage, inErr: "zones collapse:"},
		{name: "collapse --help", args: []string{"zones", "collapse", "--help"},
			code: exitOK, inOut: "move the 2LD zone data into TLD zones"},
		{name: "zones help mentions collapse", args: []string{"zones", "--help"},
			code: exitOK, inOut: "collapse [--apply]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tc.args, &stdout, &stderr)
			if code != tc.code {
				t.Errorf("run(%q) code = %d, want %d; stderr:\n%s", tc.args, code, tc.code, stderr.String())
			}
			if tc.inErr != "" && !strings.Contains(stderr.String(), tc.inErr) {
				t.Errorf("run(%q) stderr does not contain %q:\n%s", tc.args, tc.inErr, stderr.String())
			}
			if tc.inOut != "" && !strings.Contains(stdout.String(), tc.inOut) {
				t.Errorf("run(%q) stdout does not contain %q:\n%s", tc.args, tc.inOut, stdout.String())
			}
		})
	}
}

// The rule-set placement invariant (T-228, review P3-3): contenttest imports
// testing and therefore must only be in the test build. A non-test import
// of contenttest would quietly pull testing (and contenttest itself) into the
// production graph of the cmd binaries — lint, vet and tests would stay green.
// The guard is the only automated check of the "testing outside a non-test
// build" criterion; a manual check — go list -deps ./cmd/... | grep testing.
package contenttest

import (
	"os/exec"
	"strings"
	"testing"
)

// TestNotInProductionGraph — the production graph of all cmd binaries contains
// neither testing nor contenttest (the import-path pattern works from any
// directory inside the module — the test binary runs in the package
// directory).
func TestNotInProductionGraph(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skipf("the Go toolchain unavailable, the production graph check skipped: %v", err)
	}
	out, err := exec.Command("go", "list", "-deps", "crawler/cmd/...").Output()
	if err != nil {
		t.Fatalf("go list -deps crawler/cmd/...: %v", err)
	}
	var bad []string
	for _, pkg := range strings.Split(string(out), "\n") {
		switch pkg {
		case "testing", "crawler/internal/content/contenttest":
			bad = append(bad, pkg)
		}
	}
	if len(bad) > 0 {
		t.Errorf("the cmd production graph contains test packages %v — contenttest "+
			"must be imported from _test files only (T-228)", bad)
	}
}

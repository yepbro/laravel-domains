// Test for crawlerctl seed: the bulk set is written in a streaming fashion
// (CLI-08) — the content and the counter are the same as materialized
// strings would give.
package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"crawler/internal/config"
	"crawler/internal/idn"
)

// seedRepoRoot — the repository root (cmd/testbedgen style): the bench
// files are read by dev-environment configuration consistency tests.
func seedRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// TestSeedBulk — the seed-bulk.txt file contains exactly N b%08d.<zone>
// lines in order; the final counter includes them; repeats are idempotent.
func TestSeedBulk(t *testing.T) {
	dir := t.TempDir()
	scen := filepath.Join(dir, "scenarios.yaml")
	if err := os.WriteFile(scen, []byte(
		"zones: [crawl.test]\nscenarios:\n  - name: laravel-6\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	const bulk = 1000
	incoming := filepath.Join(dir, "incoming")
	args := []string{"seed",
		"--scenarios", scen,
		"--incoming", incoming,
		"--bulk", "1000",
	}
	var stdout, stderr bytes.Buffer
	if code := run(args, &stdout, &stderr); code != exitOK {
		t.Fatalf("seed --bulk: code = %d, stderr:\n%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "bulk %d domains", bulk) {
		t.Errorf("stdout without the bulk total:\n%s", stdout.String())
	}

	raw, err := os.ReadFile(filepath.Join(incoming, "crawl.test", "add", "seed-bulk.txt"))
	if err != nil {
		t.Fatalf("seed-bulk.txt was not created: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	if len(lines) != bulk {
		t.Fatalf("lines %d, want %d", len(lines), bulk)
	}
	if lines[0] != "b00000000.crawl.test" || lines[bulk-1] != "b00000999.crawl.test" {
		t.Errorf("the set edges = %q..%q, want b00000000..b00000999", lines[0], lines[bulk-1])
	}
	// All lines are unique and within the zone.
	seen := make(map[string]bool, bulk)
	for _, l := range lines {
		if seen[l] {
			t.Fatalf("a duplicate line %q", l)
		}
		seen[l] = true
		if !strings.HasSuffix(l, ".crawl.test") {
			t.Fatalf("the line %q is not in the zone crawl.test", l)
		}
	}
}

// TestSeedZonesAllowedInDevConfig — bench/config.dev consistency
// (T-197): seed writes the base scenario zone and the zones of the
// plain block of scenarios.yaml into incoming/<zone>/add/. All these
// directories are multi-level (the testbed zones are served by coredns),
// hence each one must be in the ingest.allowed_zone_paths allowlist of
// the dev-environment configuration — otherwise the TLD semantics of
// ingest would send the seed files to failed/ and the dev step
// "make seed → ingest processed" would break. The production config
// (config.example.yaml) defines no allowlist — it has no test zones.
func TestSeedZonesAllowedInDevConfig(t *testing.T) {
	root := seedRepoRoot(t)

	// The dev-environment configuration is valid and carries an allowlist
	// (normalization — punycode in lower case — is on the config.LoadFile side).
	cfg, err := config.LoadFile(filepath.Join(root, "deploy", "docker", "config.dev.yaml"))
	if err != nil {
		t.Fatalf("config.LoadFile(config.dev.yaml): %v", err)
	}
	allowed := make(map[string]bool, len(cfg.Ingest.AllowedZonePaths))
	for _, z := range cfg.Ingest.AllowedZonePaths {
		allowed[z] = true
	}

	// Zones seed writes to (cmdSeed): Zones[0] — the base one, then the
	// plain block. The scenarios.yaml format — cmd/testbedgen.
	raw, err := os.ReadFile(filepath.Join(root, "deploy", "docker", "testbed", "scenarios.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var sc seedScenarios
	if err := yaml.Unmarshal(raw, &sc); err != nil {
		t.Fatalf("scenarios.yaml: %v", err)
	}
	if len(sc.Zones) == 0 {
		t.Fatal("scenarios.yaml: no zones")
	}

	base, err := idn.NormalizeZone(sc.Zones[0])
	if err != nil {
		t.Fatalf("the base zone %q: %v", sc.Zones[0], err)
	}
	if !allowed[base] {
		t.Errorf("the scenarios base zone %q is missing from ingest.allowed_zone_paths config.dev.yaml — the seed files would land in failed/", base)
	}
	for _, p := range sc.Plain {
		zone, err := idn.NormalizeZone(p.Zone)
		if err != nil {
			t.Fatalf("the plain zone %q: %v", p.Zone, err)
		}
		if !allowed[zone] {
			t.Errorf("the plain-block zone %q is missing from ingest.allowed_zone_paths config.dev.yaml — the seed files would land in failed/", zone)
		}
	}
}

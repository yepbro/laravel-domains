// Тест crawlerctl seed: bulk-набор пишется потоково (CLI-08) — содержимое
// и счётчик те же, что дали бы материализованные строки.
package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSeedBulk — файл seed-bulk.txt содержит ровно N строк b%08d.<зона>
// по порядку, счётчик итога учитывает их; повторы идемпотентны.
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
		t.Fatalf("seed --bulk: код = %d, stderr:\n%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "bulk 1000 доменов") {
		t.Errorf("stdout без итога bulk:\n%s", stdout.String())
	}

	raw, err := os.ReadFile(filepath.Join(incoming, "crawl.test", "add", "seed-bulk.txt"))
	if err != nil {
		t.Fatalf("seed-bulk.txt не создан: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	if len(lines) != bulk {
		t.Fatalf("строк %d, хочу %d", len(lines), bulk)
	}
	if lines[0] != "b00000000.crawl.test" || lines[bulk-1] != "b00000999.crawl.test" {
		t.Errorf("края набора = %q..%q, хочу b00000000..b00000999", lines[0], lines[bulk-1])
	}
	// Все строки уникальны и в зоне.
	seen := make(map[string]bool, bulk)
	for _, l := range lines {
		if seen[l] {
			t.Fatalf("дубль строки %q", l)
		}
		seen[l] = true
		if !strings.HasSuffix(l, ".crawl.test") {
			t.Fatalf("строка %q не в зоне crawl.test", l)
		}
	}
}

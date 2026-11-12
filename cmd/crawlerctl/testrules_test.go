// Test for crawlerctl test-rules: the is_laravel threshold is kept in sync
// with analyzer.laravel_min_score from the configuration (CLI-06) — an
// explicit flag takes precedence; without a configuration it stays 0.6.
package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testRulesCorpus — a minimal corpus: one html/Laravel rule with weight
// 0.7 and one positive sample. Score 0.7 passes the 0.6 threshold
// and fails 0.9 — distinguishing the threshold sources.
func testRulesCorpus(t *testing.T) (rulesDir, samplesDir string) {
	t.Helper()
	root := t.TempDir()
	rulesDir, samplesDir = filepath.Join(root, "rules"), filepath.Join(root, "samples")
	if err := os.MkdirAll(rulesDir, 0o755); err != nil {
		t.Fatal(err)
	}
	rule := "rules:\n" +
		"  - id: html_laravel\n" +
		"    target: html\n" +
		"    regex: Laravel\n" +
		"    weight: 0.7\n"
	if err := os.WriteFile(filepath.Join(rulesDir, "test.yaml"), []byte(rule), 0o644); err != nil {
		t.Fatal(err)
	}
	sample := filepath.Join(samplesDir, "positive")
	if err := os.MkdirAll(sample, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sample, "index.html"), []byte("<html>Laravel</html>"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sample, "meta.yaml"), []byte("is_laravel: true\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return rulesDir, samplesDir
}

// testRulesConfig — a minimal valid configuration with the given threshold
// (internal/config defaults fill in the rest; only the DSN is required).
func testRulesConfig(t *testing.T, minScore string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	yaml := "postgres:\n" +
		"  dsn: postgres://test:test@127.0.0.1:5432/test?sslmode=disable\n" +
		"analyzer:\n" +
		"  laravel_min_score: " + minScore + "\n"
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestRunTestRulesMinScore — the is_laravel threshold: the default is
// taken from analyzer.laravel_min_score, an explicit flag overrides the
// configuration, without a configuration it is 0.6 (a local offline run).
// A sample with score 0.7: OK at threshold 0.6, FAIL at 0.9.
func TestRunTestRulesMinScore(t *testing.T) {
	rulesDir, samplesDir := testRulesCorpus(t)

	runCorpus := func(cfg string, extra ...string) (int, string) {
		t.Helper()
		if cfg == "" {
			t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))
		} else {
			t.Setenv("CRAWLER_CONFIG", cfg)
		}
		args := append([]string{"test-rules", "--rules", rulesDir, "--samples", samplesDir}, extra...)
		var stdout, stderr bytes.Buffer
		code := run(args, &stdout, &stderr)
		return code, stdout.String() + stderr.String()
	}

	// Default — the threshold from the configuration: 0.9 is above score 0.7 → FAIL.
	code, out := runCorpus(testRulesConfig(t, "0.9"))
	if code != exitError || !strings.Contains(out, "порог 0.90") || !strings.Contains(out, "FAIL") {
		t.Errorf("конфиг 0.9 без флага: код = %d, хочу exitError с порогом 0.90 и FAIL:\n%s", code, out)
	}

	// An explicit flag takes precedence over the configuration: 0.6 → OK.
	code, out = runCorpus(testRulesConfig(t, "0.9"), "--min-score", "0.6")
	if code != exitOK || !strings.Contains(out, "порог 0.60") || !strings.Contains(out, "OK") {
		t.Errorf("флаг 0.6 при конфиге 0.9: код = %d, хочу exitOK с порогом 0.60:\n%s", code, out)
	}

	// No configuration (a local run without the stack) — the flag default 0.6.
	code, out = runCorpus("")
	if code != exitOK || !strings.Contains(out, "порог 0.60") || !strings.Contains(out, "OK") {
		t.Errorf("без конфигурации: код = %d, хочу exitOK с порогом 0.60 (дефолт):\n%s", code, out)
	}
}

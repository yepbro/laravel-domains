// Тест crawlerctl test-rules: порог is_laravel синхронизирован с
// analyzer.laravel_min_score из конфигурации (CLI-06) — явный флаг важнее,
// без конфигурации остаётся 0.6.
package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testRulesCorpus — минимальный корпус: одно правило html/Laravel весом
// 0.7 и один положительный образец. Score 0.7 проходит порог 0.6
// и не проходит 0.9 — различение источников порога.
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

// testRulesConfig — минимальная валидная конфигурация с заданным порогом
// (остальное заполняют дефолты internal/config, обязателен только DSN).
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

// TestRunTestRulesMinScore — порог is_laravel: дефолт берётся из
// analyzer.laravel_min_score, явный флаг переопределяет конфигурацию,
// без конфигурации — 0.6 (локальный офлайн-прогон). Образец score 0.7:
// OK при пороге 0.6, FAIL при 0.9.
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

	// Дефолт — порог из конфигурации: 0.9 выше score 0.7 → FAIL.
	code, out := runCorpus(testRulesConfig(t, "0.9"))
	if code != exitError || !strings.Contains(out, "порог 0.90") || !strings.Contains(out, "FAIL") {
		t.Errorf("конфиг 0.9 без флага: код = %d, хочу exitError с порогом 0.90 и FAIL:\n%s", code, out)
	}

	// Явный флаг важнее конфигурации: 0.6 → OK.
	code, out = runCorpus(testRulesConfig(t, "0.9"), "--min-score", "0.6")
	if code != exitOK || !strings.Contains(out, "порог 0.60") || !strings.Contains(out, "OK") {
		t.Errorf("флаг 0.6 при конфиге 0.9: код = %d, хочу exitOK с порогом 0.60:\n%s", code, out)
	}

	// Конфигурации нет (локальный прогон без стека) — дефолт флага 0.6.
	code, out = runCorpus("")
	if code != exitOK || !strings.Contains(out, "порог 0.60") || !strings.Contains(out, "OK") {
		t.Errorf("без конфигурации: код = %d, хочу exitOK с порогом 0.60 (дефолт):\n%s", code, out)
	}
}

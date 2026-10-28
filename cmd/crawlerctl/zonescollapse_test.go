// Юнит-тесты crawlerctl zones collapse (T-196): роутинг, флаги и
// диагностика использования без инфраструктуры — валидный вызов доходит
// до конфигурации и падает детерминированно (код 1, стиль
// TestRunZonesSyncExecution). SQL и логика переноса — тесты пакета
// internal/collapse (фейковое соединение) и test/integration (живая
// схема).
package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

// TestRunZonesCollapseExecution — путь выполнения: разбор флагов до
// соединения с БД; отказ конфигурации — код 1; --skip-reconcile без
// --apply и лишние аргументы — код 2.
func TestRunZonesCollapseExecution(t *testing.T) {
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	cases := []runCase{
		{name: "collapse — нет конфигурации", args: []string{"zones", "collapse"},
			code: exitError, inErr: "zones collapse: config:"},
		{name: "collapse --apply — нет конфигурации", args: []string{"zones", "collapse", "--apply"},
			code: exitError, inErr: "zones collapse: config:"},
		{name: "collapse --force — нет конфигурации", args: []string{"zones", "collapse", "--force"},
			code: exitError, inErr: "zones collapse: config:"},
		{name: "collapse --skip-reconcile без --apply", args: []string{"zones", "collapse", "--skip-reconcile"},
			code: exitUsage, inErr: "--skip-reconcile дополняет --apply"},
		{name: "collapse лишний аргумент", args: []string{"zones", "collapse", "5g.in"},
			code: exitUsage, inErr: "лишние аргументы"},
		{name: "collapse неизвестный флаг", args: []string{"zones", "collapse", "--nope"},
			code: exitUsage, inErr: "zones collapse:"},
		{name: "collapse --help", args: []string{"zones", "collapse", "--help"},
			code: exitOK, inOut: "перенести данные 2LD-зон в TLD-зоны"},
		{name: "zones help упоминает collapse", args: []string{"zones", "--help"},
			code: exitOK, inOut: "collapse [--apply]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tc.args, &stdout, &stderr)
			if code != tc.code {
				t.Errorf("run(%q) код = %d, хочу %d; stderr:\n%s", tc.args, code, tc.code, stderr.String())
			}
			if tc.inErr != "" && !strings.Contains(stderr.String(), tc.inErr) {
				t.Errorf("run(%q) stderr не содержит %q:\n%s", tc.args, tc.inErr, stderr.String())
			}
			if tc.inOut != "" && !strings.Contains(stdout.String(), tc.inOut) {
				t.Errorf("run(%q) stdout не содержит %q:\n%s", tc.args, tc.inOut, stdout.String())
			}
		})
	}
}

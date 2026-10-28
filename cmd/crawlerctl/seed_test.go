// Тест crawlerctl seed: bulk-набор пишется потоково (CLI-08) — содержимое
// и счётчик те же, что дали бы материализованные строки.
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

// seedRepoRoot — корень репозитория (стиль cmd/testbedgen): файлы стенда
// читаются тестами согласованности конфигурации dev-окружения.
func seedRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

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

// TestSeedZonesAllowedInDevConfig — согласованность стенда и конфига dev
// (Т-197): seed пишет в incoming/<зона>/add/ базовую зону сценариев и зоны
// plain-блока scenarios.yaml. Все эти каталоги многоуровневые (зоны testbed
// обслуживает coredns), значит каждый обязан входить в allowlist
// ingest.allowed_zone_paths конфига dev-окружения — иначе TLD-семантика
// приёма отправит файлы seed в failed/ и dev-шаг «make seed → ingest
// обработал» сломается. Продуктовый конфиг (config.example.yaml) allowlist
// не задаёт — там тестовых зон нет.
func TestSeedZonesAllowedInDevConfig(t *testing.T) {
	root := seedRepoRoot(t)

	// Конфиг dev-окружения валиден и несёт allowlist (нормализация —
	// punycode нижним регистром — на стороне config.LoadFile).
	cfg, err := config.LoadFile(filepath.Join(root, "deploy", "docker", "config.dev.yaml"))
	if err != nil {
		t.Fatalf("config.LoadFile(config.dev.yaml): %v", err)
	}
	allowed := make(map[string]bool, len(cfg.Ingest.AllowedZonePaths))
	for _, z := range cfg.Ingest.AllowedZonePaths {
		allowed[z] = true
	}

	// Зоны, в которые пишет seed (cmdSeed): Zones[0] — базовая, далее —
	// plain-блок. Формат scenarios.yaml — cmd/testbedgen.
	raw, err := os.ReadFile(filepath.Join(root, "deploy", "docker", "testbed", "scenarios.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var sc seedScenarios
	if err := yaml.Unmarshal(raw, &sc); err != nil {
		t.Fatalf("scenarios.yaml: %v", err)
	}
	if len(sc.Zones) == 0 {
		t.Fatal("scenarios.yaml: нет зон")
	}

	base, err := idn.NormalizeZone(sc.Zones[0])
	if err != nil {
		t.Fatalf("базовая зона %q: %v", sc.Zones[0], err)
	}
	if !allowed[base] {
		t.Errorf("базовая зона сценариев %q отсутствует в ingest.allowed_zone_paths config.dev.yaml — файлы seed уйдут в failed/", base)
	}
	for _, p := range sc.Plain {
		zone, err := idn.NormalizeZone(p.Zone)
		if err != nil {
			t.Fatalf("зона plain %q: %v", p.Zone, err)
		}
		if !allowed[zone] {
			t.Errorf("зона plain-блока %q отсутствует в ingest.allowed_zone_paths config.dev.yaml — файлы seed уйдут в failed/", zone)
		}
	}
}

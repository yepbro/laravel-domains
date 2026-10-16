// ---------------------------------------------------------------- seed
//
// Команда dev-окружения: тестовые списки доменов в incoming (scenarios.yaml
// testbed, «плохие» строки для rejected-лога, синтетический набор для
// замеров). Реализована полностью; вызов — make seed / make seed-bulk.
// Имена зон и доменов нормализуются пакетом internal/idn — единый конвейер
// проекта (ТЗ 5.2), а не локальный вызов idna.

package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"crawler/internal/idn"
)

// seedHelp — справка crawlerctl seed --help.
const seedHelp = `crawlerctl seed — тестовые списки доменов для dev-окружения

Использование:
  crawlerctl seed [--incoming DIR] [--scenarios FILE] [--bulk N]

Пишет в incoming/<зона>/add/ домены сценариев testbed (scenarios.yaml),
«плохие» строки для проверки rejected-лога ingest и (с --bulk) большой
синтетический набор для замеров. Запись идемпотентна: повторный вызов
перезаписывает файлы.
`

type seedScenarios struct {
	Zones []string `yaml:"zones"`
	Plain []struct {
		Zone  string `yaml:"zone"`
		Names []struct {
			Label string `yaml:"label"`
		} `yaml:"names"`
	} `yaml:"plain"`
	Scenarios []struct {
		Name string `yaml:"name"`
		Zone string `yaml:"zone"`
	} `yaml:"scenarios"`
}

func cmdSeed(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("seed")
	incoming := fs.String("incoming", "/data/incoming", "каталог incoming")
	scenariosFile := fs.String("scenarios", "deploy/docker/testbed/scenarios.yaml", "файл сценариев testbed")
	bulk := fs.Int("bulk", 0, "сгенерировать N синтетических доменов (seed-bulk)")
	if handled, err := parseFlags(fs, seedHelp, args, stdout); handled {
		return err
	}

	raw, err := os.ReadFile(*scenariosFile)
	if err != nil {
		return fmt.Errorf("seed: %w", err)
	}
	var sc seedScenarios
	if err := yaml.Unmarshal(raw, &sc); err != nil {
		return fmt.Errorf("seed: scenarios.yaml: %w", err)
	}
	if len(sc.Zones) == 0 {
		return fmt.Errorf("seed: в сценариях нет зон")
	}
	baseZone, err := idn.NormalizeZone(sc.Zones[0])
	if err != nil {
		return fmt.Errorf("seed: базовая зона: %w", err)
	}

	total := 0

	// 1. Домены сценариев testbed — основная зона.
	var lines []string
	for _, s := range sc.Scenarios {
		zone := baseZone
		if s.Zone != "" {
			if zone, err = idn.NormalizeZone(s.Zone); err != nil {
				return fmt.Errorf("seed: зона сценария %s: %w", s.Zone, err)
			}
		}
		full, err := idn.HostToASCII(s.Name + "." + zone)
		if err != nil {
			return fmt.Errorf("seed: домен сценария %s: %w", s.Name, err)
		}
		lines = append(lines, full)
	}
	n, err := writeFile(filepath.Join(*incoming, baseZone, "add", "seed.txt"), lines)
	if err != nil {
		return err
	}
	total += n

	// 2. Простые домены дополнительных зон (в unicode-имена каталогов зон
	// кладём и unicode-, и punycode-формы — проверка нормализации каталогов).
	for _, p := range sc.Plain {
		var zoneLines []string
		for _, nm := range p.Names {
			full, err := idn.HostToASCII(nm.Label + "." + p.Zone)
			if err != nil {
				return fmt.Errorf("seed: домен %s: %w", nm.Label, err)
			}
			zoneLines = append(zoneLines, nm.Label+"."+p.Zone, full) // обе формы
		}
		// каталог зоны — в исходной (unicode) форме, как приходит от заказчика
		n, err := writeFile(filepath.Join(*incoming, p.Zone, "add", "seed.txt"), zoneLines)
		if err != nil {
			return err
		}
		total += n
	}

	// 3. «Плохие» строки для проверки отклонения (ingest).
	bad := []string{
		"# невалидные строки: ingest должен отклонить их с указанием причины",
		"не-из-этой-зоны.ru",
		"-leading-dash." + baseZone,
		"trailing-dash-." + baseZone,
		"..empty-label." + baseZone,
		strings.Repeat("a", 64) + "." + baseZone,  // метка > 63
		strings.Repeat("b", 250) + "." + baseZone, // полное имя > 253
		"пробел внутри." + baseZone,
		"xn--." + baseZone, // некорректный punycode
	}
	n, err = writeFile(filepath.Join(*incoming, baseZone, "add", "seed-bad.txt"), bad)
	if err != nil {
		return err
	}
	total += n

	// 4. Большой синтетический набор для замеров (без обращения к сети).
	if *bulk > 0 {
		var bulkLines []string
		for i := 0; i < *bulk; i++ {
			bulkLines = append(bulkLines, fmt.Sprintf("b%08d.%s", i, baseZone))
		}
		n, err := writeFile(filepath.Join(*incoming, baseZone, "add", "seed-bulk.txt"), bulkLines)
		if err != nil {
			return err
		}
		total += n
		out(stdout, "seed: bulk %d доменов → %s\n", *bulk, filepath.Join(*incoming, baseZone, "add", "seed-bulk.txt"))
	}

	// Каталоги processed/failed рядом с incoming.
	for _, d := range []string{"processed", "failed"} {
		if err := os.MkdirAll(filepath.Join(*incoming, d), 0o755); err != nil {
			return err
		}
	}

	out(stdout, "seed: готово, строк записано: %d (каталог %s)\n", total, *incoming)
	return nil
}

// writeFile записывает файл списком строк; существующий перезаписывается
// (идемпотентность повторного make seed).
func writeFile(path string, lines []string) (int, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, err
	}
	var b strings.Builder
	for _, l := range lines {
		b.WriteString(l)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		return 0, err
	}
	return len(lines), nil
}

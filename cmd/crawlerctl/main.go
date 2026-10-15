// crawlerctl — CLI управления Domain Crawler (ТЗ 4.1 основного ТЗ).
//
// На этапе настройки окружения реализованы команды, полезные для dev-
// инфраструктуры: seed, test-rules, domain (нормализация). Команды status и
// reanalyze — заглушки: наполняются по мере реализации сервисов.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/net/idna"
	"gopkg.in/yaml.v3"

	"crawler/internal/rules"
)

func usage() {
	fmt.Fprint(os.Stderr, `crawlerctl — CLI Domain Crawler

Использование:
  crawlerctl seed [--incoming DIR] [--scenarios FILE] [--bulk N]
  crawlerctl test-rules --rules DIR --samples DIR
  crawlerctl domain <имя> [--json]
  crawlerctl status
  crawlerctl reanalyze [--zone ИМЯ]
`)
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "seed":
		err = cmdSeed(os.Args[2:])
	case "test-rules":
		err = cmdTestRules(os.Args[2:])
	case "domain":
		err = cmdDomain(os.Args[2:])
	case "status":
		err = cmdStatus()
	case "reanalyze":
		err = cmdReanalyze(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "crawlerctl: %v\n", err)
		os.Exit(1)
	}
}

// ---------------------------------------------------------------- seed

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

func cmdSeed(args []string) error {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	incoming := fs.String("incoming", "/data/incoming", "каталог incoming")
	scenariosFile := fs.String("scenarios", "deploy/docker/testbed/scenarios.yaml", "файл сценариев testbed")
	bulk := fs.Int("bulk", 0, "сгенерировать N синтетических доменов (seed-bulk)")
	if err := fs.Parse(args); err != nil {
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
	baseZone, err := idna.Lookup.ToASCII(sc.Zones[0])
	if err != nil {
		return fmt.Errorf("seed: базовая зона: %w", err)
	}

	total := 0

	// 1. Домены сценариев testbed — основная зона.
	var lines []string
	for _, s := range sc.Scenarios {
		zone := baseZone
		if s.Zone != "" {
			if zone, err = idna.Lookup.ToASCII(s.Zone); err != nil {
				return fmt.Errorf("seed: зона сценария %s: %w", s.Zone, err)
			}
		}
		full, err := idna.Lookup.ToASCII(s.Name + "." + zone)
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
			full, err := idna.Lookup.ToASCII(nm.Label + "." + p.Zone)
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
		fmt.Printf("seed: bulk %d доменов → %s\n", *bulk, filepath.Join(*incoming, baseZone, "add", "seed-bulk.txt"))
	}

	// Каталоги processed/failed рядом с incoming.
	for _, d := range []string{"processed", "failed"} {
		if err := os.MkdirAll(filepath.Join(*incoming, d), 0o755); err != nil {
			return err
		}
	}

	fmt.Printf("seed: готово, строк записано: %d (каталог %s)\n", total, *incoming)
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

// ---------------------------------------------------------------- test-rules

func cmdTestRules(args []string) error {
	fs := flag.NewFlagSet("test-rules", flag.ExitOnError)
	rulesDir := fs.String("rules", "/rules", "каталог правил")
	samplesDir := fs.String("samples", "/testdata/laravel-samples", "каталог образцов")
	minScore := fs.Float64("min-score", 0.6, "порог is_laravel")
	if err := fs.Parse(args); err != nil {
		return err
	}

	set, err := rules.LoadDir(*rulesDir)
	if err != nil {
		return err
	}
	samples, err := rules.LoadSamples(*samplesDir)
	if err != nil {
		return fmt.Errorf("корпус образцов недоступен: %w", err)
	}
	if len(samples) == 0 {
		fmt.Println("test-rules: корпус образцов пуст — нечего проверять (ok)")
		return nil
	}

	fmt.Printf("test-rules: набор правил %s (%d правил), образцов: %d, порог %.2f\n\n",
		set.Version, len(set.Rules), len(samples), *minScore)

	failed := 0
	for _, s := range samples {
		res := set.Evaluate(s.Page())
		isLaravel := res.Score >= *minScore

		status := "OK  "
		if s.Expect.IsLaravel != nil && *s.Expect.IsLaravel != isLaravel {
			status = "FAIL"
			failed++
		}
		if s.Expect.VerMin != "" || s.Expect.VerMax != "" {
			if !res.Contains(s.Expect.VerMin, s.Expect.VerMax) {
				status = "FAIL"
				failed++
			}
		}

		ver := fmt.Sprintf("[%s..%s]", orDash(res.VerMin), orDash(res.VerMax))
		if res.Conflict {
			ver = "[конфликт]"
		}
		fmt.Printf("%s %-28s laravel=%-5v score=%.3f ver=%-14s правил=%d\n",
			status, s.ID, isLaravel, res.Score, ver, len(res.Signals))
		for _, ev := range res.Signals {
			fmt.Printf("     - %-26s w=%.2f  %s\n", ev.Rule, ev.Weight, ev.Evidence)
		}
	}

	fmt.Printf("\nтестов: %d, ошибок: %d\n", len(samples), failed)
	if failed > 0 {
		return fmt.Errorf("test-rules: %d несовпадений с ожиданиями meta.yaml", failed)
	}
	return nil
}

func orDash(s string) string {
	if s == "" {
		return "?"
	}
	return s
}

// ---------------------------------------------------------------- domain

func cmdDomain(args []string) error {
	fs := flag.NewFlagSet("domain", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "машинный вывод")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("укажите имя домена: crawlerctl domain example.crawl.test")
	}

	// Нормализация по правилам 5.2 основного ТЗ (пока без списка суффиксов).
	name := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(fs.Arg(0), ".")))
	ascii, err := idna.Lookup.ToASCII(name)
	if err != nil {
		return fmt.Errorf("некорректное имя домена %q: %w", name, err)
	}
	unicode, _ := idna.Display.ToUnicode(ascii)

	if *asJSON {
		fmt.Printf("{\"ascii\":%q,\"unicode\":%q}\n", ascii, unicode)
		return nil
	}

	fmt.Printf("домен:      %s\npunycode:    %s\nunicode:     %s\n\n", name, ascii, unicode)
	fmt.Println("статус и признаки Laravel: будет доступно после реализации")
	fmt.Println("сервисов основного ТЗ (запись результатов в PostgreSQL).")
	return nil
}

// ---------------------------------------------------------------- status / reanalyze

func cmdStatus() error {
	fmt.Println(`crawlerctl status: сводка появится после реализации планировщика и monitor.

Пока можно смотреть:
  make ps                    — состояние контейнеров
  make psql                  — консоль PostgreSQL (таблицы zones/domains)
  make redis-cli             — консоль Redis (XLEN q:dns и т.п.)
  http://127.0.0.1:9090      — Prometheus (профиль monitoring: make up && docker compose --profile monitoring up -d)`)
	return nil
}

func cmdReanalyze(args []string) error {
	fs := flag.NewFlagSet("reanalyze", flag.ExitOnError)
	zone := fs.String("zone", "", "переанализ по зоне (пусто — все)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	_ = zone
	fmt.Println("crawlerctl reanalyze: будет реализован вместе с analyzer (ТЗ 5.8).")
	fmt.Println("Правила можно обновлять уже сейчас: правьте rules/*.yaml и выполните make rules-reload.")
	return nil
}

// ---------------------------------------------------------------- test-rules
//
// Команда dev-окружения: прогон правил Laravel по корпусу образцов
// testdata/laravel-samples без сети; сверка is_laravel/диапазона версий с
// meta.yaml. Реализована полностью; вызов — make test-rules.

package main

import (
	"fmt"
	"io"
	"slices"
	"sort"

	"crawler/internal/config"
	"crawler/internal/rules"
)

// testRulesHelp — справка crawlerctl test-rules --help.
const testRulesHelp = `crawlerctl test-rules — прогон правил Laravel по корпусу образцов

Использование:
  crawlerctl test-rules [--rules DIR] [--samples DIR] [--min-score F]

Оценивает каждую страницу из samples правилами из rules и сверяет
is_laravel (score >= min-score), диапазон версий и состав сработавших
правил с ожиданиями meta.yaml. Несовпадения — ошибка выполнения (код 1).

Порог is_laravel по умолчанию — analyzer.laravel_min_score из конфигурации
(боевой порог записи в laravel_domains, ТЗ 5.8/13.1): офлайн-проверка
калибруется тем же порогом, что и analyzer; без конфигурации — 0.6.
Явный --min-score переопределяет оба источника.
`

func cmdTestRules(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("test-rules")
	rulesDir := fs.String("rules", "/rules", "каталог правил")
	samplesDir := fs.String("samples", "/testdata/laravel-samples", "каталог образцов")
	minScore := fs.Float64("min-score", 0.6,
		"порог is_laravel (по умолчанию analyzer.laravel_min_score из конфигурации, без неё 0.6)")
	if handled, err := parseFlags(fs, testRulesHelp, args, stdout); handled {
		return err
	}
	// Дефолт порога синхронизирован с боевой конфигурацией (CLI-06):
	// явно заданный флаг важнее, недоступная конфигурация (локальный
	// прогон без стека) оставляет 0.6 — команда остаётся офлайн-инструментом.
	if !flagExplicit(fs, "min-score") {
		if cfg, err := config.Load(); err == nil {
			*minScore = cfg.Analyzer.LaravelMinScore
		}
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
		out(stdout, "test-rules: корпус образцов пуст — нечего проверять (ok)\n")
		return nil
	}

	out(stdout, "test-rules: набор правил v%d (%d правил), образцов: %d, порог %.2f\n\n",
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
		if s.Expect.Rules != nil {
			fired := make([]string, 0, len(res.Signals))
			for _, ev := range res.Signals {
				fired = append(fired, ev.Rule)
			}
			sort.Strings(fired)
			want := slices.Clone(s.Expect.Rules)
			sort.Strings(want)
			if !slices.Equal(fired, want) {
				status = "FAIL"
				failed++
				out(stdout, "     ! правила: в meta.yaml %v, сработали %v\n", want, fired)
			}
		}

		ver := fmt.Sprintf("[%s..%s]", orDash(res.VerMin), orDash(res.VerMax))
		if res.Conflict {
			ver = "[конфликт]"
		}
		out(stdout, "%s %-28s laravel=%-5v score=%.3f ver=%-14s правил=%d\n",
			status, s.ID, isLaravel, res.Score, ver, len(res.Signals))
		for _, ev := range res.Signals {
			out(stdout, "     - %-26s w=%.2f  %s\n", ev.Rule, ev.Weight, ev.Evidence)
		}
	}

	out(stdout, "\nтестов: %d, ошибок: %d\n", len(samples), failed)
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

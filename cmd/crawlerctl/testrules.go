// ---------------------------------------------------------------- test-rules
//
// A dev-environment command: runs the Laravel rules against the sample
// corpus testdata/laravel-samples without network; cross-checks
// is_laravel/version range against meta.yaml. Fully implemented;
// invoked via make test-rules.

package main

import (
	"fmt"
	"io"
	"slices"
	"sort"

	"crawler/internal/config"
	"crawler/internal/rules"
)

// testRulesHelp — the crawlerctl test-rules --help text.
const testRulesHelp = `crawlerctl test-rules — a run of the Laravel rules over the sample corpus

Usage:
  crawlerctl test-rules [--rules DIR] [--samples DIR] [--min-score F]

Evaluates every page from samples with the rules from rules and cross-checks
is_laravel (score >= min-score), the version range and the set of fired
rules against the meta.yaml expectations. Mismatches — an execution error
(exit code 1).

The default is_laravel threshold is analyzer.laravel_min_score from the
configuration (the production threshold for writing into laravel_domains,
spec 5.8/13.1): the offline check is calibrated by the same threshold as the
analyzer; without the configuration — 0.6. An explicit --min-score overrides
both sources.
`

func cmdTestRules(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("test-rules")
	rulesDir := fs.String("rules", "/rules", "the rules directory")
	samplesDir := fs.String("samples", "/testdata/laravel-samples", "the samples directory")
	minScore := fs.Float64("min-score", 0.6,
		"the is_laravel threshold (default analyzer.laravel_min_score from the configuration, 0.6 without it)")
	if handled, err := parseFlags(fs, testRulesHelp, args, stdout); handled {
		return err
	}
	// The threshold default is kept in sync with the production
	// configuration (CLI-06): an explicitly set flag takes precedence;
	// an unavailable configuration (a local run without the stack)
	// leaves 0.6 in place — the command stays an offline tool.
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
		return fmt.Errorf("the sample corpus is unavailable: %w", err)
	}
	if len(samples) == 0 {
		out(stdout, "test-rules: the sample corpus is empty — nothing to check (ok)\n")
		return nil
	}

	out(stdout, "test-rules: ruleset v%d (%d rules), samples: %d, threshold %.2f\n\n",
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
				out(stdout, "     ! rules: in meta.yaml %v, fired %v\n", want, fired)
			}
		}

		ver := fmt.Sprintf("[%s..%s]", orDash(res.VerMin), orDash(res.VerMax))
		if res.Conflict {
			ver = "[conflict]"
		}
		out(stdout, "%s %-28s laravel=%-5v score=%.3f ver=%-14s rules=%d\n",
			status, s.ID, isLaravel, res.Score, ver, len(res.Signals))
		for _, ev := range res.Signals {
			out(stdout, "     - %-26s w=%.2f  %s\n", ev.Rule, ev.Weight, ev.Evidence)
		}
	}

	out(stdout, "\ntests: %d, failures: %d\n", len(samples), failed)
	if failed > 0 {
		return fmt.Errorf("test-rules: %d mismatches against the meta.yaml expectations", failed)
	}
	return nil
}

func orDash(s string) string {
	if s == "" {
		return "?"
	}
	return s
}

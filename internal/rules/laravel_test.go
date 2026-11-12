package rules

// Running the production rule set rules/laravel.yaml over the sample corpus
// testdata/laravel-samples (spec §11.1, §13.3): the is_laravel threshold of
// 0.6 (§13.1), the expected version range and the complete set of hit rules
// are fixed in each sample's meta.yaml. The same check outside unit tests
// is performed by crawlerctl test-rules (make test-rules).

import (
	"os"
	"slices"
	"sort"
	"testing"
)

const (
	laravelRulesDir   = `../../rules`
	laravelSamplesDir = `../../testdata/laravel-samples`
	laravelThreshold  = 0.6 // the is_laravel threshold (spec §5.8, §13.1)
)

func TestLaravelRulesetOnCorpus(t *testing.T) {
	if _, err := os.Stat(laravelSamplesDir); err != nil {
		t.Skipf("корпус образцов недоступен: %v", err)
	}
	set, err := LoadDir(laravelRulesDir)
	if err != nil {
		t.Fatalf("набор правил не загружается: %v", err)
	}
	samples, err := LoadSamples(laravelSamplesDir)
	if err != nil {
		t.Fatalf("корпус не загружается: %v", err)
	}

	// The corpus must contain both positive and negative samples (spec §13.3).
	var positives, negatives int
	for _, s := range samples {
		if s.Expect.IsLaravel != nil {
			if *s.Expect.IsLaravel {
				positives++
			} else {
				negatives++
			}
		}
	}
	if positives < 5 || negatives < 4 {
		t.Fatalf("корпус подозрительно мал: позитивных %d (нужно >=5), негативных %d (нужно >=4)",
			positives, negatives)
	}

	for _, s := range samples {
		t.Run(s.ID, func(t *testing.T) {
			res := set.Evaluate(s.Page())

			if res.Conflict {
				t.Errorf("неожиданный конфликт диапазонов версий: [%s..%s]", res.VerMin, res.VerMax)
			}
			if isLaravel := res.Score >= laravelThreshold; s.Expect.IsLaravel != nil && isLaravel != *s.Expect.IsLaravel {
				t.Errorf("is_laravel = %v (score %.3f), ожидалось %v", isLaravel, res.Score, *s.Expect.IsLaravel)
			}
			if !res.Contains(s.Expect.VerMin, s.Expect.VerMax) {
				t.Errorf("диапазон [%s..%s] не покрывает ожидание [%s..%s]",
					orQ(res.VerMin), orQ(res.VerMax), orQ(s.Expect.VerMin), orQ(s.Expect.VerMax))
			}

			// The complete set of hit rules: rules: [] — nothing may hit.
			if s.Expect.Rules != nil {
				fired := make([]string, 0, len(res.Signals))
				for _, ev := range res.Signals {
					fired = append(fired, ev.Rule)
				}
				sort.Strings(fired)
				want := slices.Clone(s.Expect.Rules)
				sort.Strings(want)
				if !slices.Equal(fired, want) {
					t.Errorf("сработали %v, в meta.yaml %v", fired, want)
				}
			}
		})
	}
}

func TestLaravelRulesetNoFalsePositiveOnPlainPage(t *testing.T) {
	if _, err := os.Stat(laravelRulesDir); err != nil {
		t.Skipf("набор правил недоступен: %v", err)
	}
	set, err := LoadDir(laravelRulesDir)
	if err != nil {
		t.Fatalf("набор правил не загружается: %v", err)
	}
	// A page without framework signs (a typical hosting stub).
	res := set.Evaluate(Page{
		HTML: []byte("<!DOCTYPE html><html><head><title>It works!</title></head>" +
			"<body><h1>It works!</h1><p>This is the default web page.</p></body></html>"),
	})
	if res.Score != 0 || len(res.Signals) != 0 {
		t.Errorf("ложные срабатывания на пустой странице: score=%v, сигналы=%v", res.Score, res.Signals)
	}
}

func orQ(s string) string {
	if s == "" {
		return "?"
	}
	return s
}

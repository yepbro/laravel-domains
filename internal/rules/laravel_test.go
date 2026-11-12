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
		t.Skipf("the sample corpus is unavailable: %v", err)
	}
	set, err := LoadDir(laravelRulesDir)
	if err != nil {
		t.Fatalf("the rule set does not load: %v", err)
	}
	samples, err := LoadSamples(laravelSamplesDir)
	if err != nil {
		t.Fatalf("the corpus does not load: %v", err)
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
		t.Fatalf("the corpus is suspiciously small: positives %d (need >=5), negatives %d (need >=4)",
			positives, negatives)
	}

	for _, s := range samples {
		t.Run(s.ID, func(t *testing.T) {
			res := set.Evaluate(s.Page())

			if res.Conflict {
				t.Errorf("an unexpected version range conflict: [%s..%s]", res.VerMin, res.VerMax)
			}
			if isLaravel := res.Score >= laravelThreshold; s.Expect.IsLaravel != nil && isLaravel != *s.Expect.IsLaravel {
				t.Errorf("is_laravel = %v (score %.3f), expected %v", isLaravel, res.Score, *s.Expect.IsLaravel)
			}
			if !res.Contains(s.Expect.VerMin, s.Expect.VerMax) {
				t.Errorf("the range [%s..%s] does not cover the expectation [%s..%s]",
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
					t.Errorf("fired %v, in meta.yaml %v", fired, want)
				}
			}
		})
	}
}

func TestLaravelRulesetNoFalsePositiveOnPlainPage(t *testing.T) {
	if _, err := os.Stat(laravelRulesDir); err != nil {
		t.Skipf("the rule set is unavailable: %v", err)
	}
	set, err := LoadDir(laravelRulesDir)
	if err != nil {
		t.Fatalf("the rule set does not load: %v", err)
	}
	// A page without framework signs (a typical hosting stub).
	res := set.Evaluate(Page{
		HTML: []byte("<!DOCTYPE html><html><head><title>It works!</title></head>" +
			"<body><h1>It works!</h1><p>This is the default web page.</p></body></html>"),
	})
	if res.Score != 0 || len(res.Signals) != 0 {
		t.Errorf("false hits on an empty page: score=%v, signals=%v", res.Score, res.Signals)
	}
}

func orQ(s string) string {
	if s == "" {
		return "?"
	}
	return s
}

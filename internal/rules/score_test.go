package rules

import (
	"math"
	"testing"
)

func TestNoisyOR(t *testing.T) {
	cases := []struct {
		ws   []float64
		want float64
	}{
		{nil, 0}, // no rules hit at all
		{[]float64{0.25}, 0.25},
		{[]float64{0.25, 0.45}, 1 - 0.75*0.55},
		{[]float64{0.2, 0.2, 0.2, 0.2, 0.2}, 1 - 0.8*0.8*0.8*0.8*0.8},
		{[]float64{1, 0.2}, 1}, // weight 1 — a confident sign closes the score
		{[]float64{0.15, 0.15}, 1 - 0.85*0.85},
	}
	for _, c := range cases {
		if got := ScoreFixed(c.ws); diff(got, c.want) > 1e-9 {
			t.Errorf("noisyOR(%v) = %v, want %v", c.ws, got, c.want)
		}
	}
}

func diff(a, b float64) float64 {
	if a-b < 0 {
		return b - a
	}
	return a - b
}

// scoreRulesCases — rules without a target/expression: ScoreRules only
// looks at the weight and the version boundaries, matching does not
// participate here.
func scoreRulesCases() []struct {
	name           string
	rules          []Rule
	verMin, verMax string
	conflict       bool
} {
	return []struct {
		name           string
		rules          []Rule
		verMin, verMax string
		conflict       bool
	}{
		{
			name: "no rules",
		},
		{
			name:  "no bounds — the range is unbounded",
			rules: []Rule{{ID: "a", Weight: 0.25}, {ID: "b", Weight: 0.2}},
		},
		{
			name:   "ver_min only — the maximum is taken",
			rules:  []Rule{{ID: "a", Weight: 0.3, VerMin: "9.19"}, {ID: "b", Weight: 0.2, VerMin: "10"}, {ID: "c", Weight: 0.2}},
			verMin: "10",
		},
		{
			name:   "ver_max only — the minimum is taken",
			rules:  []Rule{{ID: "a", Weight: 0.3, VerMax: "11.x"}, {ID: "b", Weight: 0.2, VerMax: "9.x"}},
			verMax: "9.x",
		},
		{
			name:   "the intersection [9.19,∞) ∩ (−∞,9.x] = [9.19,9.x]",
			rules:  []Rule{{ID: "a", Weight: 0.15, VerMin: "9.19"}, {ID: "b", Weight: 0.15, VerMax: "9.x"}},
			verMin: "9.19", verMax: "9.x",
		},
		{
			name:   "equal bounds — a single-version range, not a conflict",
			rules:  []Rule{{ID: "a", Weight: 0.3, VerMin: "9.19"}, {ID: "b", Weight: 0.3, VerMax: "9.19"}},
			verMin: "9.19", verMax: "9.19",
		},
		{
			name:   "equal bounds in a different notation — 9.19 = 9.19.0",
			rules:  []Rule{{ID: "a", Weight: 0.3, VerMin: "9.19"}, {ID: "b", Weight: 0.3, VerMax: "9.19.0"}},
			verMin: "9.19", verMax: "9.19.0",
		},
		{
			name:   "an empty intersection [10,∞) ∩ (−∞,9.x] — a conflict, the range is reset",
			rules:  []Rule{{ID: "a", Weight: 0.3, VerMin: "10"}, {ID: "b", Weight: 0.3, VerMax: "9.x"}},
			verMin: "10", verMax: "9.x",
			conflict: true,
		},
		{
			name:   "a point conflict 9.19.1 < 10",
			rules:  []Rule{{ID: "a", Weight: 0.3, VerMin: "10"}, {ID: "b", Weight: 0.3, VerMax: "9.19.1"}},
			verMin: "10", verMax: "9.19.1",
			conflict: true,
		},
		{
			name:   "invalid versions are ignored",
			rules:  []Rule{{ID: "a", Weight: 0.3, VerMin: "lts"}, {ID: "b", Weight: 0.3, VerMax: "9..1"}, {ID: "c", Weight: 0.3, VerMin: "9.19"}},
			verMin: "9.19",
		},
		{
			// ver_min "0" — the first boundary is fixed even with a zero
			// component: the comparison against the still empty accumulator
			// must not discard it
			name:   "ver_min «0» — the lower bound is pinned",
			rules:  []Rule{{ID: "a", Weight: 0.3, VerMin: "0"}},
			verMin: "0",
		},
		{
			// ver_max with a negative component and no ver_min: no
			// conflict — the empty left boundary is not substituted into
			// the comparison
			name:   "ver_max without ver_min — no conflict",
			rules:  []Rule{{ID: "a", Weight: 0.3, VerMax: "-1"}},
			verMax: "-1",
		},
	}
}

// In the conflict cases the verMin/verMax in the table are the expected
// boundaries BEFORE the reset: ScoreRules must zero them.
func TestScoreRules(t *testing.T) {
	for _, c := range scoreRulesCases() {
		weights := make([]float64, len(c.rules))
		for i, r := range c.rules {
			weights[i] = r.Weight
		}

		score, verMin, verMax, conflict := ScoreRules(c.rules)
		if want := ScoreFixed(weights); diff(score, want) > 1e-9 {
			t.Errorf("%s: score = %v, want %v", c.name, score, want)
		}
		if c.conflict {
			if !conflict {
				t.Errorf("%s: a range conflict was expected", c.name)
			}
			if verMin != "" || verMax != "" {
				t.Errorf("%s: on a conflict the range must be reset, got [%s, %s]", c.name, verMin, verMax)
			}
			continue
		}
		if conflict {
			t.Errorf("%s: an unexpected range conflict", c.name)
		}
		if verMin != c.verMin || verMax != c.verMax {
			t.Errorf("%s: the range = [%s, %s], want [%s, %s]", c.name, verMin, verMax, c.verMin, c.verMax)
		}
	}
}

func TestIsLaravel(t *testing.T) {
	cases := []struct {
		name            string
		score, minScore float64
		want            bool
	}{
		{"exactly the threshold", 0.6, 0.6, true}, // the comparison is non-strict: score >= the threshold
		{"below the threshold", 0.59, 0.6, false},
		{"above the threshold", 0.61, 0.6, true},
		{"a threshold from the config", 0.75, 0.75, true},
		{"the maximum threshold", 1, 1, true},
		{"a zero threshold — the default 0.6", 0.6, 0, true},
		{"a zero threshold — the default 0.6, the score below", 0.5, 0, false},
		{"a negative threshold — the default 0.6", 0.55, -1, false},
		{"the threshold 1, the score below — not replaced by the default", 0.8, 1, false},
	}
	for _, c := range cases {
		res := Result{Score: c.score}
		if got := res.IsLaravel(c.minScore); got != c.want {
			t.Errorf("%s: IsLaravel(%v, threshold %v) = %v, want %v", c.name, c.score, c.minScore, got, c.want)
		}
	}
}

func TestVersionParse(t *testing.T) {
	inf := math.Inf(1)
	cases := []struct {
		in   string
		want version
	}{
		{"", nil},
		{"   ", nil},
		{"9.19", version{9, 19}},
		{"10", version{10}},
		{" 9.19 ", version{9, 19}}, // whitespace around and inside the components
		{"9. 19", version{9, 19}},
		{"9.x", version{9, inf}},
		{"9.*", version{9, inf}},
		{"8.83.27", version{8, 83, 27}},
		{"lts", nil},  // not a number
		{"9..1", nil}, // an empty component
		{"9.19.1.2.x", version{9, 19, 1, 2, inf}},
		{"9.x.1", version{9, inf, 1}}, // x not in the last component: the tail is parsed
	}
	for _, c := range cases {
		if got := parseVersion(c.in); !versionEq(got, c.want) {
			t.Errorf("parseVersion(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func versionEq(a, b version) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestVersionCompare(t *testing.T) {
	cases := []struct {
		a, b string
		less bool
	}{
		{"9.19", "11", true}, // numeric comparison, not string comparison
		{"11", "9.19", false},
		{"9.19", "9.x", true}, // 9.19 < everything in the nines
		{"9.x", "10", true},   // infinity only in the last component
		{"9", "9.1", true},    // a missing component = 0
		{"9.19", "9.19", false},
		{"9.19", "9.19.1", true},  // 9.19 = 9.19.0 < 9.19.1
		{"9.19.1", "9.19", false}, // and vice versa
		{"10.x", "10.1", false},   // 10.x — until the end of the major branch
		{"10.1", "10.x", true},
		{"9.-1", "9", true}, // the negative tail of the receiver is only visible to a full scan of the components
	}
	for _, c := range cases {
		va, vb := parseVersion(c.a), parseVersion(c.b)
		if va == nil || vb == nil {
			t.Fatalf("parseVersion(%q or %q) returned nil", c.a, c.b)
		}
		if got := va.less(vb); got != c.less {
			t.Errorf("%s < %s = %v, want %v", c.a, c.b, got, c.less)
		}
	}
}

// TestCompareVersions — the exported comparison (the laravel_domains
// aggregation, T-072): the order of the numeric components, the
// equivalence of different spellings, the unorderedness of empty/invalid
// strings.
func TestCompareVersions(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"9.19", "11", -1}, // numeric, not textual: a textual MIN would take "10"/"11" before "5.3"
		{"10", "5.3", 1},
		{"5.3", "10", -1},
		{"9.x", "11", -1},     // 9.x = 9.∞ < 10 < 11
		{"11", "9.x", 1},      // the upper bound max(9.x, 11) = 11
		{"9.19", "9.19.0", 0}, // equivalent in different spellings
		{"9.19.0", "9.19", 0},
		{"7.x", "7.x", 0},
		{"5.3", "5.3", 0},
		{"", "9.19", 0}, // empty — no data
		{"9.19", "", 0},
		{"", "", 0},
		{"abc", "9.19", 0}, // invalid — no data
		{"9.19", "9,19", 0},
	}
	for _, c := range cases {
		if got := CompareVersions(c.a, c.b); got != c.want {
			t.Errorf("CompareVersions(%q, %q) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestResultContains(t *testing.T) {
	res := Result{VerMin: "9.19", VerMax: ""}
	if !res.Contains("9.19", "") {
		t.Error("[9.19,∞) must include 9.19")
	}
	if !res.Contains("10", "11") {
		t.Error("[9.19,∞) must include [10,11]")
	}
	if res.Contains("9.18", "") {
		t.Error("[9.19,∞) must not include 9.18")
	}
	res2 := Result{VerMin: "9.19", VerMax: "11.x"}
	if !res2.Contains("10", "11") {
		t.Error("[9.19,11.x] must include [10,11]")
	}
	if res2.Contains("11", "12") {
		t.Error("[9.19,11.x] must not include 12")
	}
	res3 := Result{} // an unbounded range includes everything
	if !res3.Contains("5", "12") {
		t.Error("(−∞,∞) must include any range")
	}
	if res3.Contains("lts", "") {
		t.Error("an invalid expectation cannot fall into any range")
	}
	// A negative expected boundary in a range unbounded from below: the
	// result's missing lower boundary is not compared against the
	// expectation (lo == nil is not substituted into less).
	if !res3.Contains("-1", "") {
		t.Error("(−∞,∞) includes any valid expectation, including «-1»")
	}
	// An invalid upper expectation — does not fit any range.
	if res3.Contains("", "lts") {
		t.Error("an invalid upper expectation cannot fall into any range")
	}
}

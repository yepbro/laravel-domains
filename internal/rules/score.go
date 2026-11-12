// score.go — computing the outcomes of rule application (spec 5.8): the
// score via "noisy-OR", the version range as the intersection of the hit
// rules' ranges, and the is_laravel threshold.

package rules

import (
	"math"
	"strconv"
	"strings"
)

// DefaultMinScore — the default is_laravel threshold
// (analyzer.laravel_min_score, spec 5.8/5.10; the config validates the
// value to be in (0, 1]).
const DefaultMinScore = 0.6

// ScoreRules — the computation over the page's hit rules:
//
//   - score — the "noisy-OR" 1 − Π(1 − wᵢ): weak signs accumulate but
//     never reach 100%;
//   - the version range — the intersection of the hit rules'
//     [ver_min, ver_max]: the lower bound is the maximum of the ver_min
//     values, the upper bound is the minimum of the ver_max values (a rule
//     without a boundary does not narrow the range); the boundary strings
//     are kept from the rules as is ("9.x" does not become "9");
//   - an empty intersection (ver_max < ver_min) — conflict=true, the
//     boundaries are reset: the range is undefined (written to the DB as
//     ver_min and ver_max NULL; the conflict sign is visible in both empty
//     boundaries).
//
// Invalid version strings (parseVersion = nil) are ignored — the rule set
// has already passed the loader's validation; here it is a guard against
// typos.
func ScoreRules(matched []Rule) (score float64, verMin, verMax string, conflict bool) {
	var (
		minV, maxV       version
		minStr, maxStr   string
		haveMin, haveMax bool
	)
	for _, r := range matched {
		score = noisyOR(score, r.Weight)

		if v := parseVersion(r.VerMin); v != nil {
			if !haveMin || minV.less(v) {
				minV, minStr, haveMin = v, r.VerMin, true
			}
		}
		if v := parseVersion(r.VerMax); v != nil {
			if !haveMax || v.less(maxV) {
				maxV, maxStr, haveMax = v, r.VerMax, true
			}
		}
	}

	if haveMin {
		verMin = minStr
	}
	if haveMax {
		verMax = maxStr
	}
	if haveMin && haveMax && maxV.less(minV) {
		verMin, verMax, conflict = "", "", true
	}
	return score, verMin, verMax, conflict
}

// IsLaravel — a confident hit: score ≥ the threshold (the
// laravel_findings.is_laravel column, spec 5.8/6.1). minScore —
// analyzer.laravel_min_score; a non-positive value is replaced with
// DefaultMinScore (the config validation never allows that — a guard for
// direct calls with an uninitialized threshold).
func (res Result) IsLaravel(minScore float64) bool {
	if minScore <= 0 {
		minScore = DefaultMinScore
	}
	return res.Score >= minScore
}

// noisyOR: 1 − (1−a)(1−b) — weak signs accumulate but never reach 100%.
func noisyOR(a, b float64) float64 {
	return 1 - (1-a)*(1-b)
}

// ScoreFixed — noisy-OR over a set of weights.
func ScoreFixed(weights []float64) float64 {
	s := 0.0
	for _, w := range weights {
		s = noisyOR(s, w)
	}
	return s
}

// ---------------------------------------------------------------- versions

// version — the numeric version components; "x" means +∞ (the "until the
// end of the major branch" boundary, e.g. ver_max: "9.x"). A missing
// component is 0.
type version []float64

func parseVersion(s string) version {
	// An equivalent mutant (T-M12): trimming the whole string is redundant —
	// each component is trimmed separately by strings.TrimSpace below, and
	// whitespace at the start/end of the string is the start of the first
	// and the end of the last component.
	// mutator-disable-next-line statement/remove
	s = strings.TrimSpace(s)
	// An equivalent mutant (T-M12): without the early return, an empty
	// string splits into [""], ParseFloat("") yields ErrSyntax — the same
	// final nil. The annotation is on the if line, not the body: the
	// branch/if anchor is the if statement (a lesson from the control run
	// T-M10).
	// mutator-disable-next-line branch/if
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ".")
	out := make(version, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "x" || p == "*" {
			// Equivalent mutants (T-M12): math.Inf returns +Inf for any
			// sign ≥ 0 — Inf(0) and Inf(2) are the same +Inf as Inf(1)
			// (stdlib documentation).
			// mutator-disable-next-line numbers/decrementer, numbers/incrementer
			out = append(out, math.Inf(1))
			continue
		}
		// Equivalent mutants (T-M12): strconv.ParseFloat only distinguishes
		// bitSize 32 (a separate parsing branch); other values take the 64
		// path — 63 and 65 behave like 64, including ErrRange on "1e400"
		// (verified empirically).
		// mutator-disable-next-line numbers/decrementer, numbers/incrementer
		n, err := strconv.ParseFloat(p, 64)
		if err != nil {
			return nil // an invalid version is ignored
		}
		out = append(out, n)
	}
	return out
}

func (v version) less(o version) bool {
	// Equivalent mutants (T-M12): the extra iteration at i == len compares
	// two missing components (0 and 0) — they are equal, the loop exits
	// without a result; the outcome is the same. Removing the left operand
	// of the OR is killable — a receiver tail with a negative component is
	// only visible to a full scan (TestVersionCompare, "9.-1").
	// mutator-disable-next-line expression/comparison
	for i := 0; i < len(v) || i < len(o); i++ {
		var a, b float64
		if i < len(v) {
			a = v[i]
		}
		if i < len(o) {
			b = o[i]
		}
		if a != b {
			// An equivalent mutant (T-M12): the branch executes only at
			// a != b, where "a <= b" ≡ "a < b"; NaN components give false
			// in both variants (incomparability).
			// mutator-disable-next-line expression/comparison
			return a < b
		}
	}
	return false
}

// CompareVersions — comparing version strings by numeric components (the
// rule boundary semantics, T-070): a missing component is 0, "x"/"*" is
// +∞; returns -1 (a < b), 0 (equivalent, including the different
// spellings "9.19" and "9.19.0") or 1 (a > b). Exported for the
// laravel_domains aggregation (T-072: min(ver_min)/max(ver_max) over the
// domain's findings) — a textual MIN/MAX in SQL would order "10" before
// "5.3". An empty or invalid version (parseVersion = nil) is unordered — 0;
// findings only store strings that passed the set validation (T-068), the
// caller does not pass garbage here.
func CompareVersions(a, b string) int {
	va, vb := parseVersion(a), parseVersion(b)
	switch {
	case va == nil || vb == nil:
		return 0
	case va.less(vb):
		return -1
	case vb.less(va):
		return 1
	default:
		return 0
	}
}

// Contains — whether the expected range [expMin, expMax] falls inside the
// computed one (used by make test-rules).
func (res Result) Contains(expMin, expMax string) bool {
	if expMin != "" {
		e := parseVersion(expMin)
		lo := parseVersion(res.VerMin)
		if e == nil {
			return false
		}
		if lo != nil && e.less(lo) {
			return false
		}
	}
	if expMax != "" {
		e := parseVersion(expMax)
		hi := parseVersion(res.VerMax)
		if e == nil {
			return false
		}
		if hi != nil && hi.less(e) {
			return false
		}
	}
	return true
}

// The end-to-end rule engine corpus — task T-075 (spec §11.1: "the rule
// engine: score, version range intersection, conflicts; a run over the
// corpus of the customer's real Laravel samples (/testdata/laravel-samples,
// with expected is_laravel and version ranges) and over negative samples";
// acceptance criteria §11.4 items 4, 11; samples — §13.3).
//
// Unlike laravel_test.go (the threshold, the coverage of the sample's real
// version, the contents) and score_test.go (ScoreRules on isolated rules),
// the corpus case is a whole story "headers + cookies + HTML → EvaluateCtx
// (DefaultLimits) → ScoreRules → IsLaravel(0.6)" as the analyzer worker
// will assemble it: the expectations are exact (the score with a
// corpusScoreTol tolerance, the boundary strings of the computed range, the
// conflict), not just the coverage.
//
// The approach to the score expectation: the value in meta.yaml is the
// noisy-OR of the weights of the rules from the rules field, rounded to 3
// digits; the test allows ±corpusScoreTol. There are no exact float
// constants in the meta: rule weights change as the set is reworked, 3
// digits catch any substantive change and are not fragile to the
// accumulation order.
//
// Case groups: the full story over all samples (+ empty HTML for the
// JSON-API sample), the analysis of the negative samples' weak hits, the
// score math (noisy-OR accumulation against single weights, the threshold
// at the boundary), the range intersection and a conflicting rule set on a
// synthetic page, the limits (over-limit HTML is truncated — tail signs do
// not match).
package rules

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"
)

const (
	corpusRulesDir   = `../../rules`
	corpusSamplesDir = `../../testdata/laravel-samples`
	// corpusScoreTol — the tolerance for comparing the score against the
	// meta.yaml expectation (rounding to 3 digits; the float accumulation
	// order adds ~1e-16 noise).
	corpusScoreTol = 0.001
)

// corpusSet — the production Laravel rule set (rules/laravel.yaml).
func corpusSet(t *testing.T) *Set {
	t.Helper()
	if _, err := os.Stat(corpusRulesDir); err != nil {
		t.Skipf("набор правил недоступен: %v", err)
	}
	set, err := LoadDir(corpusRulesDir)
	if err != nil {
		t.Fatalf("набор правил не загружается: %v", err)
	}
	return set
}

// corpusSamples — the sample corpus (testdata/laravel-samples).
func corpusSamples(t *testing.T) []Sample {
	t.Helper()
	if _, err := os.Stat(corpusSamplesDir); err != nil {
		t.Skipf("корпус образцов недоступен: %v", err)
	}
	samples, err := LoadSamples(corpusSamplesDir)
	if err != nil {
		t.Fatalf("корпус не загружается: %v", err)
	}
	return samples
}

// corpusSampleByID — one corpus sample by directory name.
func corpusSampleByID(t *testing.T, id string) Sample {
	t.Helper()
	for _, s := range corpusSamples(t) {
		if s.ID == id {
			return s
		}
	}
	t.Fatalf("образец %s отсутствует в корпусе", id)
	return Sample{}
}

// corpusFired — the hit rules (ids, sorted — the comparison against the
// expectation does not depend on the set order).
func corpusFired(res Result) []string {
	fired := make([]string, 0, len(res.Signals))
	for _, ev := range res.Signals {
		fired = append(fired, ev.Rule)
	}
	sort.Strings(fired)
	return fired
}

// corpusSortedIDs — a sorted copy of the id list.
func corpusSortedIDs(ids []string) []string {
	out := slices.Clone(ids)
	sort.Strings(out)
	return out
}

// corpusMatchedRules — the set rules that hit the page, in set order (the
// same order EvaluateCtx accumulated them in) — the input of the direct
// ScoreRules call, the next link of the full story.
func corpusMatchedRules(t *testing.T, set *Set, res Result) []Rule {
	t.Helper()
	fired := make(map[string]bool, len(res.Signals))
	for _, ev := range res.Signals {
		fired[ev.Rule] = true
	}
	var matched []Rule
	for _, r := range set.Rules {
		if fired[r.ID] {
			matched = append(matched, r)
		}
	}
	return matched
}

// TestCorpusSamplesFullHistory — the full story for each corpus sample:
// headers.json + cookies.json + index.html → Page → EvaluateCtx
// (DefaultLimits) → ScoreRules → IsLaravel(0.6). The exact engine
// expectations (score/calc_ver_min/calc_ver_max/conflict) are mandatory in
// every meta.yaml — without them the corpus would silently degrade to
// checking a single threshold.
func TestCorpusSamplesFullHistory(t *testing.T) {
	set := corpusSet(t)
	samples := corpusSamples(t)

	// Corpus integrity (spec §11.4 items 4, 11; §13.3): positive and
	// negative samples; the negatives must include "weak" ones — with hits
	// below the threshold (a false-positive control).
	var positives, negatives, weakNegatives int
	for _, s := range samples {
		if s.Expect.IsLaravel == nil {
			t.Fatalf("samples/%s/meta.yaml: нужно ожидание is_laravel", s.ID)
		}
		if s.Expect.Score == nil || s.Expect.CalcVerMin == nil || s.Expect.CalcVerMax == nil {
			t.Fatalf("samples/%s/meta.yaml: нужны точные ожидания движка score/calc_ver_min/calc_ver_max", s.ID)
		}
		if s.Expect.Rules == nil {
			t.Fatalf("samples/%s/meta.yaml: нужен полный состав сработавших правил (rules)", s.ID)
		}
		if *s.Expect.IsLaravel {
			positives++
		} else {
			negatives++
			if len(s.Expect.Rules) > 0 {
				weakNegatives++
			}
		}
	}
	if positives < 5 || negatives < 4 || weakNegatives < 1 {
		t.Fatalf("корпус подозрительно мал: позитивных %d (нужно >=5), негативных %d (нужно >=4), из них со слабыми срабатываниями %d (нужно >=1)",
			positives, negatives, weakNegatives)
	}

	for _, s := range samples {
		t.Run(s.ID, func(t *testing.T) {
			res := set.EvaluateCtx(context.Background(), s.Page(), DefaultLimits())

			// Contents: complete and exact.
			if fired := corpusFired(res); !slices.Equal(fired, corpusSortedIDs(s.Expect.Rules)) {
				t.Errorf("сработали %v, в meta.yaml %v", fired, s.Expect.Rules)
			}

			// Score — an exact expectation (± corpusScoreTol).
			if d := diff(res.Score, *s.Expect.Score); d > corpusScoreTol {
				t.Errorf("score = %.6f, в meta.yaml %.3f (расхождение %.6f > %g)",
					res.Score, *s.Expect.Score, d, corpusScoreTol)
			}

			// The ScoreRules link: a direct call on the hit rules — the same
			// outcome EvaluateCtx assembled.
			score2, verMin2, verMax2, conflict2 := ScoreRules(corpusMatchedRules(t, set, res))
			if score2 != res.Score || verMin2 != res.VerMin || verMax2 != res.VerMax || conflict2 != res.Conflict {
				t.Errorf("ScoreRules не сходится с EvaluateCtx: (%v, %q, %q, %v) против (%v, %q, %q, %v)",
					score2, verMin2, verMax2, conflict2, res.Score, res.VerMin, res.VerMax, res.Conflict)
			}

			// The version range — the exact boundary strings from the rules
			// ("" — no boundary; written to the DB as NULL).
			if res.VerMin != *s.Expect.CalcVerMin || res.VerMax != *s.Expect.CalcVerMax {
				t.Errorf("диапазон = [%q..%q], в meta.yaml [%q..%q]",
					res.VerMin, res.VerMax, *s.Expect.CalcVerMin, *s.Expect.CalcVerMax)
			}
			if res.Conflict != s.Expect.Conflict {
				t.Errorf("conflict = %v, в meta.yaml %v", res.Conflict, s.Expect.Conflict)
			}
			if res.Conflict && (res.VerMin != "" || res.VerMax != "") {
				t.Errorf("при конфликте диапазон неопределён (обе границы NULL), got [%q..%q]", res.VerMin, res.VerMax)
			}

			// The is_laravel threshold (spec §13.1: 0.6); a zero threshold
			// must substitute the same default.
			if got := res.IsLaravel(DefaultMinScore); got != *s.Expect.IsLaravel {
				t.Errorf("IsLaravel(0.6) = %v (score %.3f), в meta.yaml %v",
					got, res.Score, *s.Expect.IsLaravel)
			}
			if res.IsLaravel(0) != res.IsLaravel(DefaultMinScore) {
				t.Error("нулевой порог должен подставлять DefaultMinScore")
			}
		})
	}
}

// TestCorpusJSONAPIEmptyHTML — a JSON-API sample without HTML: a JSON
// episode may not have stored the body (an error before reading the body,
// content over the limit), passive analysis by cookies must work on empty
// content too. htmlCap on nil HTML is safe, the HTML rules
// (json_message_key) do not hit, the threshold is reached by cookies
// alone: 1 − 0.5·0.65·0.85·0.75 = 0.793 ≥ 0.6
// (the cookie_encrypted_payload weight of 0.5 — the T-116 calibration).
func TestCorpusJSONAPIEmptyHTML(t *testing.T) {
	set := corpusSet(t)
	page := corpusSampleByID(t, "laravel-9-json-api").Page()
	page.HTML = nil

	res := set.EvaluateCtx(context.Background(), page, DefaultLimits())

	want := []string{
		"cookie_encrypted_payload",
		"cookie_laravel_session",
		"cookie_session_suffix",
		"cookie_xsrf_token",
	}
	if fired := corpusFired(res); !slices.Equal(fired, want) {
		t.Fatalf("сработали %v, ожидались только куки %v", fired, want)
	}
	if wantScore := ScoreFixed([]float64{0.5, 0.35, 0.15, 0.25}); diff(res.Score, wantScore) > 1e-9 {
		t.Errorf("score = %v, want %v (куки без JSON-признака)", res.Score, wantScore)
	}
	if !res.IsLaravel(DefaultMinScore) {
		t.Errorf("куки одни дают уверенное срабатывание: score %.3f ≥ 0.6", res.Score)
	}
	if res.VerMin != "" || res.VerMax != "" || res.Conflict {
		t.Errorf("диапазон без версионных правил должен быть пуст: [%q..%q] conflict=%v",
			res.VerMin, res.VerMax, res.Conflict)
	}
}

// TestCorpusJSONMessageKey — the broad json_message_key rule (T-117, the
// passive counterpart of the active larasites 404 probe): it matches both
// the exact "Unauthenticated." and "Not Found" — exactly once per response
// (no double counting: no exact rule exists next to it), but alone it does
// not reach the threshold (the weight of 0.25 is lowered against the exact
// 0.3: NestJS/Express produce the pattern — {"message":"Cannot GET /"}).
func TestCorpusJSONMessageKey(t *testing.T) {
	set := corpusSet(t)

	for _, body := range []string{
		`{"message":"Unauthenticated."}`, // the exact form (the former json_unauthenticated)
		`{"message":"Not Found"}`,        // a 404 API subdomain (the T-117 sample)
		`{"message":"Too Many Attempts."}`,
	} {
		res := set.EvaluateCtx(context.Background(), Page{HTML: []byte(body)}, DefaultLimits())
		if fired := corpusFired(res); !slices.Equal(fired, []string{"json_message_key"}) {
			t.Errorf("тело %q: сработали %v, хочу ровно json_message_key (без дублей)", body, fired)
		}
		if res.Score != 0.25 {
			t.Errorf("тело %q: score = %v, want 0.25", body, res.Score)
		}
		if res.IsLaravel(DefaultMinScore) {
			t.Errorf("тело %q: одиночное правило 0.25 не должно достигать порога %.2f", body, DefaultMinScore)
		}
	}

	// Pattern boundaries: a long value (>120) and a non-string key do not
	// match; an empty one does not either (the {1,120} quantifier).
	for _, body := range []string{
		`{"message":"` + strings.Repeat("x", 121) + `"}`,
		`{"message":""}`,
		`{"message":404}`,
		`{"messages":"Not Found"}`,
	} {
		res := set.EvaluateCtx(context.Background(), Page{HTML: []byte(body)}, DefaultLimits())
		if len(res.Signals) != 0 {
			t.Errorf("тело %.40q…: неожиданные срабатывания %v", body, corpusFired(res))
		}
	}

	// A foreign stack: NestJS/Express produces the same form — below the
	// threshold alone, detected in combination with the T-116 cookies (the
	// laravel-12-api-404 sample).
	nest := set.EvaluateCtx(context.Background(),
		Page{HTML: []byte(`{"message":"Cannot GET /"}`)}, DefaultLimits())
	if nest.IsLaravel(DefaultMinScore) || nest.Score != 0.25 {
		t.Errorf("NestJS-ответ: score %.3f — одиночное правило ниже порога, не Laravel", nest.Score)
	}
}

// TestCorpusCMSSession — the cookie_cms_session rule (T-134, a cross-check
// with larasites): Laravel-based CMSes set their own session names
// (october/winter/bagisto — verified against the CMS sources) which before
// the rule were only caught by the generic *_session suffix — the
// "CMS cookie + encrypted envelope" pair without XSRF-TOKEN gave
// 0.15 + 0.5 = 0.575 < 0.6 (a miss not closed by the T-116 calibration).
// The weight of 0.35 is the laravel_session level (the names are
// Laravel-ecosystem-specific, unlike the suffix, which also matches
// ci_session): 0.35 + 0.5 → 0.675 ≥ 0.6; the double counting with the
// suffix is consistent with the laravel_session behavior (it matches
// both). The __Secure-/__Host- prefixes (October ≥1.1 sets them
// automatically on https) are accounted for in both the cms rule and the
// *_session suffix.
func TestCorpusCMSSession(t *testing.T) {
	set := corpusSet(t)

	// Corpus samples: October (the __Secure- prefix — an https episode of
	// October ≥1.1) and Winter (the stock name), both without XSRF-TOKEN —
	// detection by cookies: exactly three rules, no noisy HTML signs.
	for _, id := range []string{"october-cms", "winter-cms"} {
		res := set.EvaluateCtx(context.Background(), corpusSampleByID(t, id).Page(), DefaultLimits())
		want := []string{"cookie_cms_session", "cookie_encrypted_payload", "cookie_session_suffix"}
		if fired := corpusFired(res); !slices.Equal(fired, want) {
			t.Errorf("%s: сработали %v, ожидались %v", id, fired, want)
		}
		if wantScore := ScoreFixed([]float64{0.35, 0.15, 0.5}); diff(res.Score, wantScore) > 1e-9 {
			t.Errorf("%s: score = %v, want %v (0.35+0.15+0.5)", id, res.Score, wantScore)
		}
		if !res.IsLaravel(DefaultMinScore) {
			t.Errorf("%s: score %.3f ≥ порога — пропуск T-134 закрыт", id, res.Score)
		}
	}

	// The list contents — only names verifiable against the sources:
	// october/winter/bagisto match with and without the __Secure-/__Host-
	// prefixes.
	fires := func(name string) bool {
		t.Helper()
		res := set.EvaluateCtx(context.Background(),
			Page{Cookies: []Cookie{{Name: name, Value: "plain"}}}, DefaultLimits())
		return slices.Contains(corpusFired(res), "cookie_cms_session")
	}
	for _, name := range []string{
		"october_session", "winter_session", "bagisto_session",
		"__Secure-october_session", "__Host-winter_session", "__Secure-bagisto_session",
	} {
		if !fires(name) {
			t.Errorf("кука %s: правило cookie_cms_session должно срабатывать", name)
		}
	}
	// No match: no confirmed names (botble/pyrocms/asgard — the contents
	// are not extended with unverifiable ones), generic names and pattern
	// edges — anchors, postfixes, a foreign shape (statamic leaves
	// laravel_session).
	for _, name := range []string{
		"ci_session", "myapp_session", "laravel_session",
		"botble_session", "pyrocms_session", "asgard_session",
		"october_sessions",         // a postfix tail
		"foo_october_session",      // an application name before the cms name
		"october-session",          // a hyphen instead of the underscore
		"__secure-october_session", // the prefix is case-sensitive (RFC 6265bis)
	} {
		if fires(name) {
			t.Errorf("кука %s: правило cookie_cms_session срабатывать не должно", name)
		}
	}

	// laravel_session — the reference for double-counting consistency: it
	// matches cookie_laravel_session and the suffix, but not the cms rule.
	res := set.EvaluateCtx(context.Background(),
		Page{Cookies: []Cookie{{Name: "laravel_session", Value: "plain"}}}, DefaultLimits())
	if fired := corpusFired(res); !slices.Equal(fired, []string{"cookie_laravel_session", "cookie_session_suffix"}) {
		t.Errorf("laravel_session: сработали %v, ожидались cookie_laravel_session и cookie_session_suffix", fired)
	}

	// The *_session suffix with prefixes: __Secure-myapp_session (an
	// arbitrary name, as in the larasites myapp_session test) is matched by
	// the suffix — 0.15; __Secure-october_session — by both rules (double
	// counting).
	res = set.EvaluateCtx(context.Background(),
		Page{Cookies: []Cookie{{Name: "__Secure-myapp_session", Value: "plain"}}}, DefaultLimits())
	if fired := corpusFired(res); !slices.Equal(fired, []string{"cookie_session_suffix"}) {
		t.Errorf("__Secure-myapp_session: сработали %v, ожидался только cookie_session_suffix", fired)
	}
	if diff(res.Score, 0.15) > 1e-9 || res.IsLaravel(DefaultMinScore) {
		t.Errorf("__Secure-myapp_session: score %.3f — суффикс один ниже порога", res.Score)
	}

	// The gray zone (the T-115 control: the new pairs add no noise):
	//
	//   - a CMS name without the envelope: 0.35 + 0.15 = 0.4475 < 0.6
	//     (EncryptCookies disabled/the cookie is from a proxy) — a finding
	//     in findings, but not laravel_domains;
	//   - the closest negative to the new pair: ci_session + the envelope —
	//     0.15 + 0.5 = 0.575 < 0.6 (ci_session is not in the cms list, the
	//     suffix is indifferent to the envelope — the same miss class for
	//     non-CMS names was deliberately left below the threshold: without
	//     an ecosystem name the envelope is weaker).
	res = set.EvaluateCtx(context.Background(),
		Page{Cookies: []Cookie{{Name: "october_session", Value: "plain"}}}, DefaultLimits())
	if want := 1 - 0.65*0.85; diff(res.Score, want) > 1e-9 || res.IsLaravel(DefaultMinScore) {
		t.Errorf("CMS-имя без конверта: score %.4f, want %v, ниже порога", res.Score, want)
	}
	res = set.EvaluateCtx(context.Background(),
		Page{Cookies: []Cookie{{Name: "ci_session", Value: "eyJpdiI6Y2k6MTY6e30="}}}, DefaultLimits())
	if fired := corpusFired(res); !slices.Equal(fired, []string{"cookie_encrypted_payload", "cookie_session_suffix"}) {
		t.Fatalf("ci_session с конвертом: сработали %v, ожидались cookie_encrypted_payload и cookie_session_suffix", fired)
	}
	if want := 1 - 0.85*0.5; diff(res.Score, want) > 1e-9 || res.IsLaravel(DefaultMinScore) {
		t.Errorf("ci_session с конвертом: score %.4f, want %v — ниже порога", res.Score, want)
	}
}

// TestCorpusNegativesWeakSignals — the negative samples (spec §13.3:
// "including ones on other frameworks using similar signs"): is_laravel
// must be false, and the test breaks down which weak hits the engine is
// limited to and why they are below the threshold.
func TestCorpusNegativesWeakSignals(t *testing.T) {
	set := corpusSet(t)

	for _, s := range corpusSamples(t) {
		if s.Expect.IsLaravel == nil || *s.Expect.IsLaravel {
			continue
		}
		t.Run(s.ID, func(t *testing.T) {
			res := set.EvaluateCtx(context.Background(), s.Page(), DefaultLimits())

			if res.IsLaravel(DefaultMinScore) {
				t.Fatalf("ложноположительное срабатывание: score %.3f ≥ порога %.2f", res.Score, DefaultMinScore)
			}
			if s.Expect.Score == nil {
				t.Fatal("нет ожидания score")
			}
			if d := diff(res.Score, *s.Expect.Score); d > corpusScoreTol {
				t.Errorf("score = %.6f, в meta.yaml %.3f (расхождение %.6f)", res.Score, *s.Expect.Score, d)
			}
			if fired := corpusFired(res); !slices.Equal(fired, corpusSortedIDs(s.Expect.Rules)) {
				t.Errorf("сработали %v, в meta.yaml %v", fired, s.Expect.Rules)
			}

			// Why below the threshold: every individual sign is weak — no
			// hit weight reaches the threshold by itself, and the
			// accumulation (res.Score) stayed below. The production set is
			// built this way deliberately (the header of rules/laravel.yaml):
			// rules that also occur outside Laravel get a weight < 0.6 — a
			// hit lands in laravel_findings but not in laravel_domains.
			for _, ev := range res.Signals {
				if ev.Weight >= DefaultMinScore {
					t.Errorf("правило %s одним весом %.2f достигает порога: для похожего не-Laravel признака вес завышен",
						ev.Rule, ev.Weight)
				}
			}
		})
	}

	// Rails — the reference "similar" framework (§13.3): Rails templates
	// also write the csrf-token meta (0.2), the _shop_session cookie is
	// matched by the *_session suffix rule (0.15, the weight lowered
	// precisely because of Rails and CodeIgniter). Alone and together —
	// the noisy-OR 1 − 0.8·0.85 = 0.32 — below the 0.6 threshold.
	t.Run("rails-negative: 0.32 = noisy-OR(0.2, 0.15)", func(t *testing.T) {
		res := set.EvaluateCtx(context.Background(),
			corpusSampleByID(t, "rails-negative").Page(), DefaultLimits())
		weights := map[string]float64{}
		for _, ev := range res.Signals {
			weights[ev.Rule] = ev.Weight
		}
		if len(weights) != 2 || weights["meta_csrf_token"] != 0.2 || weights["cookie_session_suffix"] != 0.15 {
			t.Fatalf("сработали %v, ожидались meta_csrf_token (0.2) и cookie_session_suffix (0.15)", weights)
		}
		if want := 1 - (1-0.2)*(1-0.15); diff(res.Score, want) > 1e-9 {
			t.Errorf("score = %v, want %v", res.Score, want)
		}
		if res.IsLaravel(DefaultMinScore) {
			t.Errorf("score %.3f не должен достигать порога %.2f", res.Score, DefaultMinScore)
		}
	})
}

// TestCorpusScoreMath — the score math end-to-end (a synthetic set, a page
// with markers — the full story up to IsLaravel): the noisy-OR
// accumulation against single weights and the threshold at the boundary.
// ScoreRules itself is covered by score_test.go; here — the behavior of
// the "Laravel / not Laravel" decision on a whole page.
func TestCorpusScoreMath(t *testing.T) {
	// Accumulation: the page adds one weak sign at a time (weight 0.2), the
	// score grows as 1 − 0.8ⁿ; the 0.6 threshold is reached only by the
	// fifth — single weights, pairs and triples stay below. Weak signs
	// accumulate but never reach 100% (spec §5.8).
	markers := `
rules:
  - id: m1
    target: html
    regex: "MARK1"
    weight: 0.2
  - id: m2
    target: html
    regex: "MARK2"
    weight: 0.2
  - id: m3
    target: html
    regex: "MARK3"
    weight: 0.2
  - id: m4
    target: html
    regex: "MARK4"
    weight: 0.2
  - id: m5
    target: html
    regex: "MARK5"
    weight: 0.2
`
	set := evalSet(t, markers)
	for n := 1; n <= 5; n++ {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = fmt.Sprintf("MARK%d", i+1)
		}
		res := set.EvaluateCtx(context.Background(),
			Page{HTML: []byte(strings.Join(parts, " "))}, DefaultLimits())
		remain := 1.0 // Π(1 − 0.2) over the n hits
		for i := 0; i < n; i++ {
			remain *= 0.8
		}
		if want := 1 - remain; diff(res.Score, want) > 1e-9 {
			t.Errorf("%d маркера: score = %v, want %v", n, res.Score, want)
		}
		if got := res.IsLaravel(DefaultMinScore); got != (n == 5) {
			t.Errorf("%d маркера: IsLaravel = %v (score %.4f)", n, got, res.Score)
		}
	}
	// The accumulation exceeds any single component but stays below one.
	five := set.EvaluateCtx(context.Background(),
		Page{HTML: []byte("MARK1 MARK2 MARK3 MARK4 MARK5")}, DefaultLimits())
	if five.Score <= 0.2 || five.Score >= 1 {
		t.Errorf("накопление пяти по 0.2 вне (0.2, 1): %v", five.Score)
	}

	// A single weight: the score equals the weight (a tolerance for the operation order).
	for _, w := range []float64{0.25, 0.45, 0.5, 0.9} {
		set := evalSet(t, fmt.Sprintf("rules:\n  - id: solo\n    target: html\n    regex: MARK\n    weight: %v\n", w))
		res := set.EvaluateCtx(context.Background(), Page{HTML: []byte("MARK")}, DefaultLimits())
		if diff(res.Score, w) > 1e-9 {
			t.Errorf("одиночный вес %v: score = %v", w, res.Score)
		}
	}

	// The threshold at the boundary: a weight of exactly 0.6 — the score is
	// exactly 0.6 without rounding (1−(1−0)·(1−0.6) is exact in IEEE-754),
	// the non-strict comparison gives true.
	set = evalSet(t, "rules:\n  - id: edge\n    target: html\n    regex: MARK\n    weight: 0.6\n")
	res := set.EvaluateCtx(context.Background(), Page{HTML: []byte("MARK")}, DefaultLimits())
	if diff(res.Score, 0.6) > 1e-12 {
		t.Fatalf("вес 0.6: score = %.17g, want ровно 0.6", res.Score)
	}
	if !res.IsLaravel(DefaultMinScore) {
		t.Error("score ровно на пороге: нестрогое ≥ должно давать is_laravel")
	}

	// The real pair cookie_xsrf_token (0.25) + cookie_encrypted_payload
	// (0.5, the T-116 calibration): 1 − 0.75·0.5 = 0.625 ≥ the threshold —
	// a stateless API (an api subdomain without a session on the main page)
	// is detected by two cookies; before the calibration (weight 0.45) the
	// pair gave 0.5875 and the site dropped out of laravel_domains (a
	// defect found by the cross-check with larasites).
	set = evalSet(t, `
rules:
  - id: a_xsrf
    target: cookie_name
    match: "XSRF-TOKEN"
    weight: 0.25
  - id: b_enc
    target: cookie_value
    regex: "^eyJpdiI6"
    weight: 0.5
`)
	res = set.EvaluateCtx(context.Background(),
		Page{Cookies: []Cookie{{Name: "XSRF-TOKEN", Value: "eyJpdiI6abc=="}}}, DefaultLimits())
	if want := 1 - 0.75*0.5; diff(res.Score, want) > 1e-9 {
		t.Errorf("score = %v, want %v", res.Score, want)
	}
	if !res.IsLaravel(DefaultMinScore) {
		t.Errorf("score %.4f обязан достигать порога %v (T-116: stateless API)", res.Score, DefaultMinScore)
	}

	// The weakening did not bring in noisy pairs (a T-116 check by the gray
	// zone analogy): combinations WITHOUT the encrypted cookie stay below
	// the threshold — the CodeIgniter triple ci_session (the suffix 0.15) +
	// the csrf meta (0.2) + the _token form (0.25):
	// 1 − 0.85·0.8·0.75 = 0.49. The suffix is a copy of the production rule
	// (with the __Secure-/__Host- prefixes from T-134): a cookie with a
	// prefix is caught by the same weight and the threshold does not
	// change.
	set = evalSet(t, `
rules:
  - id: ci_sess
    target: cookie_name
    regex: '^(?:__Secure-|__Host-)?[a-z0-9_\-]+_session$'
    weight: 0.15
  - id: csrf_meta
    target: meta
    regex: 'name=["'']csrf-token["'']'
    weight: 0.2
  - id: csrf_form
    target: html
    regex: '<input[^>]+name="_token"'
    weight: 0.25
`)
	ciPage := Page{
		HTML:    []byte(`<meta name="csrf-token" content="x"><input type="hidden" name="_token" value="y">`),
		Cookies: []Cookie{{Name: "ci_session", Value: "aGVsbG8"}},
	}
	res = set.EvaluateCtx(context.Background(), ciPage, DefaultLimits())
	if want := 1 - 0.85*0.8*0.75; diff(res.Score, want) > 1e-9 {
		t.Errorf("score шумовой тройки = %v, want %v", res.Score, want)
	}
	if res.IsLaravel(DefaultMinScore) {
		t.Errorf("шумовая тройка без шифрованной куки: %.4f не должна достигать порога", res.Score)
	}
	ciPage.Cookies[0].Name = "__Secure-ci_session"
	res = set.EvaluateCtx(context.Background(), ciPage, DefaultLimits())
	if want := 1 - 0.85*0.8*0.75; diff(res.Score, want) > 1e-9 {
		t.Errorf("score тройки с __Secure-ci_session = %v, want %v (префикс вес не меняет)", res.Score, want)
	}

	// Weight 1 — a confident sign closes the score up to one.
	set = evalSet(t, "rules:\n  - id: sure\n    target: html\n    regex: MARK\n    weight: 1\n")
	if res := set.EvaluateCtx(context.Background(), Page{HTML: []byte("MARK")}, DefaultLimits()); res.Score != 1 {
		t.Errorf("вес 1: score = %v, want 1", res.Score)
	}
}

// TestCorpusVersionRanges — the version range intersection end-to-end: a
// pre-built Set is applied to a synthetic page, the boundaries are read
// from the EvaluateCtx outcome (not from ScoreRules directly). Spec §5.8:
// the range is the intersection of the hit rules' [ver_min, ver_max], an
// empty intersection is a conflict and an undefined range.
func TestCorpusVersionRanges(t *testing.T) {
	cases := []struct {
		name             string
		body             string // the set yaml
		page             string // the synthetic page HTML
		wantMin, wantMax string // exact boundary strings ("" — no boundary)
		conflict         bool
	}{
		{
			name: "пересечение: нижняя = max(ver_min), верхняя = min(ver_max)",
			body: `
rules:
  - id: a_welcome
    target: html
    regex: "WELCOME"
    weight: 0.45
    ver_min: "5.3"
    ver_max: "7.x"
  - id: b_floor
    target: html
    regex: "FLOOR"
    weight: 0.2
    ver_min: "5.4"
  - id: c_ceiling
    target: html
    regex: "CEILING"
    weight: 0.1
    ver_max: "11.x"
  - id: d_plain
    target: html
    regex: "PLAIN"
    weight: 0.25
`,
			page:     "WELCOME FLOOR CEILING PLAIN",
			wantMin:  "5.4", // max(5.3, 5.4)
			wantMax:  "7.x", // min(7.x, 11.x); d_plain without boundaries does not narrow
			conflict: false,
		},
		{
			name: "равные границы в разной записи — диапазон из одной версии, не конфликт",
			body: `
rules:
  - id: a_low
    target: html
    regex: "LOW"
    weight: 0.3
    ver_min: "9.19"
  - id: b_high
    target: html
    regex: "HIGH"
    weight: 0.3
    ver_max: "9.19.0"
`,
			page:     "LOW HIGH", // 9.19 = 9.19.0
			wantMin:  "9.19",
			wantMax:  "9.19.0",
			conflict: false,
		},
		{
			name: "строки границ сохраняются из правил как есть",
			body: `
rules:
  - id: a_new
    target: html
    regex: "NEW"
    weight: 0.2
    ver_min: "9.19"
  - id: b_old
    target: html
    regex: "OLD"
    weight: 0.15
    ver_max: "9.x"
`,
			page:     "NEW OLD", // [9.19,∞) ∩ (−∞,9.x]
			wantMin:  "9.19",
			wantMax:  "9.x", // "9.x" does not become "9"
			conflict: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := evalSet(t, c.body).EvaluateCtx(context.Background(),
				Page{HTML: []byte(c.page)}, DefaultLimits())
			if res.VerMin != c.wantMin || res.VerMax != c.wantMax || res.Conflict != c.conflict {
				t.Errorf("диапазон = [%q..%q] conflict=%v, want [%q..%q] conflict=%v",
					res.VerMin, res.VerMax, res.Conflict, c.wantMin, c.wantMax, c.conflict)
			}
		})
	}

	// A conflicting set on one synthetic page: signs with incompatible
	// boundaries (≥10 and ≤9.x) hit together — the intersection is empty,
	// the range is reset (both boundaries NULL), but the score is computed
	// over all hits and the confident detection stays confident: a version
	// conflict is not a "Laravel / not Laravel" conflict.
	t.Run("конфликт: [10,∞) ∩ (−∞,9.x] — диапазон сброшен, порог в силе", func(t *testing.T) {
		set := evalSet(t, `
rules:
  - id: a_new
    target: html
    regex: "NEW"
    weight: 0.45
    ver_min: "10"
  - id: b_old
    target: html
    regex: "OLD"
    weight: 0.5
    ver_max: "9.x"
`)
		res := set.EvaluateCtx(context.Background(),
			Page{HTML: []byte("NEW OLD")}, DefaultLimits())
		if !res.Conflict {
			t.Fatal("ожидался конфликт диапазонов")
		}
		if res.VerMin != "" || res.VerMax != "" {
			t.Errorf("при конфликте границы сбрасываются, got [%q..%q]", res.VerMin, res.VerMax)
		}
		if len(res.Signals) != 2 {
			t.Errorf("оба срабатывания сохраняются (в laravel_findings пишутся все), got %d", len(res.Signals))
		}
		if want := 1 - 0.55*0.5; diff(res.Score, want) > 1e-9 {
			t.Errorf("score = %v, want %v (конфликт не меняет score)", res.Score, want)
		}
		if !res.IsLaravel(DefaultMinScore) {
			t.Errorf("score %.3f ≥ порога: конфликт версий не отменяет детекцию", res.Score)
		}
	})

	// A conflict on the production set: a Laravel 12 page with inherited
	// classic welcome markup — Vite (≥9.19) and welcome_classic (≤7.x)
	// cannot coexist in any version.
	t.Run("рабочий набор: vite (≥9.19) + welcome_classic (≤7.x) — конфликт", func(t *testing.T) {
		set := corpusSet(t)
		res := set.EvaluateCtx(context.Background(), Page{HTML: []byte(`<!DOCTYPE html>
<html><head>
<link rel="stylesheet" href="/build/assets/app-Bk3kX93l.css">
</head>
<body>
<div class="flex-center position-ref full-height">
  <div class="title m-b-md">Laravel</div>
  <a href="https://laravel.com/docs">Docs</a>
</div>
</body></html>`)}, DefaultLimits())
		if fired := corpusFired(res); !slices.Equal(fired,
			[]string{"laravel_docs_link", "vite_assets", "welcome_classic"}) {
			t.Fatalf("сработали %v, ожидались laravel_docs_link, vite_assets, welcome_classic", fired)
		}
		if !res.Conflict {
			t.Fatal("ожидался конфликт: [9.19,∞) ∩ [5.3,7.x] пусто")
		}
		if res.VerMin != "" || res.VerMax != "" {
			t.Errorf("при конфликте границы сбрасываются, got [%q..%q]", res.VerMin, res.VerMax)
		}
		if want := 1 - 0.7*0.8*0.55; diff(res.Score, want) > 1e-9 {
			t.Errorf("score = %v, want %v", res.Score, want)
		}
		if !res.IsLaravel(DefaultMinScore) {
			t.Errorf("score %.3f ≥ порога несмотря на конфликт диапазона", res.Score)
		}
	})
}

// TestCorpusHTMLTailLimit — the limits end-to-end (spec §9: parsers have
// size limits): over-limit HTML is truncated before matching — tail signs
// do not match, the other targets (cookies) are not affected.
func TestCorpusHTMLTailLimit(t *testing.T) {
	// The production set on a real sample with 2 MiB of junk before the
	// document: the HTML signs (meta csrf-token, vite, inertia) fall beyond
	// DefaultLimits().MaxHTMLBytes and do not hit; cookies are not touched
	// by the HTML limit — they are enough for the threshold
	// (0.772 ≥ 0.6), is_laravel is preserved.
	t.Run("образец с хвостом за 2 MiB: HTML-признаки не видны, куки работают", func(t *testing.T) {
		set := corpusSet(t)
		page := corpusSampleByID(t, "laravel-11-vite").Page()
		padded := make([]byte, DefaultLimits().MaxHTMLBytes+len(page.HTML))
		copy(padded[DefaultLimits().MaxHTMLBytes:], page.HTML)

		res := set.EvaluateCtx(context.Background(),
			Page{HTML: padded, Cookies: page.Cookies, Headers: page.Headers}, DefaultLimits())

		want := []string{
			"cookie_encrypted_payload",
			"cookie_laravel_session",
			"cookie_session_suffix",
			"cookie_xsrf_token",
		}
		if fired := corpusFired(res); !slices.Equal(fired, want) {
			t.Fatalf("сработали %v, ожидались только куки %v", fired, want)
		}
		if wantScore := ScoreFixed([]float64{0.5, 0.35, 0.15, 0.25}); diff(res.Score, wantScore) > 1e-9 {
			t.Errorf("score = %v, want %v", res.Score, wantScore)
		}
		if !res.IsLaravel(DefaultMinScore) {
			t.Errorf("куки дают уверенное срабатывание даже с обрезанным HTML: %.3f", res.Score)
		}
	})

	// The truncation boundary byte by byte: a sign whose last byte is
	// exactly at the limit is still visible; one byte further — it is not.
	// The sign weight of 0.6 — the truncation directly decides is_laravel.
	t.Run("маркер на границе лимита", func(t *testing.T) {
		set := evalSet(t, "rules:\n  - id: needle\n    target: html\n    regex: NEEDLE\n    weight: 0.6\n")
		lim := DefaultLimits()
		lim.MaxHTMLBytes = 100

		// NEEDLE (6 bytes) ends exactly at the byte with index 99.
		in := append([]byte(strings.Repeat(" ", 100-6)), []byte("NEEDLE")...)
		res := set.EvaluateCtx(context.Background(), Page{HTML: in}, lim)
		if len(res.Signals) != 1 || !res.IsLaravel(DefaultMinScore) {
			t.Errorf("признак целиком в пределах лимита должен срабатывать: %+v", res)
		}

		// NEEDLE's trailing byte beyond the limit — no match.
		out := append([]byte(strings.Repeat(" ", 101-6)), []byte("NEEDLE")...)
		res = set.EvaluateCtx(context.Background(), Page{HTML: out}, lim)
		if len(res.Signals) != 0 || res.Score != 0 || res.IsLaravel(DefaultMinScore) {
			t.Errorf("признак за пределами лимита не должен находиться: %+v", res)
		}
	})
}

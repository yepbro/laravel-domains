package rules

import (
	"math"
	"regexp"
	"strconv"
	"strings"
)

// Cookie — cookie со страницы (достаточно для пассивного анализа).
type Cookie struct {
	Name  string
	Value string
}

// Page — данные, по которым применяются правила: контент, cookies и заголовки.
type Page struct {
	HTML    []byte
	Cookies []Cookie
	Headers map[string][]string // имена в канонической форме MIME-заголовка
}

// Evidence — сработавшее правило и краткое свидетельство.
type Evidence struct {
	Rule     string
	Weight   float64
	Evidence string
}

// Result — итог применения набора правил к странице.
type Result struct {
	Score    float64 // noisy-OR по сработавшим правилам
	Signals  []Evidence
	VerMin   string
	VerMax   string
	Conflict bool // пересечение диапазонов версий пустое
}

var metaTagRe = regexp.MustCompile(`(?is)<meta\b[^>]*>`)

// Evaluate применяет все правила набора к странице.
func (s *Set) Evaluate(page Page) Result {
	res := Result{}

	var (
		minStr, maxStr   string
		minV, maxV       version
		haveMin, haveMax bool
	)

	for _, r := range s.Rules {
		ev, ok := matchRule(r, page)
		if !ok {
			continue
		}
		res.Score = noisyOR(res.Score, r.Weight)
		res.Signals = append(res.Signals, Evidence{Rule: r.ID, Weight: r.Weight, Evidence: ev})

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
		res.VerMin = minStr
	}
	if haveMax {
		res.VerMax = maxStr
	}
	if haveMin && haveMax && maxV.less(minV) {
		res.Conflict = true
		res.VerMin, res.VerMax = "", ""
	}
	return res
}

func matchRule(r Rule, page Page) (string, bool) {
	regex, err := regexp.Compile(r.Regex)
	if r.Regex == "" {
		regex = nil
	} else if err != nil {
		return "", false // некорректные выражения отсеиваются при загрузке
	}
	matches := func(s string) bool {
		if r.Match != "" {
			return s == r.Match
		}
		return regex != nil && regex.MatchString(s)
	}
	snippet := func(s string) string {
		if len(s) > 64 {
			return s[:64] + "…"
		}
		return s
	}

	switch r.Target {
	case TargetCookieName:
		for _, c := range page.Cookies {
			if matches(c.Name) {
				return "cookie " + c.Name, true
			}
		}
	case TargetCookieValue:
		for _, c := range page.Cookies {
			if regex != nil && regex.MatchString(c.Value) {
				return "cookie " + c.Name + "=" + snippet(c.Value), true
			}
		}
	case TargetHeaderName:
		for name := range page.Headers {
			if matches(name) {
				return "header " + name, true
			}
		}
	case TargetHeaderValue:
		for name, vals := range page.Headers {
			for _, v := range vals {
				if regex != nil && regex.MatchString(v) {
					return "header " + name + ": " + snippet(v), true
				}
			}
		}
	case TargetHTML:
		if regex != nil {
			if m := regex.Find(page.HTML); m != nil {
				return snippet(string(m)), true
			}
		}
	case TargetMeta:
		if regex != nil {
			for _, tag := range metaTagRe.FindAllString(string(page.HTML), -1) {
				if regex.MatchString(tag) {
					return snippet(tag), true
				}
			}
		}
	}
	return "", false
}

// noisyOR: 1 − (1−a)(1−b) — слабые признаки накапливаются, но не дают 100%.
func noisyOR(a, b float64) float64 {
	return 1 - (1-a)*(1-b)
}

// ScoreFixed — noisy-OR по набору весов.
func ScoreFixed(weights []float64) float64 {
	s := 0.0
	for _, w := range weights {
		s = noisyOR(s, w)
	}
	return s
}

// ---------------------------------------------------------------- версии

// version — числовые компоненты версии; "x" означает +∞ (граница «до конца
// мажорной ветки», например ver_max: "9.x"). Отсутствующий компонент — 0.
type version []float64

func parseVersion(s string) version {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ".")
	out := make(version, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "x" || p == "*" {
			out = append(out, math.Inf(1))
			continue
		}
		n, err := strconv.ParseFloat(p, 64)
		if err != nil {
			return nil // некорректная версия игнорируется
		}
		out = append(out, n)
	}
	return out
}

func (v version) less(o version) bool {
	for i := 0; i < len(v) || i < len(o); i++ {
		var a, b float64
		if i < len(v) {
			a = v[i]
		}
		if i < len(o) {
			b = o[i]
		}
		if a != b {
			return a < b
		}
	}
	return false
}

// Contains — попадает ли ожидаемый диапазон [expMin, expMax] внутрь
// вычисленного (используется make test-rules).
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

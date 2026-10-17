package rules

import (
	"regexp"
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

	var matched []Rule
	for _, r := range s.Rules {
		ev, ok := matchRule(r, page)
		if !ok {
			continue
		}
		res.Signals = append(res.Signals, Evidence{Rule: r.ID, Weight: r.Weight, Evidence: ev})
		matched = append(matched, r)
	}

	res.Score, res.VerMin, res.VerMax, res.Conflict = ScoreRules(matched)
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

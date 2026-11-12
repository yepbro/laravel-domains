package rules

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"sync"
)

// Cookie — a page cookie (sufficient for passive analysis).
type Cookie struct {
	Name  string
	Value string
}

// Page — the data the rules are applied to: content, cookies and headers.
type Page struct {
	HTML    []byte
	Cookies []Cookie
	Headers map[string][]string // names in canonical MIME header form
}

// Evidence — a hit rule and a brief evidence snippet; the format of the
// laravel_findings.signals column (jsonb, migration 000001: [{rule, weight, evidence}]).
type Evidence struct {
	Rule     string
	Weight   float64
	Evidence string
}

// Result — the outcome of applying a rule set to a page.
type Result struct {
	Score    float64 // noisy-OR over the hit rules
	Signals  []Evidence
	VerMin   string
	VerMax   string
	Conflict bool // the version range intersection is empty
	// Partial — the scan was interrupted by ctx (EvalTimeout/cancellation):
	// not all rules were scanned, the outcome is incomplete. A consumer
	// persisting the result (a worker) must not record it as complete (AN-09).
	Partial bool
}

// metaTagRe cuts the meta tags out of the HTML. The closing ">" is looked
// up outside the quoted attribute values: the "not > and not a quote"
// branch alternates with quoted chunks ("…" / '…'), so content="a>b" does
// not terminate the tag in the middle of a value (AN-08). An unterminated
// quote means no tag is extracted at all, rather than a truncated fragment.
// Matching is linear (RE2, no backtracking).
var metaTagRe = regexp.MustCompile(`(?is)<meta\b(?:[^>"']|"[^"]*"|'[^']*')*>`)

// errPatternTooLong — the pattern fails the Limits.MaxPatternBytes bound
// (spec §9: untrusted input is bounded; the rule is skipped silently, just
// like a non-compilable one — set validation at load time is T-068, the
// engine guards itself as well).
var errPatternTooLong = errors.New("rules: pattern is longer than the limit")

// Evaluate applies all the rules of the set to a page with the default
// limits (DefaultLimits, spec §9). The analyzer's main entry point.
func (s *Set) Evaluate(page Page) Result {
	return s.EvaluateCtx(context.Background(), page, DefaultLimits())
}

// EvaluateCtx applies the rules with an explicit context and limits
// (the analyzer sets the cancellation budget for shutdown/consumption,
// tests use narrow limits). Zero Limits enable DefaultLimits wholesale;
// the context is checked between rules and between cookie/header values;
// on cancellation the scan stops, the result keeps what hit before the
// cancellation and is marked Partial (it must not be treated as complete).
func (s *Set) EvaluateCtx(ctx context.Context, page Page, lim Limits) Result {
	if lim == (Limits{}) {
		lim = DefaultLimits()
	}
	if lim.EvalTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, lim.EvalTimeout)
		defer cancel()
	}

	res := Result{}

	var matched []Rule
	for _, r := range s.Rules {
		if ctx.Err() != nil {
			res.Partial = true
			// An equivalent mutant (T-M12): continuing the loop after the
			// cancellation is fruitless — matchRule rejects every rule on
			// the same ctx at its input, no new signals appear, context
			// cancellation is irreversible; the final Result is identical
			// (the only difference is the extra iterations).
			// mutator-disable-next-line loop/break
			break // the time budget is exhausted — what was found is kept (incomplete)
		}
		ev, ok := matchRule(ctx, r, page, lim)
		if !ok {
			continue
		}
		res.Signals = append(res.Signals, Evidence{Rule: r.ID, Weight: r.Weight, Evidence: ev})
		matched = append(matched, r)
	}

	res.Score, res.VerMin, res.VerMax, res.Conflict = ScoreRules(matched)
	return res
}

// compileCache caches compiled patterns (sync.Map — sets are reloaded on
// SIGHUP without a restart, Evaluate is called by parallel worker
// goroutines): without the cache every page would pay for compiling every
// rule. Compilation errors are cached too — an invalid pattern is not
// recompiled on every call. The cache lifetime is the current set: a
// rules_version change clears it (resetCompileCache from Reloader.Reload),
// so entries of removed patterns and settled errors do not accumulate for
// the whole lifetime of the process.
var compileCache sync.Map // string → compileResult

// resetCompileCache clears the compilation cache when the rule set changes:
// patterns (and cached compilation errors) do not outlive the set that
// produced them. Safe under parallel Evaluate — a concurrent compileCached
// on a miss simply recompiles the pattern.
func resetCompileCache() {
	compileCache.Clear()
}

type compileResult struct {
	re  *regexp.Regexp
	err error
}

func compileCached(pattern string) (*regexp.Regexp, error) {
	if v, ok := compileCache.Load(pattern); ok {
		cr := v.(compileResult)
		return cr.re, cr.err
	}
	re, err := regexp.Compile(pattern) // RE2: linear matching, no backtracking
	compileCache.Store(pattern, compileResult{re, err})
	return re, err
}

// ruleRegex compiles a rule pattern subject to the limits; a nil regex
// means an exact-match rule (match) or a skippable one (limit/error).
func ruleRegex(r Rule, lim Limits) (*regexp.Regexp, error) {
	if r.Regex == "" {
		return nil, nil
	}
	if !lim.patternAllowed(r.Regex) {
		return nil, errPatternTooLong
	}
	return compileCached(r.Regex)
}

// sortedHeaderNames — a deterministic header name order: map iteration in
// Go is random, while re-analysis evidence must be reproducible.
func sortedHeaderNames(h map[string][]string) []string {
	names := make([]string, 0, len(h))
	for name := range h {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// matchRule looks up the first match of a rule on the page: cookie
// name/value and headers use an exact match (match) or a regex; html/meta
// use a regex only (spec 5.8: "a regular expression over HTML"). Returns
// the evidence of the hit match — it is built from the same limit-capped
// strings the matching ran on (evidence work is bounded by MaxValueBytes,
// spec §9). The traversal order is deterministic (see evidence.go).
func matchRule(ctx context.Context, r Rule, page Page, lim Limits) (string, bool) {
	if ctx.Err() != nil {
		return "", false // the budget is exhausted — the rule is not examined
	}
	regex, err := ruleRegex(r, lim)
	if err != nil {
		return "", false // invalid/bloated expressions are filtered out
	}
	matches := func(s string) bool {
		if r.Match != "" {
			return s == r.Match
		}
		return regex != nil && regex.MatchString(s)
	}

	switch r.Target {
	case TargetCookieName:
		for _, c := range page.Cookies {
			if ctx.Err() != nil {
				return "", false
			}
			name := lim.valueCap(c.Name)
			if matches(name) {
				return finalizeEvidence(evidenceCookieName(name), lim), true
			}
		}
	case TargetCookieValue:
		for _, c := range page.Cookies {
			if ctx.Err() != nil {
				return "", false
			}
			value := lim.valueCap(c.Value)
			if matches(value) {
				return finalizeEvidence(evidenceCookieValue(c.Name, value), lim), true
			}
		}
	case TargetHeaderName:
		for _, name := range sortedHeaderNames(page.Headers) {
			if ctx.Err() != nil {
				return "", false
			}
			capped := lim.valueCap(name)
			if matches(capped) {
				return finalizeEvidence(evidenceHeaderName(capped), lim), true
			}
		}
	case TargetHeaderValue:
		for _, name := range sortedHeaderNames(page.Headers) {
			for _, v := range page.Headers[name] {
				if ctx.Err() != nil {
					return "", false
				}
				value := lim.valueCap(v)
				if matches(value) {
					return finalizeEvidence(evidenceHeaderValue(name, value), lim), true
				}
			}
		}
	case TargetHTML:
		if regex != nil {
			// the input is capped before matching (spec §9); Find returns
			// the first fragment — that fragment is the evidence
			if m := regex.Find(lim.htmlCap(page.HTML)); m != nil {
				return finalizeEvidence(string(m), lim), true
			}
		}
	case TargetMeta:
		if regex != nil {
			// sliding search for meta tags without materializing all
			// matches: exit on the first hit, each step moves the position
			// past the end of the previous tag
			html := lim.htmlCap(page.HTML)
			pos := 0
			// An equivalent mutant (T-M12): the iteration at pos == len(html)
			// searches an empty slice — FindIndex returns nil and the loop
			// exits with the same break.
			// mutator-disable-next-line expression/comparison
			for pos < len(html) {
				if ctx.Err() != nil {
					return "", false
				}
				loc := metaTagRe.FindIndex(html[pos:])
				if loc == nil {
					break
				}
				tag := html[pos+loc[0] : pos+loc[1]]
				if regex.Match(tag) {
					return finalizeEvidence(string(tag), lim), true
				}
				pos += loc[1]
			}
		}
	}
	return "", false
}

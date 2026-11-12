// Engine limits on untrusted content (spec §9: "page content is considered
// untrusted: all parsers have size and time limits"). The format of the
// engine inputs is bounded already at the fetch and render stages (HTTP:
// body and header limits; storage: content.max_store_bytes, spec 3.2 —
// the 1–2 MB recommendation) — the limits below are defense in depth:
// the engine must terminate in finite time on any input.
//
// Why the limits look this way (the ReDoS decisions):
//
//   - Go regexp is an RE2 implementation: matching is linear in the input
//     length and has no catastrophic backtracking; a "hanging" expression
//     like (a+)+$ against aaaa…b is impossible here. Two vectors remain:
//     a bloated input (linear time still grows) and a bloated pattern
//     (compilation and the number of states). Both are closed by size
//     limits.
//
//   - MaxHTMLBytes — the bound on the HTML/DOM being scanned. Laravel
//     signs (meta csrf-token, vite/mix build paths, data-page, wire:*)
//     live in the head part of the document — the first dozens of
//     kilobytes; the tail beyond the limit is capped before matching.
//     The default of 2 MiB is the upper bound of the spec's content size
//     recommendation (1–2 MB): everything storage keeps per config is
//     usually seen by the engine in full.
//
//   - MaxValueBytes — the bound on a single cookie/header value. Laravel
//     values (encrypted cookies) are a few kilobytes; all value rules are
//     anchored to the start of the string (^…), so truncation is invisible
//     to them. The price of the limit: a rule anchored to the end ($…)
//     will not hit a value longer than the limit — a deliberate trade of
//     completeness for the time guarantee.
//
//   - MaxPatternBytes — the bound on the length of a single rule's regex
//     pattern: rules are written by humans, kilobyte-scale patterns are a
//     mistake or abuse; compiling a bloated pattern in RE2 can cost orders
//     of magnitude more than the matching itself.
//
//   - EvalTimeout — the overall budget for applying a rule set to a page.
//     The context is checked between rules and between cookie/header
//     values (a single match cannot be interrupted, but it is linear in
//     the already capped input), so cancellation takes effect within one
//     match: microseconds on real pages.
package rules

import "time"

// Limits — the limits of a single EvaluateCtx call. The zero value (all
// fields zero) enables DefaultLimits: limits cannot be forgotten —
// protection by default. An individual field ≤ 0 disables the
// corresponding limit (except MaxSnippetBytes — the evidence is always
// brief, 64 bytes); that is a tool for tests and narrow scenarios.
type Limits struct {
	MaxHTMLBytes    int           // the bound on the HTML/DOM being scanned
	MaxValueBytes   int           // the bound on a single cookie/header value
	MaxPatternBytes int           // the bound on a rule's regex pattern length
	MaxSnippetBytes int           // the evidence (Evidence) bound in bytes
	EvalTimeout     time.Duration // the time budget for the whole page
}

// DefaultLimits — the default values (see the rationale above).
func DefaultLimits() Limits {
	return Limits{
		MaxHTMLBytes:    2 << 20,  // 2 MiB
		MaxValueBytes:   64 << 10, // 64 KiB
		MaxPatternBytes: 4 << 10,  // 4 KiB
		MaxSnippetBytes: 64,       // brief evidence, truncation + "…"
		EvalTimeout:     5 * time.Second,
	}
}

// htmlCap applies MaxHTMLBytes (called per rule — cheap; the protection
// also works when matchRule is called directly without EvaluateCtx).
func (l Limits) htmlCap(b []byte) []byte {
	if l.MaxHTMLBytes > 0 && len(b) > l.MaxHTMLBytes {
		return b[:l.MaxHTMLBytes]
	}
	return b
}

// valueCap applies MaxValueBytes to a string value (cookie/header).
func (l Limits) valueCap(s string) string {
	if l.MaxValueBytes > 0 && len(s) > l.MaxValueBytes {
		return s[:l.MaxValueBytes]
	}
	return s
}

// patternAllowed — whether the pattern passes the MaxPatternBytes bound.
func (l Limits) patternAllowed(pattern string) bool {
	return l.MaxPatternBytes <= 0 || len(pattern) <= l.MaxPatternBytes
}

// snippetCap — the evidence bound (64 by default when the field is zero).
func (l Limits) snippetCap() int {
	if l.MaxSnippetBytes > 0 {
		return l.MaxSnippetBytes
	}
	return 64
}

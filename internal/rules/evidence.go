// Rule hit evidence — a brief fragment of "what exactly was found" for the
// jsonb laravel_findings.signals (migration 000001:
// [{rule, weight, evidence}, …]). Requirements: brevity (truncated to N
// bytes), single-line readability and storage safety — the content is
// untrusted (spec §9), the evidence must not drag along raw bulky chunks,
// control characters or invalid UTF-8.
package rules

import (
	"strings"
	"unicode/utf8"
)

// evidenceNameBytes — the limit for a cookie/header name inside the
// evidence: so that a hostilely long name does not push the value out of
// the truncation window. Applied to all evidence forms carrying a name —
// name-only as well (docs/analyzer.md "Evidence").
const evidenceNameBytes = 32

// evidence formats per target (a stable contract, T-070+ writes them to
// jsonb):
//
//	cookie_name   → "cookie XSRF-TOKEN"
//	cookie_value  → "cookie laravel_session=eyJpdiI6…"
//	header_name   → "header X-Powered-By"
//	header_value  → "header X-Powered-By: PHP/8.2"
//	html          → the matched fragment of the regular expression
//	meta          → the whole meta tag (truncated by the evidence limit)
//
// There is one evidence per rule hit — the first match in the deterministic
// traversal order (cookies in write order; header names are sorted — map
// iteration in Go is non-deterministic, and the re-analysis result must be
// reproducible).
func evidenceCookieName(name string) string {
	return "cookie " + capName(name)
}

func evidenceCookieValue(name, value string) string {
	return "cookie " + capName(name) + "=" + value
}

func evidenceHeaderName(name string) string {
	return "header " + capName(name)
}

func evidenceHeaderValue(name, value string) string {
	return "header " + capName(name) + ": " + value
}

// capName caps a name compactly inside the evidence (no ellipsis — the
// final truncation of the whole string is done once by finalizeEvidence).
func capName(name string) string {
	if len(name) <= evidenceNameBytes {
		return name
	}
	return truncateUTF8(name, evidenceNameBytes)
}

// finalizeEvidence brings a raw evidence to its stored form: invalid
// UTF-8 → U+FFFD (json.Marshal would do the same on output — we fix it
// here so the evidence is valid already in memory), control characters
// (C0, C1, DEL) → "·" (a single line without terminal escape
// sequences), truncation to the limit at a rune boundary with an
// ellipsis.
func finalizeEvidence(s string, lim Limits) string {
	s = cleanEvidence(s)
	max := lim.snippetCap()
	if len(s) <= max {
		return s
	}
	return truncateUTF8(s, max) + "…"
}

// controlCutset — the sanitized control runes for the fast path: C0
// (U+0000..U+001F), DEL and the C1 range (U+0080..U+009F). Built in
// code: strings.ContainsAny matches UTF-8-encoded code points, and after
// ToValidUTF8 every C1 code point is exactly the bytes C2 80..C2 9F —
// the cutset is precise, a page with ordinary multibyte text
// (continuation bytes 80..BF of OTHER runes) does not fall through to
// the slow path.
var controlCutset = buildControlCutset()

func buildControlCutset() string {
	var b strings.Builder
	for r := rune(0); r < 0x20; r++ {
		b.WriteRune(r)
	}
	b.WriteRune(0x7f)
	for r := rune(0x80); r <= 0x9f; r++ {
		b.WriteRune(r)
	}
	return b.String()
}

// cleanEvidence sanitizes untrusted text: valid UTF-8, no control
// characters (C0, C1, DEL). The C1 range U+0080..U+009F (Unicode
// category Cc; U+009B is the CSI analog) is sanitized like C0 — the
// promise "a single line without terminal escape sequences" must hold
// for it too (07#P3-3).
func cleanEvidence(s string) string {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "\uFFFD")
	}
	if !strings.ContainsAny(s, controlCutset) {
		return s // the fast path of ordinary pages
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r < 0x20 || (r >= 0x7f && r <= 0x9f) {
			r = '·'
		}
		b.WriteRune(r)
	}
	return b.String()
}

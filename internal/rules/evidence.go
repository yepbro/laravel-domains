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
// here so the evidence is valid already in memory), control characters → "·"
// (a single line without terminal escape sequences), truncation to the
// limit at a rune boundary with an ellipsis.
func finalizeEvidence(s string, lim Limits) string {
	s = cleanEvidence(s)
	max := lim.snippetCap()
	if len(s) <= max {
		return s
	}
	return truncateUTF8(s, max) + "…"
}

// cleanEvidence sanitizes untrusted text: valid UTF-8, no control
// characters (C0, DEL).
func cleanEvidence(s string) string {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "\uFFFD")
	}
	if !strings.ContainsAny(s, "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"+
		"\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x7f") {
		return s // the fast path of ordinary pages
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			r = '·'
		}
		b.WriteRune(r)
	}
	return b.String()
}

// truncateUTF8 truncates a string to at most max bytes without splitting a
// rune in half: the trailing bytes of an incomplete rune are rolled back
// (a leading byte without a continuation is as broken as a severed
// continuation).
func truncateUTF8(s string, max int) string {
	if max <= 0 {
		return ""
	}
	if len(s) <= max {
		return s
	}
	s = s[:max]
	for len(s) > 0 {
		r, size := utf8.DecodeLastRuneInString(s)
		if r == utf8.RuneError && size == 1 {
			s = s[:len(s)-1] // a broken/incomplete rune at the tail
			continue
		}
		break
	}
	return s
}

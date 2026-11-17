// truncate.go — UTF-8-safe truncation shared by the evidence trim and
// the input caps (07#P3-2): a byte-level cut can sever a multibyte rune
// at the boundary; RE2 then reads the remains as U+FFFD, and a rule with
// a multibyte literal at the boundary (e.g. an end anchor over the last
// intact rune) stops firing. The helpers roll the trailing bytes of an
// incomplete rune back, silently — the caps add no suffix (they are
// invisible input bounds), the evidence adds its own "…" (evidence.go).
package rules

import "unicode/utf8"

// truncateUTF8 truncates a string to at most max bytes without splitting
// a rune in half: the trailing bytes of an incomplete rune are rolled
// back (a leading byte without a continuation is as broken as a severed
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

// truncateUTF8Bytes is the []byte form of truncateUTF8 (the HTML/DOM
// cap): same rune-boundary rollback, no reallocation — the result
// aliases the input slice.
func truncateUTF8Bytes(b []byte, max int) []byte {
	if max <= 0 {
		return b[:0]
	}
	if len(b) <= max {
		return b
	}
	b = b[:max]
	for len(b) > 0 {
		r, size := utf8.DecodeLastRune(b)
		if r == utf8.RuneError && size == 1 {
			b = b[:len(b)-1] // a broken/incomplete rune at the tail
			continue
		}
		break
	}
	return b
}

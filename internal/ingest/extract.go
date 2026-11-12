// Extracting a domain from an arbitrary line (T-132 — an extension of
// spec 5.1, stage 16): real registrar exports and public dumps carry
// foreign data ("xxx.com 2029-11-11 TIMEWEB", CSV, URLs), not just a
// full domain name. extractCandidates decides whether a line is subject
// to extraction and collects the candidates; the name validation
// pipeline (spec 5.2 items 1–6) is unchanged — extraction only picks
// which token of the line goes into it (the integration point is
// Validator.Line).
//
// A single line — no separators and no explicit http(s) scheme — is not
// subject to extraction and goes through the previous validation path
// unchanged: zero regression for today's files, the reason accuracy for
// single lines is preserved.
package ingest

import (
	"net/url"
	"strings"
	"unicode"
)

// isSeparator — the token separator predicate (T-132): a whitespace
// character (unicode.IsSpace — the same predicate as in idn.cleanup and
// the parser), a comma or a semicolon. A space inside a line is a field
// separator of the export, not a typo in the name: gluing
// "при мер.рф" → "пример.рф" went away together with the old
// interpretation (the token with a dot becomes the candidate — мер.рф).
func isSeparator(r rune) bool {
	return unicode.IsSpace(r) || r == ',' || r == ';'
}

// hasSeparator — whether the line has at least one token separator.
func hasSeparator(s string) bool {
	return strings.IndexFunc(s, isSeparator) >= 0
}

// splitTokens — splitting a line into tokens by the isSeparator
// separators; empty tokens between adjacent separators are dropped
// (strings.FieldsFunc): "xxx.com", yields one candidate token.
func splitTokens(line string) []string {
	return strings.FieldsFunc(line, isSeparator)
}

// isQuoteOrPunct — quotes (the backtick is not in unicode.IsPunct —
// conditional, handled separately) and unicode punctuation, except
// hyphens (Pd) and connectors (Pc, the underscore): CSV framing,
// trailing dots and brackets are not part of a name, while edge "-"/"_"
// are parts of a name. Cutting them would substitute the token with
// another domain ("_dmarc.example.com" → "dmarc.example.com"): such a
// name must go into the pipeline as is and fail normalization (STD3),
// not turn into someone else's valid name.
func isQuoteOrPunct(r rune) bool {
	if r == '"' || r == '\'' || r == '`' {
		return true
	}
	return unicode.IsPunct(r) &&
		!unicode.Is(unicode.Pd, r) && !unicode.Is(unicode.Pc, r)
}

// trimToken strips quotes and punctuation from the token edges
// ("xxx.com", → xxx.com). Inner characters are not touched: dots and
// hyphens inside a name are significant, only the framing is cut.
func trimToken(tok string) string {
	return strings.TrimFunc(tok, isQuoteOrPunct)
}

// hasSchemePrefix — whether the token starts with an explicit
// http(s):// scheme — case-insensitively (EqualFold, ASCII prefixes)
// and without allocation: extraction asks this of every token, and
// ingest is streaming (spec 3.5); a ToLower per token is extra work.
func hasSchemePrefix(tok string) bool {
	for _, scheme := range [...]string{"http://", "https://"} {
		if len(tok) >= len(scheme) && strings.EqualFold(tok[:len(scheme)], scheme) {
			return true
		}
	}
	return false
}

// urlHost — the host of a token with an explicit http(s) scheme
// (hasSchemePrefix, the scheme case does not matter). A URL token is not
// a domain name, but carries one in its host: net/url separates the
// host from the scheme, userinfo, port and path (Hostname — the port is
// cut off). ok = false — the token does not start with an http(s)
// scheme: other schemes ("ftp://…") are not substituted — such a token
// stays a candidate and fails normalization. A parse error or an empty
// host — an empty string: there is no domain in the token. An IPv6 host
// contains no dots and will not become a candidate; an IPv4 (including
// IPv4-mapped) will, and will be rejected by the validation pipeline,
// like any IP literal.
func urlHost(tok string) (host string, ok bool) {
	if !hasSchemePrefix(tok) {
		return "", false
	}
	u, err := url.Parse(tok)
	if err != nil {
		return "", true
	}
	return u.Hostname(), true
}

// extractCandidates — the extraction step of T-132: returns the
// candidates (tokens containing a dot, left to right) and whether the
// line is subject to extraction. A candidate without a dot is not a
// domain (dates 2029-11-11, words); checking a candidate is the job of
// the full validation pipeline — here it is only token selection.
//
// A single line without separators: the only exception to the previous
// path is an explicit http(s) scheme (a URL token cannot be a domain,
// the previous "invalid" reason lost the host with the path). Edge
// spaces are trimmed before the decision: a line of only spaces stays
// the previous empty case (the parser does not deliver such lines, but
// the Line contract is for any call).
func extractCandidates(line string) (cands []string, extract bool) {
	tok := strings.TrimFunc(line, unicode.IsSpace)
	if !hasSeparator(tok) {
		host, isURL := urlHost(tok)
		if !isURL {
			return nil, false // the previous path: a single token
		}
		if strings.Contains(host, ".") {
			return []string{host}, true
		}
		return nil, true // a URL, but the host has no dot — no domain
	}
	for _, t := range splitTokens(tok) {
		t = trimToken(t)
		if host, isURL := urlHost(t); isURL {
			t = host // the scheme, port and path are cut off
		}
		if strings.Contains(t, ".") {
			cands = append(cands, t)
		}
	}
	return cands, true
}

// Unit tests of extracting a domain from an arbitrary line (T-132): the
// extractCandidates candidate table, the integration into Validator.Line
// (the first candidate that passes the full pipeline becomes the row's
// record; none — no_domain into the counters and the rejected log) and
// pinning the previous single-line behavior — zero reason regression.
package ingest

import (
	"strings"
	"testing"
)

// TestExtractCandidates — the extraction table: a line → the candidates
// and whether the step applies (T-132). The candidate order — left to
// right.
func TestExtractCandidates(t *testing.T) {
	tests := []struct {
		name  string
		line  string
		cands []string
		extra bool
	}{
		// Single lines: the step does not apply — the previous validation
		// path.
		{"a single domain", "example.com", nil, false},
		{"a single domain with edge spaces and a dot", "  EXAMPLE.com. ", nil, false},
		{"single junk", "под_чёркивание.com", nil, false},
		{"an empty line", "", nil, false},
		{"spaces only — the old empty case", "   ", nil, false},
		{"a single quoted token — the old path", `"example.com"`, nil, false},
		// A single URL token: an explicit scheme — the only exception to
		// the previous path for single lines.
		{"a URL with a path", "https://example.com/page", []string{"example.com"}, true},
		{"a URL with a port and a scheme case", "HTTPS://Shop.COM:8443/x", []string{"Shop.COM"}, true},
		{"a URL without a dotted host", "http://localhost/x", nil, true},
		{"a URL without a host", "http://", nil, true},
		{"a URL with a broken port — a url.Parse error", "http://example.com:bad/x", nil, true},
		{"an IPv6 URL without dots", "https://[::1]/x", nil, true},
		{"an IPv4 URL — a candidate, validation will reject it", "http://1.2.3.4/x", []string{"1.2.3.4"}, true},
		// Multi-word lines: separators, quotes, punctuation, URLs.
		{"a domain with a date and a registrar", "xxx.com 2029-11-11 TIMEWEB", []string{"xxx.com"}, true},
		{"tabs (TSV)", "xxx.com\t2029-11-11\tTIMEWEB", []string{"xxx.com"}, true},
		{"quotes and a comma (the order example)", `"xxx.com",`, []string{"xxx.com"}, true},
		{"quoted CSV", `"xxx.com","2029-11-11","TIMEWEB"`, []string{"xxx.com"}, true},
		{"semicolons", "xxx.com;2029-11-11;TIMEWEB", []string{"xxx.com"}, true},
		{"parentheses and a trailing dot", "(example.com), дата", []string{"example.com"}, true},
		{"guillemets", "«пример.рф» 2029-11-11", []string{"пример.рф"}, true},
		{"extra separators", ",;xxx.com;,,", []string{"xxx.com"}, true},
		{"a URL among words", "см. https://example.com/page там", []string{"example.com"}, true},
		{"a URL with a port in a mixed line", "https://example.com:8080/x y.org", []string{"example.com", "y.org"}, true},
		{"a URL with userinfo and a path", "https://user:pw@example.com/a?b=1", []string{"example.com"}, true},
		{"an IDN URL", "https://пример.рф/page", []string{"пример.рф"}, true},
		{"a dotless host in a mixed line", "a.b http://localhost/x", []string{"a.b"}, true},
		{"two candidates left to right", "yyy.org xxx.com", []string{"yyy.org", "xxx.com"}, true},
		// Edge hyphens and underscores are parts of a name, not framing:
		// cutting them would substitute the token with another domain
		// (the T-132 review, P2-1).
		{"an edge underscore is not stripped", "_dmarc.example.com пометка", []string{"_dmarc.example.com"}, true},
		{"an edge hyphen is not stripped", "-my-shop.com пометка", []string{"-my-shop.com"}, true},
		// Dates and words without dots are not candidates.
		{"dates and dotless words", "2029-11-11 TIMEWEB регистратор", nil, true},
		{"dotless words", "нет домена тут", nil, true},
		// A foreign scheme is not substituted: the token stays a candidate
		// and will fail in normalization (only net/url cuts the port, for
		// an explicit http(s)).
		{"a foreign scheme stays a token", "ftp://example.com/x y.com", []string{"ftp://example.com/x", "y.com"}, true},
		{"a host:port without a scheme is not parsed", "example.com:8443 пометка", []string{"example.com:8443"}, true},
		// A space inside a line is a field separator (T-132): typos are
		// not glued anymore, the token with a dot becomes the candidate.
		{"an inner space — a token with a dot", "при мер.рф", []string{"мер.рф"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cands, extract := extractCandidates(tc.line)
			if extract != tc.extra {
				t.Fatalf("extract = %v, want %v (the line %q)", extract, tc.extra, tc.line)
			}
			if len(cands) != len(tc.cands) {
				t.Fatalf("the candidates = %v, want %v", cands, tc.cands)
			}
			for i := range tc.cands {
				if cands[i] != tc.cands[i] {
					t.Errorf("candidate[%d] = %q, want %q", i, cands[i], tc.cands[i])
				}
			}
		})
	}
}

// TestLineExtract — the extraction integration into Validator.Line
// (T-132): the first candidate that passes the full pipeline becomes the
// row's record; none passed — no_domain. Single lines — the previous
// reasons (zero regression).
func TestLineExtract(t *testing.T) {
	tests := []struct {
		name   string
		zone   string
		line   string
		entry  Entry  // the expected record (ok = true)
		reason Reason // the expected reason (ok = false)
	}{
		// The customer's example: a registrar export.
		{"a domain with a date and a registrar", "com", "xxx.com 2029-11-11 TIMEWEB", Entry{"com", "xxx", false}, ""},
		// A URL with a path (the port is cut off).
		{"a URL with a path", "com", "https://xxx.com/page", Entry{"com", "xxx", false}, ""},
		{"a URL with a port", "com", "https://xxx.com:8443/page", Entry{"com", "xxx", false}, ""},
		// IDN in a mixed line and in a URL — the full normalization of
		// spec 5.2.
		{"an IDN in a mixed line", "рф", "пример.рф 2029-11-11 TIMEWEB", Entry{"xn--p1ai", "xn--e1afmkfd", true}, ""},
		{"an IDN URL in uppercase", "рф", "https://ПРИМЕР.РФ/page", Entry{"xn--p1ai", "xn--e1afmkfd", true}, ""},
		// Quotes, commas, semicolons.
		{"quotes and a comma", "com", `"xxx.com",`, Entry{"com", "xxx", false}, ""},
		{"quoted CSV", "com", `"xxx.com","2029-11-11","TIMEWEB"`, Entry{"com", "xxx", false}, ""},
		{"semicolons", "com", "xxx.com; 2029-11-11; TIMEWEB", Entry{"com", "xxx", false}, ""},
		{"parentheses and a dash", "com", "(xxx.com) — пометка", Entry{"com", "xxx", false}, ""},
		// The candidate order: the first is valid but of a foreign zone —
		// the next one is taken; the first invalid — the same.
		{"the first candidate from a foreign zone", "com", "yyy.org xxx.com", Entry{"com", "xxx", false}, ""},
		{"the first candidate is broken punycode", "com", "xn--bcher--kva.com ok.com", Entry{"com", "ok", false}, ""},
		{"a composite path zone", "co.uk", "yyy.org example.co.uk", Entry{"co.uk", "example", false}, ""},
		// None passed: no_domain.
		{"dotless words", "com", "нет домена тут", Entry{}, ReasonNoDomain},
		{"dates and dotless words", "com", "2029-11-11 TIMEWEB", Entry{}, ReasonNoDomain},
		{"the only candidate from a foreign zone", "com", "yyy.org регистратор", Entry{}, ReasonNoDomain},
		{"a URL without a dotted host", "com", "http://localhost/x", Entry{}, ReasonNoDomain},
		{"a URL with a broken port — no parse error, no host", "com", "http://example.com:bad/x", Entry{}, ReasonNoDomain},
		// Edge "-"/"_" are not cut: the candidate stays broken and fails
		// normalization instead of being substituted with another domain.
		{"a leading underscore — not a foreign name", "com", "_dmarc.example.com пометка", Entry{}, ReasonNoDomain},
		// Pinning the previous single-line behavior: the reasons are
		// unchanged (zero regression, T-132).
		{"a single domain", "com", "example.com", Entry{"com", "example", false}, ""},
		{"a single foreign zone", "com", "example.org", Entry{}, ReasonWrongZone},
		{"single junk", "com", "под_чёркивание.com", Entry{}, ReasonInvalid},
		{"a single quoted token — the old invalid", "com", `"example.com"`, Entry{}, ReasonInvalid},
		{"single spaces — the old empty", "com", "   ", Entry{}, ReasonEmpty},
		{"a single bare zone — the old no_label", "com", "com", Entry{}, ReasonNoLabel},
		// A behavior change (T-132): an inner space is a field separator;
		// the gluing "при мер.рф" → "пример.рф" is gone.
		{"an inner space — the dotted token extracted", "рф", "при мер.рф", Entry{"xn--p1ai", "xn--e1ann", true}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v, _ := newValidator(t, tc.zone, 0)
			entry, reason, ok := v.Line(5, tc.line)
			if tc.reason == "" {
				// An acceptance and a specific record are expected.
				if !ok {
					t.Fatalf("Line(%q) rejected: %s", tc.line, reason)
				}
				if reason != "" {
					t.Errorf("the reason of a valid line: %q", reason)
				}
				if entry != tc.entry {
					t.Errorf("the entry = %+v, want %+v", entry, tc.entry)
				}
				return
			}
			if ok {
				t.Fatalf("Line(%q) accepted: %+v", tc.line, entry)
			}
			if reason != tc.reason {
				t.Errorf("the reason = %s, want %s", reason, tc.reason)
			}
			if entry != (Entry{}) {
				t.Errorf("a rejected line returned an entry: %+v", entry)
			}
		})
	}
}

// TestLineNoDomainStats — no_domain in the counters and the reason
// breakdown of the report (Reasons, spec 5.1) and in the rejected log
// records (the format — T-016).
func TestLineNoDomainStats(t *testing.T) {
	v, buf := newValidator(t, "com", 0)
	lines := []string{
		"xxx.com 2029-11-11 TIMEWEB", // accepted
		"нет домена тут",             // no_domain
		"2029-11-11 TIMEWEB",         // no_domain (no candidates)
	}
	for i, line := range lines {
		v.Line(i+1, line)
	}
	res := v.Finish()
	if res.Total != 3 || res.Accepted != 1 || res.Rejected != 2 {
		t.Errorf("the counters: total=%d accepted=%d rejected=%d, want 3/1/2",
			res.Total, res.Accepted, res.Rejected)
	}
	wantReasons := map[Reason]int{ReasonNoDomain: 2}
	if len(res.Reasons) != len(wantReasons) {
		t.Fatalf("the reasons %+v, want %+v", res.Reasons, wantReasons)
	}
	for r, n := range wantReasons {
		if res.Reasons[r] != n {
			t.Errorf("the reason %s: %d, want %d", r, res.Reasons[r], n)
		}
	}
	// no_domain — a full-fledged rejection: it counts toward the error
	// share threshold like the other reasons (2 of 3 > 0.1 — the file is
	// bad; no new counters, added/rejected are enough).
	if !res.Failed {
		t.Errorf("the no_domain share 2 of 3 is above the 0.1 threshold — the file must be bad: %+v", res)
	}

	// The rejected log: two records with the no_domain reason and the
	// original text (slog quotes values with spaces).
	logged := buf.String()
	for _, want := range []string{
		`line=2`,
		`text="нет домена тут"`,
		`text="2029-11-11 TIMEWEB"`,
		`reason=no_domain`,
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("the rejected log = %q, want a record with %q", logged, want)
		}
	}
	if n := strings.Count(logged, "reason=no_domain"); n != 2 {
		t.Errorf("no_domain records = %d, want 2 (the log:\n%s)", n, logged)
	}
}

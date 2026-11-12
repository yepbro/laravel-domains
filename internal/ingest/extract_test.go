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
		{"одиночный домен", "example.com", nil, false},
		{"одиночный домен с краевыми пробелами и точкой", "  EXAMPLE.com. ", nil, false},
		{"одиночный мусор", "под_чёркивание.com", nil, false},
		{"пустая строка", "", nil, false},
		{"только пробелы — прежний случай empty", "   ", nil, false},
		{"одиночный токен в кавычках — прежний путь", `"example.com"`, nil, false},
		// A single URL token: an explicit scheme — the only exception to
		// the previous path for single lines.
		{"URL с путём", "https://example.com/page", []string{"example.com"}, true},
		{"URL с портом и регистром схемы", "HTTPS://Shop.COM:8443/x", []string{"Shop.COM"}, true},
		{"URL без точечного хоста", "http://localhost/x", nil, true},
		{"URL без хоста", "http://", nil, true},
		{"URL с битым портом — ошибка url.Parse", "http://example.com:bad/x", nil, true},
		{"IPv6 URL без точек", "https://[::1]/x", nil, true},
		{"IPv4 URL — кандидат, отвергнет валидация", "http://1.2.3.4/x", []string{"1.2.3.4"}, true},
		// Multi-word lines: separators, quotes, punctuation, URLs.
		{"домен с датой и регистратором", "xxx.com 2029-11-11 TIMEWEB", []string{"xxx.com"}, true},
		{"табы (TSV)", "xxx.com\t2029-11-11\tTIMEWEB", []string{"xxx.com"}, true},
		{"кавычки и запятая (пример заказа)", `"xxx.com",`, []string{"xxx.com"}, true},
		{"CSV в кавычках", `"xxx.com","2029-11-11","TIMEWEB"`, []string{"xxx.com"}, true},
		{"точки с запятой", "xxx.com;2029-11-11;TIMEWEB", []string{"xxx.com"}, true},
		{"скобки и концевая точка", "(example.com), дата", []string{"example.com"}, true},
		{"ёлочки-кавычки", "«пример.рф» 2029-11-11", []string{"пример.рф"}, true},
		{"лишние разделители", ",;xxx.com;,,", []string{"xxx.com"}, true},
		{"URL среди слов", "см. https://example.com/page там", []string{"example.com"}, true},
		{"URL с портом в смешанной строке", "https://example.com:8080/x y.org", []string{"example.com", "y.org"}, true},
		{"URL с userinfo и путём", "https://user:pw@example.com/a?b=1", []string{"example.com"}, true},
		{"IDN URL", "https://пример.рф/page", []string{"пример.рф"}, true},
		{"хост без точки в смешанной строке", "a.b http://localhost/x", []string{"a.b"}, true},
		{"два кандидата слева направо", "yyy.org xxx.com", []string{"yyy.org", "xxx.com"}, true},
		// Edge hyphens and underscores are parts of a name, not framing:
		// cutting them would substitute the token with another domain
		// (the T-132 review, P2-1).
		{"краевое подчёркивание не срезается", "_dmarc.example.com пометка", []string{"_dmarc.example.com"}, true},
		{"краевой дефис не срезается", "-my-shop.com пометка", []string{"-my-shop.com"}, true},
		// Dates and words without dots are not candidates.
		{"даты и слова без точек", "2029-11-11 TIMEWEB регистратор", nil, true},
		{"слова без точек", "нет домена тут", nil, true},
		// A foreign scheme is not substituted: the token stays a candidate
		// and will fail in normalization (only net/url cuts the port, for
		// an explicit http(s)).
		{"чужая схема остаётся токеном", "ftp://example.com/x y.com", []string{"ftp://example.com/x", "y.com"}, true},
		{"хост:порт без схемы не разбирается", "example.com:8443 пометка", []string{"example.com:8443"}, true},
		// A space inside a line is a field separator (T-132): typos are
		// not glued anymore, the token with a dot becomes the candidate.
		{"внутренний пробел — токен с точкой", "при мер.рф", []string{"мер.рф"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cands, extract := extractCandidates(tc.line)
			if extract != tc.extra {
				t.Fatalf("extract = %v, want %v (строка %q)", extract, tc.extra, tc.line)
			}
			if len(cands) != len(tc.cands) {
				t.Fatalf("кандидаты = %v, want %v", cands, tc.cands)
			}
			for i := range tc.cands {
				if cands[i] != tc.cands[i] {
					t.Errorf("кандидат[%d] = %q, want %q", i, cands[i], tc.cands[i])
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
		{"домен с датой и регистратором", "com", "xxx.com 2029-11-11 TIMEWEB", Entry{"com", "xxx", false}, ""},
		// A URL with a path (the port is cut off).
		{"URL с путём", "com", "https://xxx.com/page", Entry{"com", "xxx", false}, ""},
		{"URL с портом", "com", "https://xxx.com:8443/page", Entry{"com", "xxx", false}, ""},
		// IDN in a mixed line and in a URL — the full normalization of
		// spec 5.2.
		{"IDN в смешанной строке", "рф", "пример.рф 2029-11-11 TIMEWEB", Entry{"xn--p1ai", "xn--e1afmkfd", true}, ""},
		{"IDN URL с верхним регистром", "рф", "https://ПРИМЕР.РФ/page", Entry{"xn--p1ai", "xn--e1afmkfd", true}, ""},
		// Quotes, commas, semicolons.
		{"кавычки и запятая", "com", `"xxx.com",`, Entry{"com", "xxx", false}, ""},
		{"CSV в кавычках", "com", `"xxx.com","2029-11-11","TIMEWEB"`, Entry{"com", "xxx", false}, ""},
		{"точки с запятой", "com", "xxx.com; 2029-11-11; TIMEWEB", Entry{"com", "xxx", false}, ""},
		{"скобки и тире", "com", "(xxx.com) — пометка", Entry{"com", "xxx", false}, ""},
		// The candidate order: the first is valid but of a foreign zone —
		// the next one is taken; the first invalid — the same.
		{"первый кандидат из чужой зоны", "com", "yyy.org xxx.com", Entry{"com", "xxx", false}, ""},
		{"первый кандидат битый punycode", "com", "xn--bcher--kva.com ok.com", Entry{"com", "ok", false}, ""},
		{"составная зона пути", "co.uk", "yyy.org example.co.uk", Entry{"co.uk", "example", false}, ""},
		// None passed: no_domain.
		{"слова без точек", "com", "нет домена тут", Entry{}, ReasonNoDomain},
		{"даты и слова без точек", "com", "2029-11-11 TIMEWEB", Entry{}, ReasonNoDomain},
		{"единственный кандидат из чужой зоны", "com", "yyy.org регистратор", Entry{}, ReasonNoDomain},
		{"URL без точечного хоста", "com", "http://localhost/x", Entry{}, ReasonNoDomain},
		{"URL с битым портом — ошибки разбора нет хоста", "com", "http://example.com:bad/x", Entry{}, ReasonNoDomain},
		// Edge "-"/"_" are not cut: the candidate stays broken and fails
		// normalization instead of being substituted with another domain.
		{"подчёркивание в начале — не чужое имя", "com", "_dmarc.example.com пометка", Entry{}, ReasonNoDomain},
		// Pinning the previous single-line behavior: the reasons are
		// unchanged (zero regression, T-132).
		{"одиночный домен", "com", "example.com", Entry{"com", "example", false}, ""},
		{"одиночная чужая зона", "com", "example.org", Entry{}, ReasonWrongZone},
		{"одиночный мусор", "com", "под_чёркивание.com", Entry{}, ReasonInvalid},
		{"одиночный токен в кавычках — прежний invalid", "com", `"example.com"`, Entry{}, ReasonInvalid},
		{"одиночные пробелы — прежний empty", "com", "   ", Entry{}, ReasonEmpty},
		{"одиночная голая зона — прежний no_label", "com", "com", Entry{}, ReasonNoLabel},
		// A behavior change (T-132): an inner space is a field separator;
		// the gluing "при мер.рф" → "пример.рф" is gone.
		{"внутренний пробел — извлечён токен с точкой", "рф", "при мер.рф", Entry{"xn--p1ai", "xn--e1ann", true}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v, _ := newValidator(t, tc.zone, 0)
			entry, reason, ok := v.Line(5, tc.line)
			if tc.reason == "" {
				// An acceptance and a specific record are expected.
				if !ok {
					t.Fatalf("Line(%q) отклонена: %s", tc.line, reason)
				}
				if reason != "" {
					t.Errorf("причина валидной строки: %q", reason)
				}
				if entry != tc.entry {
					t.Errorf("запись = %+v, want %+v", entry, tc.entry)
				}
				return
			}
			if ok {
				t.Fatalf("Line(%q) принята: %+v", tc.line, entry)
			}
			if reason != tc.reason {
				t.Errorf("причина = %s, want %s", reason, tc.reason)
			}
			if entry != (Entry{}) {
				t.Errorf("отклонённая строка вернула запись: %+v", entry)
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
		t.Errorf("счётчики: total=%d accepted=%d rejected=%d, want 3/1/2",
			res.Total, res.Accepted, res.Rejected)
	}
	wantReasons := map[Reason]int{ReasonNoDomain: 2}
	if len(res.Reasons) != len(wantReasons) {
		t.Fatalf("причины %+v, want %+v", res.Reasons, wantReasons)
	}
	for r, n := range wantReasons {
		if res.Reasons[r] != n {
			t.Errorf("причина %s: %d, want %d", r, res.Reasons[r], n)
		}
	}
	// no_domain — a full-fledged rejection: it counts toward the error
	// share threshold like the other reasons (2 of 3 > 0.1 — the file is
	// bad; no new counters, added/rejected are enough).
	if !res.Failed {
		t.Errorf("доля no_domain 2 из 3 выше порога 0.1 — файл должен быть ошибочен: %+v", res)
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
			t.Errorf("rejected-лог = %q, want запись с %q", logged, want)
		}
	}
	if n := strings.Count(logged, "reason=no_domain"); n != 2 {
		t.Errorf("записей no_domain = %d, want 2 (лог:\n%s)", n, logged)
	}
}

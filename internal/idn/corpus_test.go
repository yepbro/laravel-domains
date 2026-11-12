// The exhaustive corpus of the name normalization tests — task T-010
// (spec 11.1, 5.2): mixed case, ß/ς, emoji, zero-width characters, valid
// and broken xn--, lengths, TLD zones and compound labels, unicode zone
// directories, hosts, edges.
//
// The expectations are pinned to the actual UTS-46 profile of
// golang.org/x/net v0.61.0 (the Unicode 15 tables, nontransitional:
// const transitionalLookup = false in idna.go); the version is pinned in
// go.sum — the behavior is stable. The pipeline core (case,
// spaces/dot, basic xn--, lengths) — in idn_test.go (T-009); here — the
// corpus groups of spec 11.1.
package idn

import (
	"strings"
	"testing"
)

// assertNormalize — success: Normalize(in) without an error, the result
// equals want (Result is comparable: all the fields are scalars).
func assertNormalize(t *testing.T, in string, want Result) {
	t.Helper()
	got, err := Normalize(in)
	if err != nil {
		t.Fatalf("Normalize(%q) = %v, want без ошибки", in, err)
	}
	if got != want {
		t.Errorf("Normalize(%q) = %+v, want %+v", in, got, want)
	}
}

// assertNormalizeError — a rejection: an *Error of the given kind.
func assertNormalizeError(t *testing.T, in string, want ErrorKind) {
	t.Helper()
	_, err := Normalize(in)
	if err == nil {
		t.Fatalf("Normalize(%q): нет ошибки, want %s", in, want)
	}
	kind, ok := KindOf(err)
	if !ok {
		t.Fatalf("Normalize(%q): ошибка %v не *idn.Error", in, err)
	}
	if kind != want {
		t.Errorf("Normalize(%q): kind = %s, want %s (err: %v)", in, kind, want, err)
	}
}

// TestNormalizeCaseMapping — mixed case and the UTS-46 mapping (spec 5.2
// items 1, 11.1): upper case → lower case, fullwidth ASCII and the
// alternative dot separators are mapped, A-labels are canonicalized to
// lower case.
func TestNormalizeCaseMapping(t *testing.T) {
	tests := []struct {
		name string
		want Result
	}{
		// Cyrillic: upper/mixed case in both the label and the zone.
		{"България.РФ", Result{"xn--80abgvm6a7d2b.xn--p1ai", "българия.рф", "xn--p1ai", "xn--80abgvm6a7d2b", true}},
		{"БЪЛГАРИЯ.рф", Result{"xn--80abgvm6a7d2b.xn--p1ai", "българия.рф", "xn--p1ai", "xn--80abgvm6a7d2b", true}},
		{"мАгаЗин.РФ", Result{"xn--80aairftm.xn--p1ai", "магазин.рф", "xn--p1ai", "xn--80aairftm", true}},
		// Greek with a tonos: Πόλης → πόλης (the case changes, the
		// diacritics and the final sigma are preserved).
		{"Πόλης.Γρ", Result{"xn--sxahok1c.xn--oxa2a", "πόλης.γρ", "xn--oxa2a", "xn--sxahok1c", true}},
		// CJK.
		{"日本.JP", Result{"xn--wgv71a.jp", "日本.jp", "jp", "xn--wgv71a", true}},
		// The UTS-46 mapping of fullwidth ASCII (U+FF25… → e…) — like an
		// input typo.
		{"ＥＸＡＭＰＬＥ.com", Result{"example.com", "example.com", "com", "example", false}},
		// Alternative dot separators: U+FF0E (fullwidth) and U+FF61
		// (halfwidth ideographic) are mapped to ".".
		{"пример．рф", Result{"xn--e1afmkfd.xn--p1ai", "пример.рф", "xn--p1ai", "xn--e1afmkfd", true}},
		{"пример｡рф", Result{"xn--e1afmkfd.xn--p1ai", "пример.рф", "xn--p1ai", "xn--e1afmkfd", true}},
		// All whitespace characters are removed (spec 5.2 item 1),
		// including a tab inside the name (CRLF tails of the incoming
		// rows — T-015).
		{"при\tмер.рф", Result{"xn--e1afmkfd.xn--p1ai", "пример.рф", "xn--p1ai", "xn--e1afmkfd", true}},
		// Uppercase punycode labels are canonicalized to lower case
		// (fully and in mixed form) — the zone is the same.
		{"XN--BCHER-KVA.DE", Result{"xn--bcher-kva.de", "bücher.de", "de", "xn--bcher-kva", true}},
		{"xn--BCHER-kva.de", Result{"xn--bcher-kva.de", "bücher.de", "de", "xn--bcher-kva", true}},
		{"Xn--E1Afmkfd.Xn--P1ai", Result{"xn--e1afmkfd.xn--p1ai", "пример.рф", "xn--p1ai", "xn--e1afmkfd", true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertNormalize(t, tc.name, tc.want)
		})
	}
}

// TestNormalizeDeviations — the UTS-46 deviation characters: ß (U+00DF)
// and the final sigma ς (U+03C2). Transitional processing (the IDNA2003
// legacy) maps ß→ss and ς→σ; the Lookup profile in x/net is
// nontransitional (transitionalLookup = false), the deviation is preserved
// and encoded into punycode. The actual profile behavior of spec 5.2
// item 2 is encoded: ß and ss, ς and σ are DIFFERENT names, which matters
// for deduplication (strasse.de and xn--strae-oqa.de are two different
// domains).
func TestNormalizeDeviations(t *testing.T) {
	tests := []struct {
		name string
		want Result
	}{
		{"Straße.de", Result{"xn--strae-oqa.de", "straße.de", "de", "xn--strae-oqa", true}},
		{"straße.de", Result{"xn--strae-oqa.de", "straße.de", "de", "xn--strae-oqa", true}},
		// Does not collapse with ß: a separate name without punycode and
		// the is_idn bit.
		{"STRASSE.de", Result{"strasse.de", "strasse.de", "de", "strasse", false}},
		// The final sigma ς is preserved and distinguishable from σ.
		{"πόλης.γρ", Result{"xn--sxahok1c.xn--oxa2a", "πόλης.γρ", "xn--oxa2a", "xn--sxahok1c", true}},
		{"πόλησ.γρ", Result{"xn--sxahoo6b.xn--oxa2a", "πόλησ.γρ", "xn--oxa2a", "xn--sxahoo6b", true}},
		// Upper case: Σ→σ, Ρ→ρ — the usual case mapping; the ς at the end
		// is not "restored" at that (ΞΑΝΘΗ → ξανθη).
		{"ΞΑΝΘΗ.ΓΡ", Result{"xn--mxamctd.xn--oxa2a", "ξανθη.γρ", "xn--oxa2a", "xn--mxamctd", true}},
		{"ΠΑΡΑΔΕΙΓΜΑ.ΓΡ", Result{"xn--mxaaaheg2az3al.xn--oxa2a", "παραδειγμα.γρ", "xn--oxa2a", "xn--mxaaaheg2az3al", true}},
		{"παράδειγμα.γρ", Result{"xn--hxajbheg2az3al.xn--oxa2a", "παράδειγμα.γρ", "xn--oxa2a", "xn--hxajbheg2az3al", true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertNormalize(t, tc.name, tc.want)
		})
	}
}

// TestNormalizeEmoji — emoji domains. In x/net v0.61 the Lookup profile
// encodes single emoji into punycode (xn--e28h.com = 😀.com — such names
// are really registered and resolve); spec 5.2 sets no separate "reject
// emoji" policy — the profile behavior of item 2 is pinned. ZWJ
// sequences (👨‍👩‍👦) are rejected: the CONTEXTJ rule allows U+200D only in
// an Arabic/Indic context; inside an emoji sequence it is an error.
func TestNormalizeEmoji(t *testing.T) {
	for _, tc := range []struct {
		name string
		want Result
	}{
		{"😀.com", Result{"xn--e28h.com", "😀.com", "com", "xn--e28h", true}},
		{"cafe☕.com", Result{"xn--cafe-tj3b.com", "cafe☕.com", "com", "xn--cafe-tj3b", true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertNormalize(t, tc.name, tc.want)
		})
	}
	// A ready A-label with an emoji is valid and decodes for display.
	t.Run("xn--e28h.com", func(t *testing.T) {
		assertNormalize(t, "xn--e28h.com", Result{"xn--e28h.com", "😀.com", "com", "xn--e28h", true})
	})
	// The "family" ZWJ sequence: U+200D outside the allowed context.
	t.Run("ZWJ-последовательность", func(t *testing.T) {
		assertNormalizeError(t, "\U0001F468\u200D\U0001F469\u200D\U0001F466.com", ErrInvalid)
	})
}

// TestNormalizeZeroWidth — zero-width characters. Per the IdnaMappingTable
// ignored (dropped by the mapping in any position): ZWSP U+200B, soft
// hyphen U+00AD, BOM/ZWNBSP U+FEFF, word joiner U+2060. CONTEXTJ: ZWNJ
// U+200C and ZWJ U+200D are allowed only in context (Arabic — between
// letters), outside it they are rejected; the Mongolian vowel separator
// U+180E is disallowed.
func TestNormalizeZeroWidth(t *testing.T) {
	for _, tc := range []struct {
		name string
		want Result
	}{
		// ZWSP in the middle of a label, at the label boundary — dropped.
		{"ex\u200Bample.com", Result{"example.com", "example.com", "com", "example", false}},
		{"example\u200B.com", Result{"example.com", "example.com", "com", "example", false}},
		// A soft hyphen and a BOM (a BOM at the line start is a frequent
		// UTF-8 artifact).
		{"a\u00ADb.c", Result{"ab.c", "ab.c", "c", "ab", false}},
		{"a\uFEFFb.c", Result{"ab.c", "ab.c", "c", "ab", false}},
		{"a\u2060b.c", Result{"ab.c", "ab.c", "c", "ab", false}},
		// A BOM before a ready A-label (the start of a file line) —
		// dropped, the punycode is untouched.
		{"\uFEFFxn--e1afmkfd.xn--p1ai", Result{"xn--e1afmkfd.xn--p1ai", "пример.рф", "xn--p1ai", "xn--e1afmkfd", true}},
		// ZWNJ between Arabic letters — the contextual rule passes
		// (plus the Bidi rule of the RTL label).
		{"مثال\u200Cات.com", Result{"xn--mgbajb0kd0137b.com", "مثال\u200Cات.com", "com", "xn--mgbajb0kd0137b", true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertNormalize(t, tc.name, tc.want)
		})
	}
	for _, in := range []string{
		"ex\u200Cample.com", // ZWNJ outside a context
		"ex\u200Dample.com", // ZWJ outside a context
		"a\u180Eb.c",        // U+180E — disallowed
	} {
		t.Run(in, func(t *testing.T) {
			assertNormalizeError(t, in, ErrInvalid)
		})
	}
}

// TestNormalizePunycode — valid xn-- (spec 5.2 item 3): decoded,
// canonicalized, and remaining A-labels.
func TestNormalizePunycode(t *testing.T) {
	tests := []struct {
		name string
		want Result
	}{
		// The Cyrillic homoglyph of apple.com (аррӏе — all Cyrillic
		// letters, ӏ is U+04CF): the punycode is valid, the name looks
		// like a foreign one — but that is a policy question, not
		// syntax.
		{"xn--80ak6aa92e.com", Result{"xn--80ak6aa92e.com", "аррӏе.com", "com", "xn--80ak6aa92e", true}},
		{"XN--80AK6AA92E.COM", Result{"xn--80ak6aa92e.com", "аррӏе.com", "com", "xn--80ak6aa92e", true}},
		{"xn--e1afmkfd.xn--p1ai", Result{"xn--e1afmkfd.xn--p1ai", "пример.рф", "xn--p1ai", "xn--e1afmkfd", true}},
		// The deviation ß in ready punycode: the unicode form is straße.de.
		{"xn--strae-oqa.de", Result{"xn--strae-oqa.de", "straße.de", "de", "xn--strae-oqa", true}},
		// A Kazakh label.
		{"xn--80ao21a.ai", Result{"xn--80ao21a.ai", "қаз.ai", "ai", "xn--80ao21a", true}},
		// The trailing dot after punycode is removed before the pipeline.
		{"xn--e1afmkfd.xn--p1ai.", Result{"xn--e1afmkfd.xn--p1ai", "пример.рф", "xn--p1ai", "xn--e1afmkfd", true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertNormalize(t, tc.name, tc.want)
		})
	}
}

// TestNormalizePunycodeBroken — invalid xn-- (spec 5.2 item 3): the label
// does not decode or decodes into something invalid. The cause triage — by
// the decode of golang.org/x/net/idna (validateFromPunycode). The
// degenerate xn-- (a decode to empty → an "empty label") is pinned in
// idn_test.go (TestNormalizeInvalid).
func TestNormalizePunycodeBroken(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind ErrorKind
	}{
		{"xn--a.com", ErrInvalid},          // a decode into the control character U+0080
		{"xn--0.com", ErrInvalid},          // a decode into pure ASCII "0" — a non-canonical A-label
		{"xn--ab.com", ErrInvalid},         // a decode into the control U+0081 U+0080
		{"xn---p1ai.com", ErrInvalid},      // a decode of "-p1ai" with a leading hyphen
		{"xn--zzzzzz.com", ErrInvalid},     // a decode into the ASCII "zzzzzz": the label should have been without xn--
		{"xn--bcher--kva.com", ErrInvalid}, // a decode of "bchêer-" with a trailing hyphen
		{"XN--BCHER--KVA.com", ErrInvalid}, // the A-label case does not save broken punycode
		{"xn--p1ai-", ErrInvalid},          // a trailing hyphen after decoding
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertNormalizeError(t, tc.name, tc.kind)
		})
	}
}

// TestNormalizeLengthPunycode — the length boundary for a unicode label:
// punycode lengthens the label, so 63 octets run out earlier than 63
// runes (р×57 → a label of exactly 63 octets, р×58 → 64). Punycode is
// deterministic (RFC 3492); р×57 stably gives xn--p1aaa…a of length 63.
// The boundaries in the ASCII form (a label 63/64, a name 253/254) —
// TestNormalizeValid, TestNormalizeInvalid and TestNormalizeBoundaryLengths
// in idn_test.go.
func TestNormalizeLengthPunycode(t *testing.T) {
	ok := strings.Repeat("р", 57) + ".com"
	got, err := Normalize(ok)
	if err != nil {
		t.Fatalf("Normalize(р×57): %v, want без ошибки (метка ровно 63 октета)", err)
	}
	if !strings.HasPrefix(got.Label, xnPrefix) {
		t.Errorf("метка р×57 = %q, want punycode-метку (префикс %s)", got.Label, xnPrefix)
	}
	if len(got.Label) != maxLabelLen {
		t.Errorf("метка р×57: %d октет, want ровно %d", len(got.Label), maxLabelLen)
	}
	assertNormalizeError(t, strings.Repeat("р", 58)+".com", ErrLabelLen)
}

// TestNormalizeZonesTLD — zone separation by the last label of the name
// (item 5 as amended by the tld-only-zones spec, §4.2 — the whole table):
// 2LD registrations of the registries and private suffixes give a TLD
// zone and a compound label; bare suffixes are valid domains (§12.6: junk
// without a filter); a bare TLD — no_label; numeric last labels —
// numeric_zone.
func TestNormalizeZonesTLD(t *testing.T) {
	for _, tc := range []struct {
		name string
		want Result
	}{
		// The simple case: the zone is a TLD, one label.
		{"example.com", Result{"example.com", "example.com", "com", "example", false}},
		// 2LD registrations of the registries (the PSL public suffixes):
		// collapsed into the TLD, everything before it is a compound
		// label.
		{"example.5g.in", Result{"example.5g.in", "example.5g.in", "in", "example.5g", false}},
		{"example.co.uk", Result{"example.co.uk", "example.co.uk", "uk", "example.co", false}},
		{"a.b.example.com.au", Result{"a.b.example.com.au", "a.b.example.com.au", "au", "a.b.example.com", false}},
		{"example.city.kawasaki.jp", Result{"example.city.kawasaki.jp", "example.city.kawasaki.jp", "jp", "example.city.kawasaki", false}},
		// Private suffixes are not extracted: github.io/appspot.com are
		// labels under the TLD.
		{"example.github.io", Result{"example.github.io", "example.github.io", "io", "example.github", false}},
		{"foo.appspot.com", Result{"foo.appspot.com", "foo.appspot.com", "com", "foo.appspot", false}},
		// IDN: the zone is a punycode TLD, one label.
		{"магазин.рф", Result{"xn--80aairftm.xn--p1ai", "магазин.рф", "xn--p1ai", "xn--80aairftm", true}},
		// A testbed test zone: the zone is the last label, a compound
		// label is the norm (now always, without distinguishing known
		// TLDs).
		{"example.crawl.test", Result{"example.crawl.test", "example.crawl.test", "test", "example.crawl", false}},
		// Bare suffixes are valid domains (§12.6: distinguishing a suffix
		// from a domain is only possible by a suffix list — there is no
		// filter).
		{"github.io", Result{"github.io", "github.io", "io", "github", false}},
		{"co.in", Result{"co.in", "co.in", "in", "co", false}},
		{"com.au", Result{"com.au", "com.au", "au", "com", false}},
		{"appspot.com", Result{"appspot.com", "appspot.com", "com", "appspot", false}},
		// A compound label with xn-- inside: IsIDN is by any A-label of
		// the label (HasXNLabel), not only by the first one.
		{"магазин.рф.co.uk", Result{"xn--80aairftm.xn--p1ai.co.uk", "магазин.рф.co.uk", "uk", "xn--80aairftm.xn--p1ai.co", true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertNormalize(t, tc.name, tc.want)
		})
	}
	for _, tc := range []struct {
		name string
		kind ErrorKind
	}{
		// A bare TLD — no label before the zone.
		{"com", ErrNoLabel},
		{"москва", ErrNoLabel},       // the TLD москва
		{"xn--80adxhks", ErrNoLabel}, // the TLD москва in punycode
		// An IP literal: the last label of digits is a numeric zone; such
		// TLDs do not exist. For incoming rows this is right: an IP is not
		// a zone domain. The hosts from Location — separately, HostToASCII
		// lets IPs through (spec 5.2 item 8).
		{"192.0.2.1", ErrNumericZone}, // an IPv4 literal
		{"8.8.8.8", ErrNumericZone},   // an IPv4 literal
		{"1.2.3.4", ErrNumericZone},   // an IPv4 literal
		// Numeric "zones" are not domains: a TLD of digits does not exist
		// (IANA); labels outside the IPv4 range are not even an address
		// but also not a name: a numeric last label is rejected.
		{"999.888.777.666", ErrNumericZone},
		{"18.06.1999", ErrNumericZone}, // a registrar export date
		{"1999", ErrNumericZone},       // a bare numeric TLD
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertNormalizeError(t, tc.name, tc.kind)
		})
	}
}

// TestNormalizeEdges — the edges of the input: empty, dots, spaces with
// dots.
func TestNormalizeEdges(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind ErrorKind
	}{
		{"...", ErrEmpty},            // only trailing dots
		{" . ", ErrEmpty},            // spaces and a dot
		{".example.com", ErrInvalid}, // a leading dot — an empty label
		// A space inside is removed before the pipeline → example..com →
		// an empty label.
		{"example. .com", ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertNormalizeError(t, tc.name, tc.kind)
		})
	}
}

// TestNormalizeZoneUnicode — the unicode names of the zone directories
// (spec 5.2 item 6, spec 11.1): normalized by the same pipeline; the zone
// of a directory does not have to be a TLD — compound names are allowed.
func TestNormalizeZoneUnicode(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		{"РФ.", "xn--p1ai"},                // case and a trailing dot
		{"москва.", "xn--80adxhks"},        // the TLD москва
		{" XN--80ADXHKS ", "xn--80adxhks"}, // spaces and uppercase punycode
		{"日本.jp", "xn--wgv71a.jp"},         // a label + a zone of the directory
		{"日本。jp", "xn--wgv71a.jp"},         // the ideographic dot U+3002 — a label separator
		{"сайт.рф", "xn--80aswg.xn--p1ai"}, // a compound directory name: the zone does not have to be a TLD
	} {
		t.Run(tc.in, func(t *testing.T) {
			got, err := NormalizeZone(tc.in)
			if err != nil {
				t.Fatalf("NormalizeZone(%q) = %v, want %q", tc.in, err, tc.want)
			}
			if got != tc.want {
				t.Errorf("NormalizeZone(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
	for _, in := range []string{"xn--p1ai-", "рф!!"} {
		t.Run(in, func(t *testing.T) {
			if _, err := NormalizeZone(in); err == nil {
				t.Errorf("NormalizeZone(%q): нет ошибки, want ошибка", in)
			}
		})
	}
}

// TestHostToASCIICorpus — hosts from Location/meta refresh (spec 5.2
// item 8): unicode → punycode, IP literals, empty, IPv6 — not names.
func TestHostToASCIICorpus(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		// The main scenario of spec 5.2 item 8: a unicode host from
		// Location/meta refresh → punycode for the DNS query, Host and
		// SNI.
		{"пример.рф", "xn--e1afmkfd.xn--p1ai"},             // a unicode label and zone
		{"Straße.de", "xn--strae-oqa.de"},                  // the deviation ß — like in Normalize
		{"255.255.255.255", "255.255.255.255"},             // an IPv4 literal
		{"0.0.0.0", "0.0.0.0"},                             // an IPv4 literal
		{"1.2.3.4.5", "1.2.3.4.5"},                         // five labels — not an IP anymore but a valid name; HostToASCII does not separate the zone
		{"xn--e1afmkfd.xn--p1ai", "xn--e1afmkfd.xn--p1ai"}, // ready punycode
	} {
		t.Run(tc.in, func(t *testing.T) {
			got, err := HostToASCII(tc.in)
			if err != nil {
				t.Fatalf("HostToASCII(%q) = %v, want %q", tc.in, err, tc.want)
			}
			if got != tc.want {
				t.Errorf("HostToASCII(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
	for _, tc := range []struct {
		in   string
		kind ErrorKind
	}{
		{"", ErrEmpty},   // an empty host
		{"  ", ErrEmpty}, // spaces only
	} {
		t.Run(tc.in, func(t *testing.T) {
			_, err := HostToASCII(tc.in)
			if err == nil {
				t.Fatalf("HostToASCII(%q): нет ошибки, want %s", tc.in, tc.kind)
			}
			if kind, ok := KindOf(err); !ok || kind != tc.kind {
				t.Errorf("HostToASCII(%q): kind = %s (%v), want %s", tc.in, kind, err, tc.kind)
			}
		})
	}
}

// TestToUnicodeCorpus — the derived unicode form (spec 5.2 item 7): emoji,
// deviation characters and homoglyphs decode for display.
func TestToUnicodeCorpus(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		{"xn--e28h.com", "😀.com"},              // emoji
		{"xn--strae-oqa.de", "straße.de"},      // the deviation ß is preserved
		{"xn--sxahok1c.xn--oxa2a", "πόλης.γρ"}, // the final sigma ς (U+03C2) survives punycode
		{"xn--80ak6aa92e.com", "аррӏе.com"},    // the Cyrillic homoglyph of apple.com
	} {
		t.Run(tc.in, func(t *testing.T) {
			if got := ToUnicode(tc.in); got != tc.want {
				t.Errorf("ToUnicode(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

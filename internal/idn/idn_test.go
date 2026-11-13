// Unit tests of the name normalization (spec 5.2): the Normalize pipeline,
// zones, HostToASCII, ToUnicode, HasXNLabel, the typed errors — the core
// of the package (T-009). The exhaustive corpus per spec 11.1 (ß/ς, emoji,
// zero width, broken xn-- etc.) — corpus_test.go (T-010).
package idn

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestNormalizeValid(t *testing.T) {
	tests := []struct {
		name  string
		ascii string
		uni   string
		zone  string
		label string
		isIDN bool
	}{
		// Item 1: case, spaces, the trailing dot.
		{name: "  Example.COM ", ascii: "example.com", uni: "example.com", zone: "com", label: "example"},
		{name: "\uFEFFexample.com", ascii: "example.com", uni: "example.com", zone: "com", label: "example"}, // the BOM is removed by the UTS-46 mapping
		{name: "пример.рф.", ascii: "xn--e1afmkfd.xn--p1ai", uni: "пример.рф", zone: "xn--p1ai", label: "xn--e1afmkfd", isIDN: true},
		{name: "example.com...", ascii: "example.com", uni: "example.com", zone: "com", label: "example"}, // several trailing dots
		{name: "при мер.рф", ascii: "xn--e1afmkfd.xn--p1ai", uni: "пример.рф", zone: "xn--p1ai", label: "xn--e1afmkfd", isIDN: true},
		// Item 2: unicode → punycode.
		{name: "Bücher.de", ascii: "xn--bcher-kva.de", uni: "bücher.de", zone: "de", label: "xn--bcher-kva", isIDN: true},
		// The input is already in punycode — valid and unchanged.
		{name: "XN--BCHER-KVA.com", ascii: "xn--bcher-kva.com", uni: "bücher.com", zone: "com", label: "xn--bcher-kva", isIDN: true},
		// Item 5 (as amended by the tld-only-zones spec): the zone is the
		// last label (the TLD); a 2LD registration gives a compound label.
		{name: "example.co.uk", ascii: "example.co.uk", uni: "example.co.uk", zone: "uk", label: "example.co"},
		{name: "магазин.рф.co.uk", ascii: "xn--80aairftm.xn--p1ai.co.uk", uni: "магазин.рф.co.uk", zone: "uk", label: "xn--80aairftm.xn--p1ai.co", isIDN: true},
		// Item 9: the is_idn bit — by the domain label; the zone does not
		// count.
		{name: "example.xn--p1ai", ascii: "example.xn--p1ai", uni: "example.рф", zone: "xn--p1ai", label: "example", isIDN: false},
		// A testbed test zone: the zone is the last label (TLD-only — now
		// always); a compound label is the norm.
		{name: "example.crawl.test", ascii: "example.crawl.test", uni: "example.crawl.test", zone: "test", label: "example.crawl"},
		// Private suffixes are not extracted: github.io is a label under
		// the TLD io.
		{name: "foo.github.io", ascii: "foo.github.io", uni: "foo.github.io", zone: "io", label: "foo.github"},
		// The boundary of item 4: a label of exactly 63 octets is valid.
		{name: strings.Repeat("a", 63) + ".com", ascii: strings.Repeat("a", 63) + ".com", uni: strings.Repeat("a", 63) + ".com", zone: "com", label: strings.Repeat("a", 63)},
		// A numeric label in an alphabetic zone — a regular name: numeric
		// TLDs do not exist, but the zone here is alphabetic (only a
		// numeric zone is a problem).
		{name: "123.com", ascii: "123.com", uni: "123.com", zone: "com", label: "123"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Normalize(tc.name)
			if err != nil {
				t.Fatalf("Normalize(%q) = %v, want no error", tc.name, err)
			}
			if got.ASCII != tc.ascii {
				t.Errorf("ASCII = %q, want %q", got.ASCII, tc.ascii)
			}
			if got.Unicode != tc.uni {
				t.Errorf("Unicode = %q, want %q", got.Unicode, tc.uni)
			}
			if got.Zone != tc.zone {
				t.Errorf("Zone = %q, want %q", got.Zone, tc.zone)
			}
			if got.Label != tc.label {
				t.Errorf("Label = %q, want %q", got.Label, tc.label)
			}
			if got.IsIDN != tc.isIDN {
				t.Errorf("IsIDN = %v, want %v", got.IsIDN, tc.isIDN)
			}
		})
	}
}

func TestNormalizeInvalid(t *testing.T) {
	tests := []struct {
		name string
		kind ErrorKind
	}{
		// Item 1: empty.
		{"", ErrEmpty},
		{"   ", ErrEmpty},
		{".", ErrEmpty},
		{"..", ErrEmpty},
		// Items 2–3: invalid per UTS-46/IDNA.
		{"xn--bcher--kva.com", ErrInvalid},      // broken punycode
		{"xn--.com", ErrInvalid},                // xn-- without a payload
		{"example..com", ErrInvalid},            // an empty label
		{"-lead.example.com", ErrInvalid},       // a hyphen at the label start
		{"under_score.example.com", ErrInvalid}, // STD3: an underscore
		{"плох_й.рф", ErrInvalid},               // STD3 in a unicode label
		{"\u200Cample.com", ErrInvalid},         // ZWNJ outside the contextual rules (U+200C)
		// Item 4: lengths (the ASCII form).
		{strings.Repeat("a", 64) + ".com", ErrLabelLen}, // a label of 64
		{"àéîõüßàéîõüßàéîõüßàéîõüßàéîõüßàéîõüßàéîõüßàéîõüßàéîõüß.com", ErrLabelLen},                                                           // a punycode label of 64
		{strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 62), ErrNameLen}, // a name of 254
		// Item 5: the name is a bare TLD, no label.
		{"com", ErrNoLabel},
		// Item 5: a numeric last label — such TLDs do not exist
		// (IANA): registrar export dates and IPv4 literals.
		{"18.06.1999", ErrNumericZone}, // a registrar export date
		{"1.2.3.4", ErrNumericZone},    // an IPv4 literal
		{"1999", ErrNumericZone},       // a bare numeric TLD
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Normalize(tc.name)
			if err == nil {
				t.Fatalf("Normalize(%q) = %+v, want the error %s", tc.name, got, tc.kind)
			}
			kind, ok := KindOf(err)
			if !ok {
				t.Fatalf("Normalize(%q): the error %v is not *idn.Error", tc.name, err)
			}
			if kind != tc.kind {
				t.Errorf("Normalize(%q): kind = %s, want %s (err: %v)", tc.name, kind, tc.kind, err)
			}
		})
	}
}

// TestNormalizeBoundaryLengths — the bounds of the item 4 limits: 63/253
// are valid.
func TestNormalizeBoundaryLengths(t *testing.T) {
	name := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
	if len(name) != 253 {
		t.Fatalf("the test name length %d, want 253", len(name))
	}
	got, err := Normalize(name)
	if err != nil {
		t.Fatalf("Normalize(name 253) = %v, want no error", err)
	}
	if got.ASCII != name {
		t.Errorf("ASCII = %q, want the name unchanged", got.ASCII)
	}
	// The last label is the zone (TLD-only).
	if want := strings.Repeat("d", 61); got.Zone != want {
		t.Errorf("Zone = %q, want %q", got.Zone, want)
	}
	if want := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63); got.Label != want {
		t.Errorf("Label = %q, want %q", got.Label, want)
	}
}

func TestNormalizeZone(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"com", "com"},
		{" Co.UK ", "co.uk"},
		{"рф.", "xn--p1ai"},
		{" crawl.test\t", "crawl.test"},
		{"XN--P1AI", "xn--p1ai"},
	}
	for _, tc := range tests {
		got, err := NormalizeZone(tc.in)
		if err != nil {
			t.Errorf("NormalizeZone(%q) = %v, want %q", tc.in, err, tc.want)
			continue
		}
		if got != tc.want {
			t.Errorf("NormalizeZone(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	for _, in := range []string{"", "  ", ".", "рф!!"} {
		if _, err := NormalizeZone(in); err == nil {
			t.Errorf("NormalizeZone(%q): no error, want an error", in)
		}
	}
}

func TestHostToASCII(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"Пример.РФ", "xn--e1afmkfd.xn--p1ai"}, // item 8: unicode → punycode
		{"Example.COM.", "example.com"},        // case and dot
		{"192.0.2.1", "192.0.2.1"},             // an IPv4 literal
	}
	for _, tc := range tests {
		got, err := HostToASCII(tc.in)
		if err != nil {
			t.Errorf("HostToASCII(%q) = %v, want %q", tc.in, err, tc.want)
			continue
		}
		if got != tc.want {
			t.Errorf("HostToASCII(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	for _, in := range []string{"xn--bcher--kva.com", "under_score.example.com", "xn--", "::1", "[::1]"} {
		// IPv6 literals are not names: the bracketed form from a URL and
		// the bare address (a colon is an STD3 violation); T-047/T-048
		// handles them separately via net.ParseIP.
		if _, err := HostToASCII(in); err == nil {
			t.Errorf("HostToASCII(%q): no error, want an error", in)
		}
	}
}

func TestToUnicode(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"xn--e1afmkfd.xn--p1ai", "пример.рф"}, // item 7: output on the fly
		{"xn--bcher-kva.com", "bücher.com"},
		{"example.com", "example.com"}, // no punycode — as is
		{"!!!garbage", "!!!garbage"},   // a decoding error — the input
		{"xn--", "xn--"},               // an empty decoding result — the input, not an empty string
		{"", ""},                       // an empty string — an empty answer
	}
	for _, tc := range tests {
		if got := ToUnicode(tc.in); got != tc.want {
			t.Errorf("ToUnicode(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestHasXNLabel(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"xn--e1afmkfd.xn--p1ai", true},
		{"example.com", false},
		{"XN--BCHER-KVA.com", true},     // case does not matter
		{"axn--foo.example.com", false}, // the prefix is not at the label start
		{"xn--", true},                  // a label exactly "xn--": the prefix matches entirely
		{"", false},
	}
	for _, tc := range tests {
		if got := HasXNLabel(tc.in); got != tc.want {
			t.Errorf("HasXNLabel(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestIsNumericZone — the isNumericZone contract by direct calls: the
// function is unexported and the zone == "" case is unreachable through
// Normalize (the zone is a non-empty last label of the normalizeASCII
// output, see the comment before the zone == "" guard check there), so
// the empty zone behavior is pinned here, not through the public API.
func TestIsNumericZone(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		// An empty zone is not a zone: the mutant idn.go.8 (branch/if)
		// removes the return false for zone == "", after which the loop
		// over Split("", ".") = [""] does not fire and the function
		// reaches return true. Through Normalize the branch is
		// unreachable — the contract is pinned by a direct call.
		{"", false},
		// Numeric zones — such TLDs do not exist (IANA).
		{"1999", true},       // a bare numeric TLD
		{"18.06.1999", true}, // a registrar export date
		{"1.2.3.4", true},    // an IPv4 literal
		{"0", true},
		// Alphabetic zones are not numeric; a numeric label in an
		// alphabetic zone (123.com) is a regular name, not a numeric zone.
		{"123.com", false},
		{"com", false},
		{"co.uk", false},
	}
	for _, tc := range tests {
		if got := isNumericZone(tc.in); got != tc.want {
			t.Errorf("isNumericZone(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestError — the structure of the typed error: the kind, the name, the
// cause, wrapping (the rejected log T-016 will wrap it in its own error).
func TestError(t *testing.T) {
	_, err := Normalize("xn--bcher--kva.com")
	if err == nil {
		t.Fatal("Normalize(a broken xn--): no error")
	}
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("the error %v is not *idn.Error", err)
	}
	if e.Kind != ErrInvalid {
		t.Errorf("Kind = %s, want %s", e.Kind, ErrInvalid)
	}
	if e.Name != "xn--bcher--kva.com" {
		t.Errorf("Name = %q, want the original name", e.Name)
	}
	if e.Err == nil {
		t.Error("Err = nil, want the cause (an idna error)")
	}
	if msg := e.Error(); !strings.Contains(msg, "invalid domain name") || !strings.Contains(msg, "xn--bcher--kva.com") {
		t.Errorf("Error() = %q, want the class and the name", msg)
	}

	// The Error() format — exact strings, with a cause and without (a
	// return-removal mutant would lose the cause, leaving the ": %v"
	// format).
	withCause := &Error{Kind: ErrNameLen, Name: "x", Err: errors.New("reason")}
	if got, want := withCause.Error(), `idn: name longer than 253 characters: "x": reason`; got != want {
		t.Errorf("Error() with a cause = %q, want %q", got, want)
	}
	withoutCause := &Error{Kind: ErrEmpty, Name: "y"}
	if got, want := withoutCause.Error(), `idn: empty name: "y"`; got != want {
		t.Errorf("Error() without a cause = %q, want %q", got, want)
	}

	// The kind is extracted from the wrapped error (fmt.Errorf %w).
	wrapped := fmt.Errorf("line 42: %w", err)
	kind, ok := KindOf(wrapped)
	if !ok || kind != ErrInvalid {
		t.Errorf("KindOf(wrapped) = %s, %v; want %s, true", kind, ok, ErrInvalid)
	}
	if _, ok := KindOf(errors.New("foreign")); ok {
		t.Error("KindOf(a foreign error) = true, want false")
	}

	// The cause is reachable via Unwrap (an idna error).
	if errors.Unwrap(e) == nil {
		t.Error("Unwrap(e) = nil, want the cause")
	}
}

// TestErrorNameTruncated — untrusted input does not bloat the error:
// the name is truncated to maxErrNameLen runes.
func TestErrorNameTruncated(t *testing.T) {
	long := strings.Repeat("р", 1000)
	_, err := Normalize(long)
	if err == nil {
		t.Fatal("Normalize(1000 р): no error")
	}
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("the error %v is not *idn.Error", err)
	}
	if runes := len([]rune(e.Name)); runes > maxErrNameLen+1 {
		t.Errorf("Name = %d runes, want ≤ %d+1", runes, maxErrNameLen)
	}
	if msg := e.Error(); len(msg) > 1024 {
		t.Errorf("Error() of length %d, want ≤ 1024", len(msg))
	}

	// The boundary: a name of exactly maxErrNameLen runes is not
	// truncated.
	boundary := strings.Repeat("р", maxErrNameLen)
	_, err = Normalize(boundary)
	if err == nil {
		t.Fatal("Normalize(a boundary name): no error")
	}
	if !errors.As(err, &e) {
		t.Fatalf("the error %v is not *idn.Error", err)
	}
	if e.Name != boundary {
		t.Errorf("Name truncated: %d runes, want %d without truncation", len([]rune(e.Name)), maxErrNameLen)
	}

	// The truncation bounds — by literals (the checks above are adaptive
	// through the constant and would not notice a ±1 mutant): exactly
	// 255 runes is not cut, 256 — 255 runes + "…".
	exactly := strings.Repeat("р", 255)
	_, err = Normalize(exactly)
	if err == nil {
		t.Fatal("Normalize(255 р): no error")
	}
	if !errors.As(err, &e) {
		t.Fatalf("the error %v is not *idn.Error", err)
	}
	if e.Name != exactly {
		t.Errorf("a name exactly at the limit is truncated: %d runes, want 255", len([]rune(e.Name)))
	}
	over := strings.Repeat("р", 256)
	_, err = Normalize(over)
	if err == nil {
		t.Fatal("Normalize(256 р): no error")
	}
	if !errors.As(err, &e) {
		t.Fatalf("the error %v is not *idn.Error", err)
	}
	if want := strings.Repeat("р", 255) + "…"; e.Name != want {
		t.Errorf("a name above the limit = %d runes, want 255 runes + \"…\"", len([]rune(e.Name)))
	}
}

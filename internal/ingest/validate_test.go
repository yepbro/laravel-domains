// Unit tests of ingest line validation (T-016): belonging to the path zone,
// typed rejection reasons, the rejected log (the format is fixed), the
// counters and the per-file error ratio threshold.
package ingest

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// newValidator — a validator of one file with the rejected log into a buffer
// (checking the entries) and a given error ratio threshold. A multi-level
// directory zone automatically goes into the allowlist (the tld-only-zones
// spec §5.1): these tests check the domainLabel semantics; the allowlist
// gate — separately in TestNewValidator.
func newValidator(t *testing.T, zone string, ratio float64) (*Validator, *bytes.Buffer) {
	t.Helper()
	var allowed []string
	if strings.Contains(zone, ".") {
		allowed = []string{zone}
	}
	var buf bytes.Buffer
	v, err := NewValidator(ValidatorOptions{
		File:             "incoming/example.com/add/list.txt",
		Zone:             zone,
		AllowedZonePaths: allowed,
		MaxErrorRatio:    ratio,
		RejectLog:        NewRejectedLogger(&buf, slog.LevelInfo),
	})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	return v, &buf
}

func TestLineValid(t *testing.T) {
	tests := []struct {
		name     string
		zone     string
		line     string
		wantZone string
		label    string
		isIDN    bool
	}{
		// A domain from the path zone; input blemishes are forgiven by
		// normalization (spec 5.2 item 1: case, spaces, the trailing dot).
		{"plain", "com", "example.com", "com", "example", false},
		{"case and a dot", "com", "  EXAMPLE.Com. ", "com", "example", false},
		// IDN: a line in unicode, the path zone in punycode — and vice versa.
		{"a unicode name in a punycode zone", "xn--p1ai", "пример.рф", "xn--p1ai", "xn--e1afmkfd", true},
		{"a punycode name in a punycode zone", "xn--p1ai", "xn--e1afmkfd.xn--p1ai", "xn--p1ai", "xn--e1afmkfd", true},
		{"a unicode directory zone", "РФ.", "пример.рф", "xn--p1ai", "xn--e1afmkfd", true},
		// the is_idn bit — by the domain label, not by the zone (spec 5.2 item 9).
		{"an ASCII label in an idn zone", "xn--p1ai", "example.xn--p1ai", "xn--p1ai", "example", false},
		// The path zone is a compound name deeper than the name TLD (branch 3
		// of domainLabel: the directory sets the zone explicitly, the T-014
		// test zones).
		{"a composite path zone", "co.uk", "example.co.uk", "co.uk", "example", false},
		{"a composite label", "co.uk", "магазин.рф.co.uk", "co.uk", "xn--80aairftm.xn--p1ai", true},
		// A 2LD registration in a TLD path zone: the name zone is the TLD
		// (idn, TLD-only), a compound label is the norm (the tld-only-zones
		// spec §5.3: formerly wrong_zone).
		{"a 2LD registration in a TLD zone", "uk", "example.co.uk", "uk", "example.co", false},
		{"a private suffix in a TLD zone", "io", "foo.github.io", "io", "foo.github", false},
		{"5g.in in the in zone", "in", "example.5g.in", "in", "example.5g", false},
		// The path zone deeper than the name TLD — the test zones (T-014):
		// for example.crawl.test the name zone is test, the path zone is
		// crawl.test.
		{"a path zone deeper than the TLD", "crawl.test", "example.crawl.test", "crawl.test", "example", false},
		{"a composite label outside the list", "crawl.test", "sub.example.crawl.test", "crawl.test", "sub.example", false},
		{"the last label as the zone", "test", "crawl.test", "test", "crawl", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v, _ := newValidator(t, tc.zone, 0)
			entry, reason, ok := v.Line(1, tc.line)
			if !ok {
				t.Fatalf("Line(%q) rejected: %s", tc.line, reason)
			}
			if reason != "" {
				t.Errorf("the reason of a valid line: %q", reason)
			}
			if entry.Zone != tc.wantZone {
				t.Errorf("Zone = %q, want %q", entry.Zone, tc.wantZone)
			}
			if entry.Label != tc.label {
				t.Errorf("Label = %q, want %q", entry.Label, tc.label)
			}
			if entry.IsIDN != tc.isIDN {
				t.Errorf("IsIDN = %v, want %v", entry.IsIDN, tc.isIDN)
			}
		})
	}
}

func TestLineRejected(t *testing.T) {
	longName := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." +
		strings.Repeat("c", 63) + "." + strings.Repeat("d", 62) // 254 octets
	tests := []struct {
		name   string
		zone   string
		line   string
		reason Reason
	}{
		// A domain from another zone (spec 5.2 item 6).
		{"another zone", "com", "example.org", ReasonWrongZone},
		{"a subdomain of another zone", "com", "sub.example.org", ReasonWrongZone},
		{"a valid idn not from the zone", "com", "пример.рф", ReasonWrongZone},
		// A bare suffix — a valid name of another zone (TLD-only: the zone
		// uk, the label co): in a foreign directory — wrong_zone (the
		// tld-only-zones spec §12.6).
		{"a bare suffix of another zone", "com", "co.uk", ReasonWrongZone},
		// The idn.Error classes (spec 5.2).
		{"an empty line", "com", "", ReasonEmpty},
		{"spaces only", "com", "   ", ReasonEmpty},
		{"a dot only", "com", ".", ReasonEmpty},
		{"junk", "com", "под_чёркивание.com", ReasonInvalid},
		{"broken punycode", "com", "xn--bcher--kva.com", ReasonInvalid},
		{"an empty label", "com", "example..com", ReasonInvalid},
		{"a label longer than 63", "com", strings.Repeat("a", 64) + ".com", ReasonLabelLen},
		{"a name longer than 253", "com", longName, ReasonNameLen},
		// There is no label before the zone: the name is the TLD itself.
		{"a bare TLD", "com", "com", ReasonNoLabel},
		// A path zone that is not a TLD: the name coincides with the zone.
		{"the name equals the path zone", "crawl.test", "crawl.test", ReasonNoLabel},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v, _ := newValidator(t, tc.zone, 0)
			entry, reason, ok := v.Line(7, tc.line)
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

// The rejected log format: a text slog, the service=rejected attribute, an
// entry with the file/line number/truncated line/reason attributes
// (spec 5.1).
func TestRejectedLogFormat(t *testing.T) {
	v, buf := newValidator(t, "com", 0)
	if _, _, ok := v.Line(3, "example.org"); ok {
		t.Fatal("example.org in the com zone must be rejected")
	}
	out := buf.String()
	for _, want := range []string{
		`msg="line rejected"`,
		`service=rejected`,
		`file=incoming/example.com/add/list.txt`,
		`line=3`,
		`text=example.org`,
		`reason=wrong_zone`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the rejected log record does not contain %s:\n%s", want, out)
		}
	}
}

// Long junk is truncated to 255 runes with an ellipsis (the input is untrusted).
func TestRejectedLogTruncates(t *testing.T) {
	v, buf := newValidator(t, "com", 0)
	v.Line(1, strings.Repeat("ё", 300)+".com") // an invalid label of any length
	want := "text=" + strings.Repeat("ё", maxLoggedRunes) + "…"
	if !strings.Contains(buf.String(), want) {
		t.Errorf("the line in the log is not truncated to %d runes", maxLoggedRunes)
	}
}

// The error ratio threshold: a ratio equal to the threshold does not fail
// the file; an excess — the file is failed as a whole (spec 5.1; the
// failed/ signal for T-021).
func TestErrorRatioBoundary(t *testing.T) {
	run := func(t *testing.T, ratio float64, bad, good int) FileResult {
		t.Helper()
		v, _ := newValidator(t, "com", ratio)
		for i := 0; i < good; i++ {
			v.Line(i+1, "example"+string(rune('a'+i%26))+".com")
		}
		for i := 0; i < bad; i++ {
			v.Line(good+i+1, "example.org") // not from the zone
		}
		return v.Finish()
	}
	t.Run("the share equals the threshold", func(t *testing.T) {
		if res := run(t, 0.5, 2, 2); res.Failed {
			t.Errorf("2 of 4 at the 0.5 threshold: the file must not be bad: %+v", res)
		}
	})
	t.Run("the share is above the threshold", func(t *testing.T) {
		if res := run(t, 0.5, 3, 1); !res.Failed {
			t.Errorf("3 of 4 at the 0.5 threshold: the file must be bad: %+v", res)
		}
	})
	t.Run("the default threshold of 10 percent", func(t *testing.T) {
		if res := run(t, 0, 1, 9); res.Failed {
			t.Errorf("1 of 10 at the 0.1 threshold: the file must not be bad: %+v", res)
		}
		if res := run(t, 0, 2, 8); !res.Failed {
			t.Errorf("2 of 10 at the 0.1 threshold: the file must be bad: %+v", res)
		}
	})
	t.Run("a file without lines", func(t *testing.T) {
		v, _ := newValidator(t, "com", 0.5)
		if res := v.Finish(); res.Failed || res.Total != 0 {
			t.Errorf("an empty file is not bad, got %+v", res)
		}
	})
}

// The file counters: total/accepted/rejected and the reason distribution
// (the ingest_files report, spec 5.1, T-021).
func TestFinishStats(t *testing.T) {
	v, _ := newValidator(t, "com", 0)
	lines := []string{"example.com", "example.org", "", "example..com", "shop.com"}
	for i, line := range lines {
		v.Line(i+1, line)
	}
	res := v.Finish()
	if res.Total != 5 || res.Accepted != 2 || res.Rejected != 3 {
		t.Errorf("the counters: total=%d accepted=%d rejected=%d, want 5/2/3",
			res.Total, res.Accepted, res.Rejected)
	}
	wantReasons := map[Reason]int{ReasonWrongZone: 1, ReasonEmpty: 1, ReasonInvalid: 1}
	if len(res.Reasons) != len(wantReasons) {
		t.Fatalf("the reasons %+v, want %+v", res.Reasons, wantReasons)
	}
	for r, n := range wantReasons {
		if res.Reasons[r] != n {
			t.Errorf("the reason %s: %d, want %d", r, res.Reasons[r], n)
		}
	}
}

func TestNewValidator(t *testing.T) {
	t.Run("the directory zone is normalized", func(t *testing.T) {
		v, err := NewValidator(ValidatorOptions{Zone: "РФ"})
		if err != nil {
			t.Fatal(err)
		}
		if v.zone != "xn--p1ai" {
			t.Errorf("the zone = %q, want xn--p1ai", v.zone)
		}
	})
	t.Run("a broken directory zone", func(t *testing.T) {
		if _, err := NewValidator(ValidatorOptions{Zone: "под_чёркивание.рф"}); err == nil {
			t.Error("an invalid zone must give an error")
		}
	})
	// The gate of multi-level directories (the tld-only-zones spec §5.1):
	// working directories are TLDs, a zone with a dot — from the allowlist
	// only; the file-level error is identified by errors.Is (the pipeline →
	// failed/, T-021).
	t.Run("a multi-level zone without an allowlist", func(t *testing.T) {
		for _, zone := range []string{"co.uk", "crawl.test", "a.b.c"} {
			_, err := NewValidator(ValidatorOptions{Zone: zone})
			if !errors.Is(err, errZoneNotAllowed) {
				t.Errorf("the zone %s without an allowlist: errZoneNotAllowed is expected, got %v", zone, err)
			}
		}
	})
	t.Run("a multi-level zone from the allowlist", func(t *testing.T) {
		v, err := NewValidator(ValidatorOptions{
			Zone:             "Crawl.Test",
			AllowedZonePaths: []string{"crawl.test", "co.uk"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if v.zone != "crawl.test" {
			t.Errorf("the zone = %q, want crawl.test", v.zone)
		}
	})
	t.Run("an uppercase allowlist entry", func(t *testing.T) {
		// The entry is normalized inside checkZonePath: direct calls with an
		// unnormalized list work too (the config canonicalizes the entries at
		// load — this is that guard here).
		v, err := NewValidator(ValidatorOptions{
			Zone:             "crawl.test",
			AllowedZonePaths: []string{"Crawl.Test"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if v.zone != "crawl.test" {
			t.Errorf("the zone = %q, want crawl.test", v.zone)
		}
	})
	t.Run("a foreign multi-level zone with a non-empty allowlist", func(t *testing.T) {
		_, err := NewValidator(ValidatorOptions{
			Zone:             "other.test",
			AllowedZonePaths: []string{"crawl.test"},
		})
		if !errors.Is(err, errZoneNotAllowed) {
			t.Errorf("errZoneNotAllowed is expected, got %v", err)
		}
	})
	t.Run("a threshold outside the range", func(t *testing.T) {
		for _, ratio := range []float64{-0.1, 1.0001} {
			if _, err := NewValidator(ValidatorOptions{Zone: "com", MaxErrorRatio: ratio}); err == nil {
				t.Errorf("the threshold %v must be rejected", ratio)
			}
		}
	})
	t.Run("without a logger", func(t *testing.T) {
		v, err := NewValidator(ValidatorOptions{Zone: "com"})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, ok := v.Line(1, "example.org"); ok {
			t.Fatal("a line not from the zone must be rejected without a logger too")
		}
	})
}

// TestValidatorZoneless — a zone-less file (Zone == ""): line zones are
// derived from the names themselves (the TLD is the last label), the file
// mixes zones, wrong_zone is impossible. Test zones: example.crawl.test
// belongs to the zone test with a compound label — for the testbed use
// zone directories.
func TestValidatorZoneless(t *testing.T) {
	v, err := NewValidator(ValidatorOptions{File: "add/list.txt", MaxErrorRatio: 0.5})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	cases := []struct {
		line  string
		zone  string
		label string
	}{
		{"example.com", "com", "example"},
		{"shop.co.uk", "uk", "shop.co"},
		{"FOO.RU.", "ru", "foo"},
		{"example.crawl.test", "test", "example.crawl"},
	}
	for i, tc := range cases {
		e, reason, ok := v.Line(i+1, tc.line)
		if !ok {
			t.Fatalf("Line(%d, %q) rejected: %s", i+1, tc.line, reason)
		}
		if e.Zone != tc.zone || e.Label != tc.label {
			t.Errorf("Line(%q) = zone %q label %q, want %q/%q", tc.line, e.Zone, e.Label, tc.zone, tc.label)
		}
	}
	// An IDN label: the flag by the punycode label.
	e, _, ok := v.Line(len(cases)+1, "тест.рф")
	if !ok || e.Zone != "xn--p1ai" || !e.IsIDN || !strings.HasPrefix(e.Label, "xn--") {
		t.Errorf("the IDN line = %+v ok=%v, want the zone xn--p1ai, IsIDN, a punycode label", e, ok)
	}

	// A bare TLD — no label before the zone: a no_label rejection.
	if _, reason, ok := v.Line(len(cases)+2, "com"); ok || reason != ReasonNoLabel {
		t.Errorf("the line \"com\" = ok=%v reason %s, want no_label", ok, reason)
	}
	// Foreign data without a domain — no_domain (T-132).
	if _, reason, ok := v.Line(len(cases)+3, "hello world"); ok || reason != ReasonNoDomain {
		t.Errorf("a line without a domain = ok=%v reason %s, want no_domain", ok, reason)
	}

	res := v.Finish()
	if res.Accepted != len(cases)+1 || res.Rejected != 2 || res.Failed {
		t.Errorf("Finish = %+v, want %d accepted, 2 rejected, not failed", res, len(cases)+1)
	}
}

// TestValidatorZonelessNumericZone — a numeric zone derived from the zone
// output is not a domain (the .SU dump regression: a candidate date
// "18.06.1999" passed validation, was output into the fictitious zone 1999
// and applied as a date-domain). A date and an IPv4 literal are rejected
// (numeric_zone), a numeric label in a letter zone is valid. The bare
// suffix of a dump line (sochi.su) with TLD-only zones is a valid domain:
// junk without the filter (the tld-only-zones spec §12.6); a line of dates
// only — no_domain.
func TestValidatorZonelessNumericZone(t *testing.T) {
	v, err := NewValidator(ValidatorOptions{File: "add/list.txt", MaxErrorRatio: 1})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	if _, reason, ok := v.Line(1, "18.06.1999"); ok || reason != ReasonNumericZone {
		t.Errorf("the dump-date line = ok=%v reason %s, want numeric_zone", ok, reason)
	}
	if _, reason, ok := v.Line(2, "1.2.3.4"); ok || reason != ReasonNumericZone {
		t.Errorf("the IPv4 literal = ok=%v reason %s, want numeric_zone", ok, reason)
	}
	if e, _, ok := v.Line(3, "123.com"); !ok || e.Zone != "com" || e.Label != "123" {
		t.Errorf("a numeric label in a letter zone = %+v ok=%v, want com/123", e, ok)
	}
	// A .SU dump line: the first candidate sochi.su is a bare suffix, valid
	// as a domain (the zone su, the label sochi) — junk without the filter
	// (the tld-only-zones spec §12.6); the field dates do not reach the
	// check.
	const line = "SOCHI.SU\tRUCENTER-SU\t18.06.1999\t01.10.2027\t01.11.2027\t1"
	if e, _, ok := v.Line(4, line); !ok || e.Zone != "su" || e.Label != "sochi" {
		t.Errorf("the dump line = %+v ok=%v, want su/sochi (a bare suffix is valid)", e, ok)
	}
	// A registrar export line without a domain: the candidate dates are
	// numeric_zone, none passes — no_domain.
	const datesOnly = "RUCENTER-SU\t18.06.1999\t01.10.2027\t01.11.2027\t1"
	if _, reason, ok := v.Line(5, datesOnly); ok || reason != ReasonNoDomain {
		t.Errorf("the line of dates = ok=%v reason %s, want no_domain", ok, reason)
	}
	res := v.Finish()
	if res.Accepted != 2 || res.Rejected != 3 || res.Failed {
		t.Errorf("Finish = %+v, want 2 accepted, 3 rejected, not failed", res)
	}
	if res.Reasons[ReasonNumericZone] != 2 || res.Reasons[ReasonNoDomain] != 1 || len(res.Reasons) != 2 {
		t.Errorf("Reasons = %+v, want numeric_zone: 2, no_domain: 1", res.Reasons)
	}
}

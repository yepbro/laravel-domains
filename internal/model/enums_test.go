// Unit tests of the model enumerations: the codes and names of the domain
// statuses (spec 6.2, a strict cross-check with the §6.2 table and the
// schema), check_state, variant, scheme, alias_kind, the ingest-file
// action; name parsing and code validation.
package model

import (
	"strconv"
	"strings"
	"testing"
)

// TestStatusCodes cross-checks the status codes and names strictly with
// the spec 6.2 table (the values are pinned by the domains.status column,
// migration 000001).
func TestStatusCodes(t *testing.T) {
	want := []struct {
		code int16
		name string
	}{
		{0, "new"},
		{1, "not_delegated"},
		{2, "delegated_no_address"},
		{3, "no_http"},
		{4, "alive"},
		{5, "temp_error"},
		{6, "deleted"},
		{7, "invalid"}, // diagnostic, T-109
	}
	if len(Statuses) != len(want) {
		t.Fatalf("Statuses: %d статусов, want %d (все статусы ТЗ 6.2)", len(Statuses), len(want))
	}
	for i, w := range want {
		s := Statuses[i]
		if int16(s) != w.code {
			t.Errorf("Statuses[%d] = код %d, want %d", i, int16(s), w.code)
		}
		if got := s.String(); got != w.name {
			t.Errorf("Status(%d).String() = %q, want %q", w.code, got, w.name)
		}
		if !s.Valid() {
			t.Errorf("Status(%d).Valid() = false, want true", w.code)
		}
		if got, err := ParseStatus(w.name); err != nil || got != s {
			t.Errorf("ParseStatus(%q) = %v, %v, want %v", w.name, got, err, s)
		}
	}
}

func TestStatusStringUnknown(t *testing.T) {
	for _, code := range []int16{-1, 8, 100} {
		if got, want := Status(code).String(), "Status("+itoa(code)+")"; got != want {
			t.Errorf("Status(%d).String() = %q, want %q", code, got, want)
		}
	}
}

func TestStatusValidInvalid(t *testing.T) {
	for _, code := range []int16{-1, 8, 42} {
		if Status(code).Valid() {
			t.Errorf("Status(%d).Valid() = true, want false (код вне ТЗ 6.2)", code)
		}
	}
}

func TestParseStatus(t *testing.T) {
	cases := []struct {
		in   string
		want Status
	}{
		{"new", StatusNew},
		{"deleted", StatusDeleted},
		{" alive ", StatusAlive}, // surrounding whitespace does not matter
		{"Temp_Error", StatusTempError},
	}
	for _, tc := range cases {
		got, err := ParseStatus(tc.in)
		if err != nil {
			t.Errorf("ParseStatus(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseStatus(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestParseStatusErrors(t *testing.T) {
	for _, in := range []string{"", "parked", "no-http", "notdelegated", "new domain", "не-домен"} {
		if _, err := ParseStatus(in); err == nil {
			t.Errorf("ParseStatus(%q) = nil error, want ошибка", in)
		}
	}
	// The error message lists the valid names — a hint to the caller.
	_, err := ParseStatus("parked")
	if err == nil || !strings.Contains(err.Error(), "not_delegated") {
		t.Errorf("ошибка ParseStatus должна называть допустимые имена, получено: %v", err)
	}
}

// TestParseErrorZeroCode — on a parse error the zero code is returned
// (the Go zero-value convention). For Scheme the pin is meaningful: 0 is
// an invalid code, the ±1 mutant would give the valid SchemeHTTP(1) —
// poisonous when the caller ignores err; for Status 0 is the valid new,
// so the pin only fixes the convention (the caller must check err).
func TestParseErrorZeroCode(t *testing.T) {
	if got, err := ParseStatus("nope"); err == nil || got != 0 {
		t.Errorf("ParseStatus(ошибка) = %d, %v, want 0 и ошибка", int16(got), err)
	}
	if got, err := ParseScheme("ftp"); err == nil || got != 0 {
		t.Errorf("ParseScheme(ошибка) = %d, %v, want 0 и ошибка", int16(got), err)
	}
}

func TestCheckState(t *testing.T) {
	want := []struct {
		code int16
		name string
	}{
		{0, "pending"},
		{1, "queued"},
		{2, "done"},
	}
	for _, w := range want {
		cs := CheckState(w.code)
		if got := cs.String(); got != w.name {
			t.Errorf("CheckState(%d).String() = %q, want %q", w.code, got, w.name)
		}
		if !cs.Valid() {
			t.Errorf("CheckState(%d).Valid() = false, want true", w.code)
		}
	}
	for _, code := range []int16{-1, 3, 9} {
		if CheckState(code).Valid() {
			t.Errorf("CheckState(%d).Valid() = true, want false", code)
		}
		if got, want := CheckState(code).String(), "CheckState("+itoa(code)+")"; got != want {
			t.Errorf("CheckState(%d).String() = %q, want %q", code, got, want)
		}
	}
}

func TestVariant(t *testing.T) {
	want := []struct {
		code int16
		name string
	}{
		{0, "main"}, // main (https or the only responding protocol)
		{1, "http"}, // the http variant when the sites differ (spec 5.5 item 5)
	}
	for _, w := range want {
		v := Variant(w.code)
		if got := v.String(); got != w.name {
			t.Errorf("Variant(%d).String() = %q, want %q", w.code, got, w.name)
		}
		if !v.Valid() {
			t.Errorf("Variant(%d).Valid() = false, want true", w.code)
		}
	}
	for _, code := range []int16{-1, 2} {
		if Variant(code).Valid() {
			t.Errorf("Variant(%d).Valid() = true, want false", code)
		}
	}
}

func TestScheme(t *testing.T) {
	if got, want := SchemeHTTP.String(), "http"; got != want {
		t.Errorf("SchemeHTTP.String() = %q, want %q", got, want)
	}
	if got, want := SchemeHTTPS.String(), "https"; got != want {
		t.Errorf("SchemeHTTPS.String() = %q, want %q", got, want)
	}
	if int16(SchemeHTTP) != 1 || int16(SchemeHTTPS) != 2 {
		t.Errorf("коды схем = %d/%d, want 1/2 (комментарий миграции 000001)", int16(SchemeHTTP), int16(SchemeHTTPS))
	}
	if !SchemeHTTP.Valid() || !SchemeHTTPS.Valid() {
		t.Error("SchemeHTTP/HTTPS.Valid() = false, want true")
	}
	for _, code := range []int16{0, 3, -1} {
		if Scheme(code).Valid() {
			t.Errorf("Scheme(%d).Valid() = true, want false", code)
		}
	}
}

func TestParseScheme(t *testing.T) {
	cases := []struct {
		in   string
		want Scheme
	}{
		{"http", SchemeHTTP},
		{"https", SchemeHTTPS},
		{" HTTPS ", SchemeHTTPS},
	}
	for _, tc := range cases {
		got, err := ParseScheme(tc.in)
		if err != nil {
			t.Errorf("ParseScheme(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseScheme(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
	for _, in := range []string{"", "ftp", "httpx"} {
		if _, err := ParseScheme(in); err == nil {
			t.Errorf("ParseScheme(%q) = nil error, want ошибка", in)
		}
	}
}

func TestAliasKind(t *testing.T) {
	want := []struct {
		code int16
		name string
	}{
		{0, "none"},      // no alias
		{1, "identical"}, // identical to another subdomain (spec 5.5 item 6)
		{2, "redirect"},  // a redirect to another subdomain
	}
	for _, w := range want {
		a := AliasKind(w.code)
		if got := a.String(); got != w.name {
			t.Errorf("AliasKind(%d).String() = %q, want %q", w.code, got, w.name)
		}
		if !a.Valid() {
			t.Errorf("AliasKind(%d).Valid() = false, want true", w.code)
		}
	}
	for _, code := range []int16{-1, 3} {
		if AliasKind(code).Valid() {
			t.Errorf("AliasKind(%d).Valid() = true, want false", code)
		}
	}
}

func TestAction(t *testing.T) {
	want := []struct {
		code int16
		name string
	}{
		{1, "add"},
		{2, "delete"},
	}
	for _, w := range want {
		a := Action(w.code)
		if got := a.String(); got != w.name {
			t.Errorf("Action(%d).String() = %q, want %q", w.code, got, w.name)
		}
		if !a.Valid() {
			t.Errorf("Action(%d).Valid() = false, want true", w.code)
		}
	}
	for _, code := range []int16{-1, 0, 3} {
		if Action(code).Valid() {
			t.Errorf("Action(%d).Valid() = true, want false", code)
		}
	}
}

// itoa — a short wrapper for want values like "Status(7)".
func itoa(n int16) string { return strconv.Itoa(int(n)) }

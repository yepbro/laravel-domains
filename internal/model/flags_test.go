// Unit tests of Flags: the bit numbers (a strict cross-check with the
// comment of the domains.flags column in migration 000001), Set/Test/Clear,
// the named is_idn/render_pending bits, the output.
package model

import "testing"

// TestFlagsBitsMatchSchema — the bits are pinned: bit 0 is_idn and bit 1
// render_pending — the comment of migration 000001; bit 2 http_published —
// T-108, bit 3 dead_host_cutoff — T-164 (the model and docs/schema.md;
// no migration is needed — the flags column initially allows additional
// bits, spec 6.1 «…»).
func TestFlagsBitsMatchSchema(t *testing.T) {
	if FlagIDN != 1 {
		t.Errorf("FlagIDN = %d, want 1 (bit 0)", int16(FlagIDN))
	}
	if FlagRenderPending != 2 {
		t.Errorf("FlagRenderPending = %d, want 2 (bit 1)", int16(FlagRenderPending))
	}
	if FlagHTTPPublished != 4 {
		t.Errorf("FlagHTTPPublished = %d, want 4 (bit 2, T-108)", int16(FlagHTTPPublished))
	}
	if FlagDeadHostCutoff != 8 {
		t.Errorf("FlagDeadHostCutoff = %d, want 8 (bit 3, T-164)", int16(FlagDeadHostCutoff))
	}
}

func TestFlagsSetTestClear(t *testing.T) {
	var f Flags
	if f != 0 {
		t.Fatalf("the zero Flags value = %d, want 0", int16(f))
	}
	if f.Test(FlagIDN) || f.Test(FlagRenderPending) {
		t.Error("the zero Flags: Test returned true")
	}

	f.Set(FlagIDN)
	if !f.Test(FlagIDN) {
		t.Error("after Set(FlagIDN): Test(FlagIDN) = false")
	}
	if f.Test(FlagRenderPending) {
		t.Error("after Set(FlagIDN): Test(FlagRenderPending) = true")
	}

	f.Set(FlagRenderPending)
	if !f.Test(FlagIDN) || !f.Test(FlagRenderPending) {
		t.Errorf("after Set of both: %s, want both bits", f)
	}

	f.Clear(FlagIDN)
	if f.Test(FlagIDN) {
		t.Error("after Clear(FlagIDN): Test(FlagIDN) = true")
	}
	if !f.Test(FlagRenderPending) {
		t.Error("Clear(FlagIDN) cleared a foreign bit render_pending")
	}

	f.Clear(FlagRenderPending)
	if f != 0 {
		t.Errorf("after Clear of both: %d, want 0", int16(f))
	}
}

// TestFlagsTestAnyOf — Test with several bits is true if at least one is
// set (a convenient way to check "whether there is anything of …").
func TestFlagsTestAnyOf(t *testing.T) {
	f := Flags(0)
	f.Set(FlagRenderPending)
	if !f.Test(FlagIDN | FlagRenderPending) {
		t.Error("Test(FlagIDN|FlagRenderPending) = false with render_pending set")
	}
	if f.Test(Flags(0)) {
		t.Error("Test(0) = true, want false (no bit requested)")
	}
}

func TestFlagsNamedHelpers(t *testing.T) {
	var f Flags
	f.Set(FlagIDN)
	if !f.IsIDN() || f.IsRenderPending() || f.IsHTTPPublished() || f.IsDeadHostCutoff() {
		t.Errorf("IsIDN/IsRenderPending/IsHTTPPublished/IsDeadHostCutoff = %v/%v/%v/%v, want true/false/false/false",
			f.IsIDN(), f.IsRenderPending(), f.IsHTTPPublished(), f.IsDeadHostCutoff())
	}
	f.Set(FlagRenderPending)
	f.Clear(FlagIDN)
	if f.IsIDN() || !f.IsRenderPending() || f.IsHTTPPublished() || f.IsDeadHostCutoff() {
		t.Errorf("IsIDN/IsRenderPending/IsHTTPPublished/IsDeadHostCutoff = %v/%v/%v/%v, want false/true/false/false",
			f.IsIDN(), f.IsRenderPending(), f.IsHTTPPublished(), f.IsDeadHostCutoff())
	}
	f.Set(FlagHTTPPublished)
	f.Clear(FlagRenderPending)
	if f.IsRenderPending() || !f.IsHTTPPublished() || f.IsDeadHostCutoff() {
		t.Errorf("IsRenderPending/IsHTTPPublished/IsDeadHostCutoff = %v/%v/%v, want false/true/false",
			f.IsRenderPending(), f.IsHTTPPublished(), f.IsDeadHostCutoff())
	}
	f.Set(FlagDeadHostCutoff)
	f.Clear(FlagHTTPPublished)
	if f.IsHTTPPublished() || !f.IsDeadHostCutoff() {
		t.Errorf("IsHTTPPublished/IsDeadHostCutoff = %v/%v, want false/true",
			f.IsHTTPPublished(), f.IsDeadHostCutoff())
	}
}

func TestFlagsString(t *testing.T) {
	cases := []struct {
		f    Flags
		want string
	}{
		{0, "0"},
		{FlagIDN, "is_idn"},
		{FlagRenderPending, "render_pending"},
		{FlagIDN | FlagRenderPending, "is_idn|render_pending"},
		{FlagHTTPPublished, "http_published"},
		{FlagIDN | FlagHTTPPublished, "is_idn|http_published"},
		{FlagIDN | FlagRenderPending | FlagHTTPPublished, "is_idn|render_pending|http_published"},
		{FlagDeadHostCutoff, "dead_host_cutoff"},
		{FlagIDN | FlagDeadHostCutoff, "is_idn|dead_host_cutoff"},
		{Flags(1 << 4), "0x10"},                     // an unknown bit — the hex remainder
		{FlagIDN | Flags(1<<5|1<<6), "is_idn|0x60"}, // named + unknown
	}
	for _, tc := range cases {
		if got := tc.f.String(); got != tc.want {
			t.Errorf("Flags(%d).String() = %q, want %q", int16(tc.f), got, tc.want)
		}
	}
}

// TestFlagsIDNValue — the set is_idn bit corresponds to the value 1 of the
// flags column (as in the DB when inserting a domain with an xn--… label,
// spec 5.2 item 9).
func TestFlagsIDNValue(t *testing.T) {
	var f Flags
	f.Set(FlagIDN)
	if int16(f) != 1 {
		t.Errorf("Flags with is_idn = %d, want 1", int16(f))
	}
}

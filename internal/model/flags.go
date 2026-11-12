package model

import (
	"fmt"
	"strings"
)

// Flags — bit flags of a domain, the domains.flags column (smallint,
// spec 6.1). Occupied bits:
//
//	bit 0 — is_idn: the domain label contains xn--, i.e. the domain is
//	         given in unicode notation (spec 5.2 item 9; punycode is
//	         stored inside the system, and the bit is the only trace of
//	         the original notation);
//	bit 1 — render_pending: the domain is waiting for a render — the
//	         q:render queue has overflowed, the domain is processed
//	         later and does not hold up the other stages (spec 5.6);
//	bit 2 — http_published: the q:http task of the current DNS episode
//	         has been published while the episode result is not recorded
//	         yet — a publication staging mark (T-108): redelivery of a
//	         q:dns chunk does not duplicate the task but completes the
//	         way through to the record;
//	bit 3 — dead_host_cutoff: the last HTTP episode finished on a dead
//	         apex+www signature (T-164) — a sign used by the control
//	         selection of false cutoffs (the result record sets/clears
//	         the bit).
//
// The remaining bits are free (in spec 6.1 «…») — new flags are added as
// powers-of-two constants.
type Flags int16

// Flags bits.
const (
	// FlagIDN — bit 0: is_idn (spec 5.2 item 9). A shift by zero is 1 in
	// both directions (1<<0 == 1>>0) — the bitwise mutant of the line is
	// equivalent; the bit value is pinned by TestFlagsBitsMatchSchema.
	// mutator-disable-next-line arithmetic/bitwise
	FlagIDN Flags = 1 << 0
	// FlagRenderPending — bit 1: render_pending (spec 5.6).
	FlagRenderPending Flags = 1 << 1
	// FlagHTTPPublished — bit 2: http_published — the staging mark
	// «the q:http task of this episode has been published» (T-108,
	// DNS-04). Lifecycle: the dns-worker sets the bit right after a
	// successful q:http publication (before the episode result is
	// recorded); it is cleared by the result record (done) and by the
	// scheduler enqueueing (pending → queued — every new episode starts
	// with a clean bit). While the bit is set, redelivery of a chunk
	// skips the publication — a duplicate q:http (a full re-check over
	// HTTP) is not created.
	FlagHTTPPublished Flags = 1 << 2
	// FlagDeadHostCutoff — bit 3: dead_host_cutoff — the last HTTP
	// episode of the domain finished on a dead signature (T-164): apex
	// and www failed on the connect phase (connection refused /
	// connect timeout, both protocols), the other subdomains were not
	// checked — records per §8 («no answer»). The bit is set and cleared
	// by the HTTP episode result record (the flag of the last episode):
	// the control selection of false cutoffs is built on it — a full
	// re-check of cutoff domains, the T-164 criterion «the false share
	// < 1%».
	FlagDeadHostCutoff Flags = 1 << 3
)

// Set sets the given bits.
func (f *Flags) Set(flags Flags) { *f |= flags }

// Clear clears the given bits.
func (f *Flags) Clear(flags Flags) { *f &^= flags }

// Test reports whether at least one of the given bits is set.
func (f Flags) Test(flags Flags) bool { return f&flags != 0 }

// IsIDN — is the is_idn bit set.
func (f Flags) IsIDN() bool { return f.Test(FlagIDN) }

// IsRenderPending — is the render_pending bit set.
func (f Flags) IsRenderPending() bool { return f.Test(FlagRenderPending) }

// IsHTTPPublished — is the http_published bit set (T-108).
func (f Flags) IsHTTPPublished() bool { return f.Test(FlagHTTPPublished) }

// IsDeadHostCutoff — is the dead_host_cutoff bit set (T-164).
func (f Flags) IsDeadHostCutoff() bool { return f.Test(FlagDeadHostCutoff) }

// String returns a human-readable form: "is_idn",
// "is_idn|render_pending|http_published". An empty mask is "0"; unknown
// bits are shown as a hexadecimal remainder "0x…".
func (f Flags) String() string {
	if f == 0 {
		return "0"
	}
	var parts []string
	if f.Test(FlagIDN) {
		parts = append(parts, "is_idn")
	}
	if f.Test(FlagRenderPending) {
		parts = append(parts, "render_pending")
	}
	if f.Test(FlagHTTPPublished) {
		parts = append(parts, "http_published")
	}
	if f.Test(FlagDeadHostCutoff) {
		parts = append(parts, "dead_host_cutoff")
	}
	if rest := f &^ (FlagIDN | FlagRenderPending | FlagHTTPPublished | FlagDeadHostCutoff); rest != 0 {
		parts = append(parts, fmt.Sprintf("0x%x", int16(rest)))
	}
	return strings.Join(parts, "|")
}

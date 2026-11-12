// Package idn — domain name normalization (spec 5.2 of the main spec).
//
// The rule of spec 5.2: the ASCII form (punycode, xn--…) is stored
// everywhere inside the system; the unicode form is derived, for display.
// The Normalize pipeline:
//
//   - lower case, no spaces and no trailing dot (item 1);
//   - punycode via the Lookup profile (UTS-46) from golang.org/x/net/idna
//     (items 2–3): unicode labels are encoded, xn-- labels are verified
//     by decoding, invalid ones are rejected;
//   - lengths in the ASCII form: a label ≤ 63, a full name ≤ 253 (item 4);
//   - zone separation by the last label of the name — the TLD (item 5 as
//     amended by the tld-only-zones spec): compound registrations (co.uk,
//     5g.in) are not extracted as a zone — everything before the TLD
//     becomes a name label;
//   - a numeric zone is rejected: a TLD of digits only does not exist
//     (IANA), otherwise export dates and IPv4 literals would pass the
//     zone extraction;
//   - the is_idn flag — a name label contains xn-- (item 9): bit 0 of
//     domains.flags (model.FlagIDN, spec 5.2 item 9).
//
// Errors are typed (*Error: kind, name, cause): the ingest rejected log
// (spec 5.1, task T-016) uses the kind as the record rejection reason.
//
// The package does neither file ingestion (the ingest stage, T-013…T-021)
// nor host extraction from Location/meta refresh (T-047/T-048): already
// extracted names and hosts arrive here.
package idn

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	"golang.org/x/net/idna"
)

// DNS name limits (spec 5.2 item 4) and the A-label prefix (RFC 5890).
const (
	maxLabelLen = 63  // a label ≤ 63 octets in the ASCII form
	maxNameLen  = 253 // a full name ≤ 253 octets in the ASCII form
	xnPrefix    = "xn--"
)

// ErrorKind is a normalization error kind: a stable rejection reason for
// the rejected log (spec 5.1) and crawlerctl output.
type ErrorKind string

const (
	// ErrEmpty — the name is empty (after removing spaces and trailing dots).
	ErrEmpty ErrorKind = "empty"
	// ErrInvalid — the name fails UTS-46/IDNA or contains an empty label
	// (including an invalid xn--: spec 5.2 item 3).
	ErrInvalid ErrorKind = "invalid"
	// ErrLabelLen — a label longer than 63 octets (the ASCII form).
	ErrLabelLen ErrorKind = "label_len"
	// ErrNameLen — the name is longer than 253 octets (the ASCII form).
	ErrNameLen ErrorKind = "name_len"
	// ErrNoLabel — no label before the zone: the name itself is a TLD
	// (com, москва), no domain can be made of it.
	ErrNoLabel ErrorKind = "no_label"
	// ErrNumericZone — the last label of the name (the zone) consists of
	// digits only: numeric TLDs do not exist (IANA), and without this
	// check export dates pass the zone extraction ("18.06.1999" → zone
	// 1999) as do IPv4 literals ("1.2.3.4" → zone 4).
	ErrNumericZone ErrorKind = "numeric_zone"
)

// kindDescriptions — the kind texts for Error() and String().
var kindDescriptions = map[ErrorKind]string{
	ErrEmpty:       "empty name",
	ErrInvalid:     "invalid domain name",
	ErrLabelLen:    "label longer than 63 characters",
	ErrNameLen:     "name longer than 253 characters",
	ErrNoLabel:     "no label before the zone",
	ErrNumericZone: "numeric zone — no such TLDs exist",
}

// String returns the kind text (the style of model.Status.String).
func (k ErrorKind) String() string {
	if d, ok := kindDescriptions[k]; ok {
		return d
	}
	return string(k)
}

// Error — a normalization error: the kind, the original name and the cause.
// The name is truncated to maxErrNameLen runes: the input is untrusted and
// can be arbitrarily long, and the rejected log does not need long junk.
type Error struct {
	Kind ErrorKind // the kind — the rejection reason
	Name string    // the original name (truncated)
	Err  error     // the cause (for example, an idna error); may be nil
}

// maxErrNameLen — how many runes of the original name to keep in the error.
const maxErrNameLen = 255

func newErr(kind ErrorKind, name string, cause error) *Error {
	if runes := []rune(name); len(runes) > maxErrNameLen {
		name = string(runes[:maxErrNameLen]) + "…"
	}
	return &Error{Kind: kind, Name: name, Err: cause}
}

func (e *Error) Error() string {
	desc := e.Kind.String()
	if e.Err != nil {
		return fmt.Sprintf("idn: %s: %q: %v", desc, e.Name, e.Err)
	}
	return fmt.Sprintf("idn: %s: %q", desc, e.Name)
}

// Unwrap gives access to the cause (errors.Is/As along the chain).
func (e *Error) Unwrap() error { return e.Err }

// KindOf returns the normalization error kind; ok = false if err is
// not an *Error (a foreign error).
func KindOf(err error) (ErrorKind, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind, true
	}
	return "", false
}

// Result — a normalized domain name.
type Result struct {
	// ASCII is the full name in the ASCII form (punycode), lower case, no
	// trailing dot. This is how the name is stored in the system (spec 5.2).
	ASCII string
	// Unicode is the unicode form for display (spec 5.2 item 7): ToUnicode
	// on the fly; for a purely ASCII name it equals ASCII.
	Unicode string
	// Zone is the zone of the name: the TLD, the last label of the ASCII
	// form (item 5 as amended by the tld-only-zones spec). Compound
	// suffixes (co.uk, github.io, 5g.in) are not extracted as a zone:
	// example.co.uk has zone uk, example.5g.in has in. The zone of the
	// incoming directory, when checking records, is given explicitly
	// (T-014/T-016) and may be deeper than the TLD (test zones); Zone is
	// the way to separate the zone when it is not known in advance
	// (zone-less files, output).
	Zone string
	// Label is the name without the zone (the domains.label column): the
	// left part up to Zone. A compound label is the norm for zones with a
	// second-level registration: example.5g.in → label example.5g (zone
	// in), example.co.uk → example.co (zone uk).
	Label string
	// IsIDN — a name label contains xn-- (spec 5.2 item 9): bit 0 of
	// domains.flags (model.FlagIDN). The domain label is checked, not the
	// full name: example.рф (zone xn--p1ai) does not get the bit — the
	// label itself has no punycode; the bit does not belong to the whole
	// zone.
	IsIDN bool
}

// Normalize converts the full domain name to the ASCII form and splits it
// into the zone and the label (spec 5.2 items 1–5, 9). This is how records
// of the incoming files (spec 5.1) and the operator input in crawlerctl
// (T-081) are normalized.
func Normalize(name string) (Result, error) {
	ascii, err := normalizeASCII(name)
	if err != nil {
		return Result{}, err
	}
	// Item 5 (as amended by the tld-only-zones spec): the zone is the last
	// label of the name (the TLD): example.5g.in has zone in, label
	// example.5g; no dots — the name itself is a TLD, there is no label
	// before the zone. Compound suffixes (co.uk, github.io) are not
	// extracted as a zone — everything before the TLD is the label.
	i := strings.LastIndexByte(ascii, '.')
	zone := ascii[i+1:] // i < 0 → the zone is the whole name (a bare TLD), the case below
	// A numeric zone is not a domain: export dates ("18.06.1999") and
	// IPv4 literals ("1.2.3.4") pass UTS-46, and numeric TLDs do not
	// exist (IANA) — a last label of digits (1999, 4) cannot be a zone;
	// the check runs before ErrNoLabel so that a bare numeric label
	// ("1999") is classified as a numeric zone, as before.
	// A numeric label in an alphabetic zone (123.com) is a regular name.
	if isNumericZone(zone) {
		return Result{}, newErr(ErrNumericZone, name, nil)
	}
	if i < 0 {
		return Result{}, newErr(ErrNoLabel, name, nil)
	}
	label := ascii[:i]
	// zone == "" and label == "" are unreachable: normalizeASCII guarantees
	// a non-empty ASCII without empty labels and trailing dots — the dot is
	// neither at the start nor at the end of the name, the labels on both
	// sides are non-empty. The branch is a guard against future changes of
	// the pipeline; both condition mutants are equivalent, the annotation
	// silences the whole line: a granularity trade-off,
	// docs/mutation-testing.md §4.
	// mutator-disable-next-line expression/remove
	if zone == "" || label == "" {
		return Result{}, newErr(ErrNoLabel, name, nil)
	}
	return Result{
		ASCII:   ascii,
		Unicode: ToUnicode(ascii),
		Zone:    zone,
		Label:   label,
		IsIDN:   HasXNLabel(label),
	}, nil
}

// isNumericZone — all labels of the zone consist of ASCII digits only:
// numeric TLDs do not exist (IANA), so a last label of digits is not the
// zone of the name: export dates ("18.06.1999" — the last label 1999) and
// IPv4 literals ("1.2.3.4" — 4) must be rejected by the zone extraction.
// The zone is a single label (the TLD); the loop over dots is kept for
// direct calls and as a safety net. A numeric label in an alphabetic zone
// (123.com) is not a zone and does not bother the name.
func isNumericZone(zone string) bool {
	if zone == "" {
		return false
	}
	for _, label := range strings.Split(zone, ".") {
		if strings.Trim(label, "0123456789") != "" {
			return false
		}
	}
	return true
}

// NormalizeZone normalizes the name of a zone — the incoming/<zone>
// directory (spec 5.1) — with the same pipeline as a domain (spec 5.2
// item 6: "the zone is normalized the same way too"), but without label
// separation. The zone of the directory does not have to be a TLD (test
// zones like crawl.test, compound paths) — belonging of the zone to any
// list is not checked here, only the name syntax. The unicode form for
// zones.name_unicode is ToUnicode of the zone on the fly.
func NormalizeZone(zone string) (string, error) {
	return normalizeASCII(zone)
}

// HostToASCII converts a host to the ASCII form (spec 5.2 item 8): hosts
// from Location and <meta http-equiv="refresh"> containing unicode are
// encoded to punycode before the DNS query, Host and SNI (the ASCII form
// is always used for DNS and SNI). ASCII hosts are validated and returned
// in lower case; an IPv4 literal passes as is (labels of digits are
// syntactically valid), IPv6 literals are not supported — they are not
// names, the caller (T-047/T-048) handles them separately via net.ParseIP.
// The host is passed without scheme, port and path (url.URL.Hostname); the
// Location/meta refresh interception itself — tasks T-047/T-048.
func HostToASCII(host string) (string, error) {
	return normalizeASCII(host)
}

// normalizeASCII — the shared pipeline of Normalize/NormalizeZone/HostToASCII:
// cleanup (item 1), punycode and xn-- verification (items 2–3), lengths (item 4).
func normalizeASCII(name string) (string, error) {
	s := cleanup(name)
	if s == "" {
		return "", newErr(ErrEmpty, name, nil)
	}
	// The Lookup profile (UTS-46): case mapping, encoding of unicode
	// labels to punycode, verification of xn-- A-labels by decoding,
	// STD3, Bidi and contextual rules (spec 5.2 items 2–3).
	ascii, err := idna.Lookup.ToASCII(s)
	if err != nil {
		return "", newErr(ErrInvalid, name, err)
	}
	// idna.Lookup does not reject empty labels ("a..b" passes, "xn--"
	// decodes to empty — the behavior of golang.org/x/net v0.61) and does
	// not check lengths — we control that ourselves (spec 5.2 items 3–4);
	// the behavior is pinned by tests (TestNormalizeInvalid), this is a
	// safety net. The branch is reachable with the input "xn--" (Lookup
	// decodes it to empty, ToASCII("xn--") = "", nil) and with input made
	// of characters fully ignored by the mapping (BOM/U+00AD/U+200B/
	// U+2060): in both cases ascii == "" and Split("", ".") below gives
	// [""] — the next check answers with the same "empty label" error
	// with the same Kind/Name/cause, the return-removal mutant is
	// equivalent.
	// mutator-disable-next-line branch/if
	if ascii == "" {
		return "", newErr(ErrInvalid, name, errors.New("empty label"))
	}
	for _, label := range strings.Split(ascii, ".") {
		if label == "" {
			return "", newErr(ErrInvalid, name, errors.New("empty label"))
		}
		if len(label) > maxLabelLen {
			return "", newErr(ErrLabelLen, name, nil)
		}
	}
	if len(ascii) > maxNameLen {
		return "", newErr(ErrNameLen, name, nil)
	}
	return ascii, nil
}

// cleanup performs spec 5.2 item 1: lower case, no whitespace characters
// and no trailing dots. All spaces are removed, not only at the edges
// ("remove spaces" without qualification in the spec): input typos
// ("при мер.рф") must not send the name to the rejected log. Hidden
// characters are not spaces and are left to UTS-46: BOM (U+FEFF),
// U+00AD, U+200B and U+2060 are removed by the idna.Lookup mapping
// (ignored), ZWNJ/ZWJ (U+200C/U+200D) pass only per the contextual rules
// (CONTEXTJ), U+180E is disallowed; the corpus — corpus_test.go (T-010).
func cleanup(name string) string {
	// The fast path is a pure optimization: IndexFunc for a name without
	// spaces answers -1, "< 0 → < -1" would send a space-free name to the
	// slow path, which filters only spaces (there are none) and applies
	// the same TrimRight(ToLower) — the result is indistinguishable; the
	// return-removal mutant is the same in reverse (branch/if is keyed by
	// the if string). Grow is a capacity hint, it does not affect the
	// result. Three equivalent mutants.
	// mutator-disable-next-line numbers/decrementer, branch/if
	if strings.IndexFunc(name, unicode.IsSpace) < 0 {
		return strings.TrimRight(strings.ToLower(name), ".")
	}
	var b strings.Builder
	// mutator-disable-next-line statement/remove
	b.Grow(len(name))
	for _, r := range name {
		if !unicode.IsSpace(r) {
			b.WriteRune(r)
		}
	}
	return strings.TrimRight(strings.ToLower(b.String()), ".")
}

// ToUnicode returns the unicode form of an ASCII name for display
// (spec 5.2 item 7: search and output of the unicode names of a domain —
// idna.ToUnicode on the fly, without a separate column in domains). It
// deliberately returns no errors: if decoding failed (the name is not in
// punycode), the input is returned unchanged.
func ToUnicode(ascii string) string {
	// A decoding error and an empty result are one outcome: return the
	// input (idna.ToUnicode("") gives an empty string without an error).
	if u, err := idna.ToUnicode(ascii); err == nil && u != "" {
		return u
	}
	return ascii
}

// HasXNLabel reports whether the name contains at least one xn-- label
// (a punycode A-label, spec 5.2 item 9). The name is the ASCII form with
// dot separators; case does not matter. Normalize uses the function for
// the domain label (Result.IsIDN), but it applies to any name — for
// example, to a zone (zones.name).
func HasXNLabel(name string) bool {
	for _, label := range strings.Split(name, ".") {
		if len(label) >= len(xnPrefix) && strings.EqualFold(label[:len(xnPrefix)], xnPrefix) {
			return true
		}
	}
	return false
}

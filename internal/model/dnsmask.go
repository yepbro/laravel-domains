package model

import "fmt"

// MaxSubdomains — the maximum number of subdomains (spec 5.10): the mask
// occupies a bigint (int8), bits 0..62 are available; bit 63 is the sign
// bit and is forbidden. Adding a new subdomain means a new bit, with no
// data migration.
const MaxSubdomains = 63

// maxSubdomainID — the upper bound of a mask bit number (62).
const maxSubdomainID = MaxSubdomains - 1

// DNSMask — a bitmask of subdomains: the bit numbered subdomains.id denotes
// the corresponding subdomain. Used in the domains.dns_mask column
// (resolving subdomains, spec 5.4 item 4) and laravel_domains.found_on_mask
// (the subdomains where Laravel was found, spec 5.8).
//
// Numbers outside 0..62 are forbidden: bit 63 is the sign bit; setting it
// would make the value negative and would break mask comparisons in SQL
// and Go. Set/Unset reject such numbers with an error (all numbers are
// validated before any change — the mask is never modified partially),
// and Test returns false for an invalid number: a nonexistent bit is not
// set.
type DNSMask int64

// ValidateSubdomainID checks that id is valid as a mask bit number
// (an integer in 0..62). Also used when adding subdomains to the reference
// table (crawlerctl subdomains add, spec 5.10).
func ValidateSubdomainID(id int) error {
	if id < 0 || id > maxSubdomainID {
		return fmt.Errorf("model: номер субдомена %d вне диапазона 0..%d (максимум %d субдомена, бит 63 маски знаковый; ТЗ 5.10)", id, maxSubdomainID, MaxSubdomains)
	}
	return nil
}

// NewDNSMask builds a mask with the bits of the given subdomains.
func NewDNSMask(ids ...int) (DNSMask, error) {
	var m DNSMask
	if err := m.Set(ids...); err != nil {
		return 0, err
	}
	return m, nil
}

// Set sets the bits of the given subdomains. Any invalid number is an
// error; the mask remains unchanged.
func (m *DNSMask) Set(ids ...int) error {
	for _, id := range ids {
		if err := ValidateSubdomainID(id); err != nil {
			return err
		}
	}
	for _, id := range ids {
		*m |= subdomainBit(id)
	}
	return nil
}

// Unset clears the bits of the given subdomains. Any invalid number is an
// error; the mask remains unchanged.
func (m *DNSMask) Unset(ids ...int) error {
	for _, id := range ids {
		if err := ValidateSubdomainID(id); err != nil {
			return err
		}
	}
	for _, id := range ids {
		*m &^= subdomainBit(id)
	}
	return nil
}

// Test reports whether the bit of subdomain id is set (for id outside
// 0..62 — false).
func (m DNSMask) Test(id int) bool {
	return id >= 0 && id <= maxSubdomainID && m&subdomainBit(id) != 0
}

// IDs returns the numbers of the set bits in ascending order — the mask
// decoded in terms of subdomains.id.
func (m DNSMask) IDs() []int {
	ids := make([]int, 0, MaxSubdomains)
	// The iteration bounds are redundant for the result: invalid numbers
	// (-1, 63) are cut off by Test's range check (false), and extra loop
	// iterations add nothing to ids — both mutants of the line are
	// equivalent.
	// mutator-disable-next-line expression/comparison, numbers/decrementer
	for id := 0; id < MaxSubdomains; id++ {
		if m.Test(id) {
			ids = append(ids, id)
		}
	}
	return ids
}

// Count returns the number of set bits (0..63).
func (m DNSMask) Count() int { return len(m.IDs()) }

// IsEmpty reports that no bit is set.
func (m DNSMask) IsEmpty() bool { return m == 0 }

// Valid checks the invariant of a mask read from an external source
// (the DB): the value must be non-negative — bit 63 is not used.
func (m DNSMask) Valid() bool { return m >= 0 }

// subdomainBit returns a single-bit mask; id must have been validated by
// ValidateSubdomainID beforehand.
func subdomainBit(id int) DNSMask { return DNSMask(1) << id }

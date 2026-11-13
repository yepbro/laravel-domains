// Unit tests of DNSMask: setting/clearing/testing bits by subdomain
// number, the 0..62 bounds (bit 63 is the sign bit — forbidden), atomicity
// on error, iteration over the set bits, full filling.
package model

import (
	"math"
	"reflect"
	"testing"
)

func TestDNSMaskSetTestUnset(t *testing.T) {
	m, err := NewDNSMask(0, 5, 62)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{0, 5, 62} {
		if !m.Test(id) {
			t.Errorf("Test(%d) = false after Set", id)
		}
	}
	for _, id := range []int{1, 4, 6, 61} {
		if m.Test(id) {
			t.Errorf("Test(%d) = true for an unset bit", id)
		}
	}

	if err := m.Unset(5); err != nil {
		t.Fatal(err)
	}
	if m.Test(5) {
		t.Error("Test(5) = true after Unset(5)")
	}
	if !m.Test(0) || !m.Test(62) {
		t.Error("Unset(5) cleared other bits")
	}

	// Setting again and clearing an unset bit — idempotency.
	before := m
	if err := m.Set(0); err != nil {
		t.Fatal(err)
	}
	if err := m.Unset(7); err != nil {
		t.Fatal(err)
	}
	if m != before {
		t.Errorf("repeated Set/Unset changed the mask: %d, want %d", int64(m), int64(before))
	}
}

func TestDNSMaskLimits(t *testing.T) {
	if MaxSubdomains != 63 {
		t.Fatalf("MaxSubdomains = %d, want 63 (spec 5.10: the int8 mask limit)", MaxSubdomains)
	}
	if err := ValidateSubdomainID(0); err != nil {
		t.Errorf("ValidateSubdomainID(0) = %v, want nil", err)
	}
	if err := ValidateSubdomainID(62); err != nil {
		t.Errorf("ValidateSubdomainID(62) = %v, want nil (the last allowed bit)", err)
	}
	for _, id := range []int{-1, 63, 64, 1000} {
		if err := ValidateSubdomainID(id); err == nil {
			t.Errorf("ValidateSubdomainID(%d) = nil, want an error (outside 0..62)", id)
		}
	}

	var m DNSMask
	if err := m.Set(63); err == nil {
		t.Error("Set(63) = nil error, want an error (bit 63 is the sign bit)")
	}
	if err := m.Set(-1); err == nil {
		t.Error("Set(-1) = nil error, want an error")
	}
	if err := m.Unset(63); err == nil {
		t.Error("Unset(63) = nil error, want an error")
	}
	if _, err := NewDNSMask(0, 63); err == nil {
		t.Error("NewDNSMask(0, 63) = nil error, want an error")
	}
	if m != 0 {
		t.Errorf("after the failed operations the mask = %d, want 0", int64(m))
	}

	// Test of an invalid number is false, not a panic: a nonexistent bit
	// is not set.
	for _, id := range []int{-1, 63, 100} {
		if m.Test(id) {
			t.Errorf("Test(%d) = true, want false", id)
		}
	}
}

// TestDNSMaskErrorValue — on an error the constructor returns a zero
// mask (the zero-value convention; the ±1 mutants would give -1/1).
func TestDNSMaskErrorValue(t *testing.T) {
	m, err := NewDNSMask(63)
	if err == nil {
		t.Fatal("NewDNSMask(63): want an error")
	}
	if m != 0 {
		t.Errorf("NewDNSMask(63) = %d, want 0 on error", int64(m))
	}
}

// TestDNSMaskTestOutsideRange — an invalid number is not set even in an
// all-ones (negative) mask: bit 63 does not exist, the range check in
// Test cuts it off before the mask is touched.
func TestDNSMaskTestOutsideRange(t *testing.T) {
	if DNSMask(-1).Test(63) {
		t.Error("DNSMask(-1).Test(63) = true, want false (63 is outside 0..62)")
	}
}

// TestDNSMaskAtomicOnError — an error in any number does not change the
// mask (all numbers are validated before applying).
func TestDNSMaskAtomicOnError(t *testing.T) {
	m, err := NewDNSMask(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Set(1, 63); err == nil {
		t.Fatal("Set(1, 63): want an error")
	}
	if m.Test(1) {
		t.Error("the failed Set applied partially: bit 1 is set")
	}
	if err := m.Unset(0, 63); err == nil {
		t.Fatal("Unset(0, 63): want an error")
	}
	if !m.Test(0) {
		t.Error("the failed Unset applied partially: bit 0 is cleared")
	}
}

// TestDNSMaskAllBits — all 63 valid bits: the value is positive (bit 63
// is untouched) and equals MaxInt64.
func TestDNSMaskAllBits(t *testing.T) {
	ids := make([]int, 0, MaxSubdomains)
	for id := 0; id < MaxSubdomains; id++ {
		ids = append(ids, id)
	}
	m, err := NewDNSMask(ids...)
	if err != nil {
		t.Fatal(err)
	}
	if int64(m) != math.MaxInt64 {
		t.Errorf("the mask of all bits 0..62 = %d, want %d (bit 63 is unused)", int64(m), int64(math.MaxInt64))
	}
	if !m.Valid() {
		t.Error("a mask of bits 0..62 must be valid (non-negative)")
	}
	if m.Count() != MaxSubdomains {
		t.Errorf("Count = %d, want %d", m.Count(), MaxSubdomains)
	}
	if got := m.IDs(); len(got) != MaxSubdomains {
		t.Errorf("IDs: %d numbers, want %d", len(got), MaxSubdomains)
	}
}

func TestDNSMaskIDsAscending(t *testing.T) {
	m, err := NewDNSMask(5, 0, 62, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := []int{0, 3, 5, 62}
	if got := m.IDs(); !reflect.DeepEqual(got, want) {
		t.Errorf("IDs() = %v, want %v (ascending)", got, want)
	}
	if m.Count() != len(want) {
		t.Errorf("Count = %d, want %d", m.Count(), len(want))
	}
}

func TestDNSMaskEmpty(t *testing.T) {
	var m DNSMask
	if !m.IsEmpty() {
		t.Error("the zero mask is not IsEmpty")
	}
	if m.Count() != 0 {
		t.Errorf("Count = %d, want 0", m.Count())
	}
	if got := m.IDs(); len(got) != 0 {
		t.Errorf("IDs() = %v, want empty", got)
	}
	if !m.Valid() {
		t.Error("the zero mask must be valid")
	}
	m2, err := NewDNSMask()
	if err != nil {
		t.Fatal(err)
	}
	if !m2.IsEmpty() {
		t.Errorf("NewDNSMask() without arguments = %d, want 0", int64(m2))
	}
}

// TestDNSMaskValid — a value with bit 63 (negative) fails the invariant
// check: such data is never written to the DB.
func TestDNSMaskValid(t *testing.T) {
	if DNSMask(-1).Valid() {
		t.Error("DNSMask(-1).Valid() = true, want false (bit 63 is the sign bit)")
	}
	if !DNSMask(0).Valid() || !DNSMask(math.MaxInt64).Valid() {
		t.Error("non-negative masks must be valid")
	}
}

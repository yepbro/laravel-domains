// Юнит-тесты DNSMask: установка/снятие/проверка битов по номеру
// субдомена, границы 0..62 (бит 63 знаковый — запрещён), атомарность при
// ошибке, итерация установленных битов, полное заполнение.
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
			t.Errorf("Test(%d) = false после установки", id)
		}
	}
	for _, id := range []int{1, 4, 6, 61} {
		if m.Test(id) {
			t.Errorf("Test(%d) = true для неустановленного бита", id)
		}
	}

	if err := m.Unset(5); err != nil {
		t.Fatal(err)
	}
	if m.Test(5) {
		t.Error("Test(5) = true после Unset(5)")
	}
	if !m.Test(0) || !m.Test(62) {
		t.Error("Unset(5) снял другие биты")
	}

	// Повторная установка и снятие неустановленного — идемпотентность.
	before := m
	if err := m.Set(0); err != nil {
		t.Fatal(err)
	}
	if err := m.Unset(7); err != nil {
		t.Fatal(err)
	}
	if m != before {
		t.Errorf("повторные Set/Unset изменили маску: %d, want %d", int64(m), int64(before))
	}
}

func TestDNSMaskLimits(t *testing.T) {
	if MaxSubdomains != 63 {
		t.Fatalf("MaxSubdomains = %d, want 63 (ТЗ 5.10: ограничение маски int8)", MaxSubdomains)
	}
	if err := ValidateSubdomainID(0); err != nil {
		t.Errorf("ValidateSubdomainID(0) = %v, want nil", err)
	}
	if err := ValidateSubdomainID(62); err != nil {
		t.Errorf("ValidateSubdomainID(62) = %v, want nil (последний допустимый бит)", err)
	}
	for _, id := range []int{-1, 63, 64, 1000} {
		if err := ValidateSubdomainID(id); err == nil {
			t.Errorf("ValidateSubdomainID(%d) = nil, want ошибка (вне 0..62)", id)
		}
	}

	var m DNSMask
	if err := m.Set(63); err == nil {
		t.Error("Set(63) = nil error, want ошибка (бит 63 знаковый)")
	}
	if err := m.Set(-1); err == nil {
		t.Error("Set(-1) = nil error, want ошибка")
	}
	if err := m.Unset(63); err == nil {
		t.Error("Unset(63) = nil error, want ошибка")
	}
	if _, err := NewDNSMask(0, 63); err == nil {
		t.Error("NewDNSMask(0, 63) = nil error, want ошибка")
	}
	if m != 0 {
		t.Errorf("после ошибочных операций маска = %d, want 0", int64(m))
	}

	// Test недопустимого номера — false, не паника: несуществующий бит
	// не установлен.
	for _, id := range []int{-1, 63, 100} {
		if m.Test(id) {
			t.Errorf("Test(%d) = true, want false", id)
		}
	}
}

// TestDNSMaskAtomicOnError — ошибка в любом номере не меняет маску
// (проверка всех номеров до применения).
func TestDNSMaskAtomicOnError(t *testing.T) {
	m, err := NewDNSMask(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Set(1, 63); err == nil {
		t.Fatal("Set(1, 63): want ошибка")
	}
	if m.Test(1) {
		t.Error("ошибочный Set частично применился: бит 1 установлен")
	}
	if err := m.Unset(0, 63); err == nil {
		t.Fatal("Unset(0, 63): want ошибка")
	}
	if !m.Test(0) {
		t.Error("ошибочный Unset частично применился: бит 0 снят")
	}
}

// TestDNSMaskAllBits — все 63 допустимых бита: значение положительно
// (бит 63 не тронут) и равно MaxInt64.
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
		t.Errorf("маска всех битов 0..62 = %d, want %d (бит 63 не используется)", int64(m), int64(math.MaxInt64))
	}
	if !m.Valid() {
		t.Error("маска из битов 0..62 обязана быть валидной (неотрицательной)")
	}
	if m.Count() != MaxSubdomains {
		t.Errorf("Count = %d, want %d", m.Count(), MaxSubdomains)
	}
	if got := m.IDs(); len(got) != MaxSubdomains {
		t.Errorf("IDs: %d номеров, want %d", len(got), MaxSubdomains)
	}
}

func TestDNSMaskIDsAscending(t *testing.T) {
	m, err := NewDNSMask(5, 0, 62, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := []int{0, 3, 5, 62}
	if got := m.IDs(); !reflect.DeepEqual(got, want) {
		t.Errorf("IDs() = %v, want %v (по возрастанию)", got, want)
	}
	if m.Count() != len(want) {
		t.Errorf("Count = %d, want %d", m.Count(), len(want))
	}
}

func TestDNSMaskEmpty(t *testing.T) {
	var m DNSMask
	if !m.IsEmpty() {
		t.Error("нулевая маска не IsEmpty")
	}
	if m.Count() != 0 {
		t.Errorf("Count = %d, want 0", m.Count())
	}
	if got := m.IDs(); len(got) != 0 {
		t.Errorf("IDs() = %v, want пусто", got)
	}
	if !m.Valid() {
		t.Error("нулевая маска обязана быть валидной")
	}
	m2, err := NewDNSMask()
	if err != nil {
		t.Fatal(err)
	}
	if !m2.IsEmpty() {
		t.Errorf("NewDNSMask() без аргументов = %d, want 0", int64(m2))
	}
}

// TestDNSMaskValid — значение с битом 63 (отрицательное) не проходит
// проверку инварианта: такие данные в БД не записываются.
func TestDNSMaskValid(t *testing.T) {
	if DNSMask(-1).Valid() {
		t.Error("DNSMask(-1).Valid() = true, want false (бит 63 знаковый)")
	}
	if !DNSMask(0).Valid() || !DNSMask(math.MaxInt64).Valid() {
		t.Error("неотрицательные маски обязаны быть валидными")
	}
}

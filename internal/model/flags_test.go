// Юнит-тесты Flags: номера битов (жёсткая сверка с комментарием колонки
// domains.flags в миграции 000001), Set/Test/Clear, именованные биты
// is_idn/render_pending, вывод.
package model

import "testing"

// TestFlagsBitsMatchSchema — биты зафиксированы: бит 0 is_idn и бит 1
// render_pending — комментарий миграции 000001; бит 2 http_published —
// T-108 (модель и docs/schema.md; миграций не требует — колонка flags
// изначально допускает дополнительные биты, ТЗ 6.1 «…»).
func TestFlagsBitsMatchSchema(t *testing.T) {
	if FlagIDN != 1 {
		t.Errorf("FlagIDN = %d, want 1 (бит 0)", int16(FlagIDN))
	}
	if FlagRenderPending != 2 {
		t.Errorf("FlagRenderPending = %d, want 2 (бит 1)", int16(FlagRenderPending))
	}
	if FlagHTTPPublished != 4 {
		t.Errorf("FlagHTTPPublished = %d, want 4 (бит 2, T-108)", int16(FlagHTTPPublished))
	}
}

func TestFlagsSetTestClear(t *testing.T) {
	var f Flags
	if f != 0 {
		t.Fatalf("нулевое значение Flags = %d, want 0", int16(f))
	}
	if f.Test(FlagIDN) || f.Test(FlagRenderPending) {
		t.Error("нулевая Flags: Test дал true")
	}

	f.Set(FlagIDN)
	if !f.Test(FlagIDN) {
		t.Error("после Set(FlagIDN): Test(FlagIDN) = false")
	}
	if f.Test(FlagRenderPending) {
		t.Error("после Set(FlagIDN): Test(FlagRenderPending) = true")
	}

	f.Set(FlagRenderPending)
	if !f.Test(FlagIDN) || !f.Test(FlagRenderPending) {
		t.Errorf("после Set обоих: %s, want оба бита", f)
	}

	f.Clear(FlagIDN)
	if f.Test(FlagIDN) {
		t.Error("после Clear(FlagIDN): Test(FlagIDN) = true")
	}
	if !f.Test(FlagRenderPending) {
		t.Error("Clear(FlagIDN) снял чужой бит render_pending")
	}

	f.Clear(FlagRenderPending)
	if f != 0 {
		t.Errorf("после Clear обоих: %d, want 0", int16(f))
	}
}

// TestFlagsTestAnyOf — Test с несколькими битами истинен, если установлен
// хотя бы один (удобно проверять «есть ли что-то из …»).
func TestFlagsTestAnyOf(t *testing.T) {
	f := Flags(0)
	f.Set(FlagRenderPending)
	if !f.Test(FlagIDN | FlagRenderPending) {
		t.Error("Test(FlagIDN|FlagRenderPending) = false при установленном render_pending")
	}
	if f.Test(Flags(0)) {
		t.Error("Test(0) = true, want false (ни один бит не запрошен)")
	}
}

func TestFlagsNamedHelpers(t *testing.T) {
	var f Flags
	f.Set(FlagIDN)
	if !f.IsIDN() || f.IsRenderPending() || f.IsHTTPPublished() {
		t.Errorf("IsIDN/IsRenderPending/IsHTTPPublished = %v/%v/%v, want true/false/false",
			f.IsIDN(), f.IsRenderPending(), f.IsHTTPPublished())
	}
	f.Set(FlagRenderPending)
	f.Clear(FlagIDN)
	if f.IsIDN() || !f.IsRenderPending() || f.IsHTTPPublished() {
		t.Errorf("IsIDN/IsRenderPending/IsHTTPPublished = %v/%v/%v, want false/true/false",
			f.IsIDN(), f.IsRenderPending(), f.IsHTTPPublished())
	}
	f.Set(FlagHTTPPublished)
	f.Clear(FlagRenderPending)
	if f.IsRenderPending() || !f.IsHTTPPublished() {
		t.Errorf("IsRenderPending/IsHTTPPublished = %v/%v, want false/true",
			f.IsRenderPending(), f.IsHTTPPublished())
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
		{Flags(1 << 3), "0x8"},                      // неизвестный бит — остаток hex
		{FlagIDN | Flags(1<<4|1<<5), "is_idn|0x30"}, // именованный + неизвестные
	}
	for _, tc := range cases {
		if got := tc.f.String(); got != tc.want {
			t.Errorf("Flags(%d).String() = %q, want %q", int16(tc.f), got, tc.want)
		}
	}
}

// TestFlagsIDNValue — установленному биту is_idn соответствует значение 1
// колонки flags (как в БД при вставке домена с меткой xn--…, ТЗ 5.2 п. 9).
func TestFlagsIDNValue(t *testing.T) {
	var f Flags
	f.Set(FlagIDN)
	if int16(f) != 1 {
		t.Errorf("Flags с is_idn = %d, want 1", int16(f))
	}
}

// Юнит-тесты перечислений model: коды и имена статусов домена (ТЗ 6.2,
// жёсткая сверка с таблицей §6.2 и схемой), check_state, вариант, схема,
// alias_kind, действие ingest-файла; разбор имён и валидация кодов.
package model

import (
	"strconv"
	"strings"
	"testing"
)

// TestStatusCodes сверяет коды и имена статусов жёстко с таблицей ТЗ 6.2
// (значения закреплены колонкой domains.status, миграция 000001).
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
		{7, "invalid"}, // диагностический, T-109
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
		{" alive ", StatusAlive}, // пробелы по краям не важны
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
	// Сообщение об ошибке перечисляет допустимые имена — подсказка вызывающему.
	_, err := ParseStatus("parked")
	if err == nil || !strings.Contains(err.Error(), "not_delegated") {
		t.Errorf("ошибка ParseStatus должна называть допустимые имена, получено: %v", err)
	}
}

// TestParseErrorZeroCode — при ошибке разбора возвращается нулевой код
// (конвенция нулевого значения Go). Для Scheme пин содержателен: 0 —
// невалидный код, мутант ±1 давал бы валидный SchemeHTTP(1) — ядовито
// при игнорировании err вызывающим; для Status 0 — валидный new,
// пин фиксирует только конвенцию (вызывающий обязан проверять err).
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
		{0, "main"}, // основной (https или единственный ответивший протокол)
		{1, "http"}, // http-вариант, если сайты разные (ТЗ 5.5 п. 5)
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
		{0, "none"},      // нет алиаса
		{1, "identical"}, // идентичен другому субдомену (ТЗ 5.5 п. 6)
		{2, "redirect"},  // редирект на другой субдомен
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

// itoa — короткая обёртка для ожидаемых значений вида "Status(7)".
func itoa(n int16) string { return strconv.Itoa(int(n)) }

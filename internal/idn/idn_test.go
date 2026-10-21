// Юнит-тесты нормализации имён (ТЗ 5.2): конвейер Normalize, зоны,
// HostToASCII, ToUnicode, HasXNLabel, типизированные ошибки — ядро пакета
// (T-009). Исчерпывающий корпус по ТЗ 11.1 (ß/ς, эмодзи, нулевая ширина,
// битые xn-- и т. д.) — corpus_test.go (T-010).
package idn

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestNormalizeValid(t *testing.T) {
	tests := []struct {
		name  string
		ascii string
		uni   string
		zone  string
		label string
		isIDN bool
	}{
		// п. 1: регистр, пробелы, завершающая точка.
		{name: "  Example.COM ", ascii: "example.com", uni: "example.com", zone: "com", label: "example"},
		{name: "\uFEFFexample.com", ascii: "example.com", uni: "example.com", zone: "com", label: "example"}, // BOM убирается маппингом UTS-46
		{name: "пример.рф.", ascii: "xn--e1afmkfd.xn--p1ai", uni: "пример.рф", zone: "xn--p1ai", label: "xn--e1afmkfd", isIDN: true},
		{name: "example.com...", ascii: "example.com", uni: "example.com", zone: "com", label: "example"}, // несколько завершающих точек
		{name: "при мер.рф", ascii: "xn--e1afmkfd.xn--p1ai", uni: "пример.рф", zone: "xn--p1ai", label: "xn--e1afmkfd", isIDN: true},
		// п. 2: unicode → punycode.
		{name: "Bücher.de", ascii: "xn--bcher-kva.de", uni: "bücher.de", zone: "de", label: "xn--bcher-kva", isIDN: true},
		// Вход уже в punycode — валиден и не меняется.
		{name: "XN--BCHER-KVA.com", ascii: "xn--bcher-kva.com", uni: "bücher.com", zone: "com", label: "xn--bcher-kva", isIDN: true},
		// п. 5: составная зона, не последняя метка.
		{name: "example.co.uk", ascii: "example.co.uk", uni: "example.co.uk", zone: "co.uk", label: "example"},
		{name: "магазин.рф.co.uk", ascii: "xn--80aairftm.xn--p1ai.co.uk", uni: "магазин.рф.co.uk", zone: "co.uk", label: "xn--80aairftm.xn--p1ai", isIDN: true},
		// п. 9: бит is_idn — по метке домена, зона не считается.
		{name: "example.xn--p1ai", ascii: "example.xn--p1ai", uni: "example.рф", zone: "xn--p1ai", label: "example", isIDN: false},
		// Неизвестный PSL TLD: зона — последняя метка (правило «*»).
		{name: "example.crawl.test", ascii: "example.crawl.test", uni: "example.crawl.test", zone: "test", label: "example.crawl"},
		// Частный суффикс PSL: github.io — зона, как appspot.com.
		{name: "foo.github.io", ascii: "foo.github.io", uni: "foo.github.io", zone: "github.io", label: "foo"},
		// Граница п. 4: метка ровно 63 октета — валидна.
		{name: strings.Repeat("a", 63) + ".com", ascii: strings.Repeat("a", 63) + ".com", uni: strings.Repeat("a", 63) + ".com", zone: "com", label: strings.Repeat("a", 63)},
		// Числовая метка в буквенной зоне — обычное имя: числовых TLD нет,
		// но зона здесь буквенная (мешает только числовая зона).
		{name: "123.com", ascii: "123.com", uni: "123.com", zone: "com", label: "123"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Normalize(tc.name)
			if err != nil {
				t.Fatalf("Normalize(%q) = %v, want без ошибки", tc.name, err)
			}
			if got.ASCII != tc.ascii {
				t.Errorf("ASCII = %q, want %q", got.ASCII, tc.ascii)
			}
			if got.Unicode != tc.uni {
				t.Errorf("Unicode = %q, want %q", got.Unicode, tc.uni)
			}
			if got.Zone != tc.zone {
				t.Errorf("Zone = %q, want %q", got.Zone, tc.zone)
			}
			if got.Label != tc.label {
				t.Errorf("Label = %q, want %q", got.Label, tc.label)
			}
			if got.IsIDN != tc.isIDN {
				t.Errorf("IsIDN = %v, want %v", got.IsIDN, tc.isIDN)
			}
		})
	}
}

func TestNormalizeInvalid(t *testing.T) {
	tests := []struct {
		name string
		kind ErrorKind
	}{
		// п. 1: пустые.
		{"", ErrEmpty},
		{"   ", ErrEmpty},
		{".", ErrEmpty},
		{"..", ErrEmpty},
		// пп. 2–3: невалидные по UTS-46/IDNA.
		{"xn--bcher--kva.com", ErrInvalid},      // битый punycode
		{"xn--.com", ErrInvalid},                // xn-- без полезной нагрузки
		{"example..com", ErrInvalid},            // пустая метка
		{"-lead.example.com", ErrInvalid},       // дефис в начале метки
		{"under_score.example.com", ErrInvalid}, // STD3: подчёркивание
		{"плох_й.рф", ErrInvalid},               // STD3 в unicode-метке
		{"\u200Cample.com", ErrInvalid},         // ZWNJ вне контекстных правил (U+200C)
		// п. 4: длины (ASCII-форма).
		{strings.Repeat("a", 64) + ".com", ErrLabelLen}, // метка 64
		{"àéîõüßàéîõüßàéîõüßàéîõüßàéîõüßàéîõüßàéîõüßàéîõüßàéîõüß.com", ErrLabelLen},                                                           // punycode-метка 64
		{strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 62), ErrNameLen}, // имя 254
		// п. 5: имя — сам публичный суффикс, метки нет.
		{"com", ErrNoLabel},
		{"co.uk", ErrNoLabel},
		{"github.io", ErrNoLabel}, // частный суффикс PSL
		// п. 5: числовая зона — таких TLD не существует (правило «*» PSL
		// отдаёт неизвестный числовой TLD как есть).
		{"18.06.1999", ErrNumericZone}, // дата выгрузки регистратора
		{"1.2.3.4", ErrNumericZone},    // IPv4-литерал
		{"1999", ErrNumericZone},       // голый числовой TLD
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Normalize(tc.name)
			if err == nil {
				t.Fatalf("Normalize(%q) = %+v, want ошибка %s", tc.name, got, tc.kind)
			}
			kind, ok := KindOf(err)
			if !ok {
				t.Fatalf("Normalize(%q): ошибка %v не *idn.Error", tc.name, err)
			}
			if kind != tc.kind {
				t.Errorf("Normalize(%q): kind = %s, want %s (err: %v)", tc.name, kind, tc.kind, err)
			}
		})
	}
}

// TestNormalizeBoundaryLengths — границы лимитов п. 4: 63/253 — валидны.
func TestNormalizeBoundaryLengths(t *testing.T) {
	name := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
	if len(name) != 253 {
		t.Fatalf("длина тестового имени %d, want 253", len(name))
	}
	got, err := Normalize(name)
	if err != nil {
		t.Fatalf("Normalize(имя 253) = %v, want без ошибки", err)
	}
	if got.ASCII != name {
		t.Errorf("ASCII = %q, want имя без изменений", got.ASCII)
	}
	// Последняя метка — неизвестный PSL TLD: зона по правилу «*».
	if want := strings.Repeat("d", 61); got.Zone != want {
		t.Errorf("Zone = %q, want %q", got.Zone, want)
	}
	if want := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63); got.Label != want {
		t.Errorf("Label = %q, want %q", got.Label, want)
	}
}

func TestNormalizeZone(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"com", "com"},
		{" Co.UK ", "co.uk"},
		{"рф.", "xn--p1ai"},
		{" crawl.test\t", "crawl.test"},
		{"XN--P1AI", "xn--p1ai"},
	}
	for _, tc := range tests {
		got, err := NormalizeZone(tc.in)
		if err != nil {
			t.Errorf("NormalizeZone(%q) = %v, want %q", tc.in, err, tc.want)
			continue
		}
		if got != tc.want {
			t.Errorf("NormalizeZone(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	for _, in := range []string{"", "  ", ".", "рф!!"} {
		if _, err := NormalizeZone(in); err == nil {
			t.Errorf("NormalizeZone(%q): нет ошибки, want ошибка", in)
		}
	}
}

func TestHostToASCII(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"Пример.РФ", "xn--e1afmkfd.xn--p1ai"}, // п. 8: unicode → punycode
		{"Example.COM.", "example.com"},        // регистр и точка
		{"192.0.2.1", "192.0.2.1"},             // IPv4-литерал
	}
	for _, tc := range tests {
		got, err := HostToASCII(tc.in)
		if err != nil {
			t.Errorf("HostToASCII(%q) = %v, want %q", tc.in, err, tc.want)
			continue
		}
		if got != tc.want {
			t.Errorf("HostToASCII(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	for _, in := range []string{"xn--bcher--kva.com", "under_score.example.com", "xn--", "::1", "[::1]"} {
		// IPv6-литералы — не имена: скобочная форма из URL и чистый
		// адрес (двоеточие — STD3-нарушение); T-047/T-048 обрабатывает
		// их отдельно через net.ParseIP.
		if _, err := HostToASCII(in); err == nil {
			t.Errorf("HostToASCII(%q): нет ошибки, want ошибка", in)
		}
	}
}

func TestToUnicode(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"xn--e1afmkfd.xn--p1ai", "пример.рф"}, // п. 7: вывод на лету
		{"xn--bcher-kva.com", "bücher.com"},
		{"example.com", "example.com"}, // без punycode — как есть
		{"!!!garbage", "!!!garbage"},   // ошибка декодирования — вход
		{"xn--", "xn--"},               // пустой результат декодирования — вход, не пустая строка
		{"", ""},                       // пустая строка — пустой ответ
	}
	for _, tc := range tests {
		if got := ToUnicode(tc.in); got != tc.want {
			t.Errorf("ToUnicode(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestHasXNLabel(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"xn--e1afmkfd.xn--p1ai", true},
		{"example.com", false},
		{"XN--BCHER-KVA.com", true},     // регистр не важен
		{"axn--foo.example.com", false}, // префикс не в начале метки
		{"xn--", true},                  // метка ровно «xn--»: префикс совпадает целиком
		{"", false},
	}
	for _, tc := range tests {
		if got := HasXNLabel(tc.in); got != tc.want {
			t.Errorf("HasXNLabel(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestError — структура типизированной ошибки: класс, имя, причина,
// оборачивание (rejected-лог T-016 обернёт её в свою ошибку).
func TestError(t *testing.T) {
	_, err := Normalize("xn--bcher--kva.com")
	if err == nil {
		t.Fatal("Normalize(битый xn--): нет ошибки")
	}
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("ошибка %v не *idn.Error", err)
	}
	if e.Kind != ErrInvalid {
		t.Errorf("Kind = %s, want %s", e.Kind, ErrInvalid)
	}
	if e.Name != "xn--bcher--kva.com" {
		t.Errorf("Name = %q, want исходное имя", e.Name)
	}
	if e.Err == nil {
		t.Error("Err = nil, want причина (ошибка idna)")
	}
	if msg := e.Error(); !strings.Contains(msg, "невалидное имя домена") || !strings.Contains(msg, "xn--bcher--kva.com") {
		t.Errorf("Error() = %q, want класс и имя", msg)
	}

	// Формат Error() — точные строки, с причиной и без (мутант
	// вырезания return терял бы причину, оставляя формат «: %v»).
	withCause := &Error{Kind: ErrNameLen, Name: "x", Err: errors.New("причина")}
	if got, want := withCause.Error(), `idn: имя длиннее 253 символов: "x": причина`; got != want {
		t.Errorf("Error() с причиной = %q, want %q", got, want)
	}
	withoutCause := &Error{Kind: ErrEmpty, Name: "y"}
	if got, want := withoutCause.Error(), `idn: пустое имя: "y"`; got != want {
		t.Errorf("Error() без причины = %q, want %q", got, want)
	}

	// Класс извлекается из обёрнутой ошибки (fmt.Errorf %w).
	wrapped := fmt.Errorf("строка 42: %w", err)
	kind, ok := KindOf(wrapped)
	if !ok || kind != ErrInvalid {
		t.Errorf("KindOf(обёрнутая) = %s, %v; want %s, true", kind, ok, ErrInvalid)
	}
	if _, ok := KindOf(errors.New("чужая")); ok {
		t.Error("KindOf(чужая ошибка) = true, want false")
	}

	// Причина доступна через Unwrap (ошибка idna).
	if errors.Unwrap(e) == nil {
		t.Error("Unwrap(e) = nil, want причина")
	}
}

// TestErrorNameTruncated — недоверенный вход не раздувает ошибку:
// имя обрезается до maxErrNameLen рун.
func TestErrorNameTruncated(t *testing.T) {
	long := strings.Repeat("р", 1000)
	_, err := Normalize(long)
	if err == nil {
		t.Fatal("Normalize(1000 р): нет ошибки")
	}
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("ошибка %v не *idn.Error", err)
	}
	if runes := len([]rune(e.Name)); runes > maxErrNameLen+1 {
		t.Errorf("Name = %d рун, want ≤ %d+1", runes, maxErrNameLen)
	}
	if msg := e.Error(); len(msg) > 1024 {
		t.Errorf("Error() длиной %d, want ≤ 1024", len(msg))
	}

	// Граница: имя ровно maxErrNameLen рун не обрезается.
	boundary := strings.Repeat("р", maxErrNameLen)
	_, err = Normalize(boundary)
	if err == nil {
		t.Fatal("Normalize(граничное имя): нет ошибки")
	}
	if !errors.As(err, &e) {
		t.Fatalf("ошибка %v не *idn.Error", err)
	}
	if e.Name != boundary {
		t.Errorf("Name обрезано: %d рун, want %d без обрезки", len([]rune(e.Name)), maxErrNameLen)
	}

	// Границы обрезки — литералами (проверки выше адаптивны через
	// константу и мутанта ±1 не замечают): ровно 255 рун не режется,
	// 256 — 255 рун + «…».
	exactly := strings.Repeat("р", 255)
	_, err = Normalize(exactly)
	if err == nil {
		t.Fatal("Normalize(255 р): нет ошибки")
	}
	if !errors.As(err, &e) {
		t.Fatalf("ошибка %v не *idn.Error", err)
	}
	if e.Name != exactly {
		t.Errorf("имя ровно в лимите обрезано: %d рун, want 255", len([]rune(e.Name)))
	}
	over := strings.Repeat("р", 256)
	_, err = Normalize(over)
	if err == nil {
		t.Fatal("Normalize(256 р): нет ошибки")
	}
	if !errors.As(err, &e) {
		t.Fatalf("ошибка %v не *idn.Error", err)
	}
	if want := strings.Repeat("р", 255) + "…"; e.Name != want {
		t.Errorf("имя над лимитом = %d рун, want 255 рун + «…»", len([]rune(e.Name)))
	}
}

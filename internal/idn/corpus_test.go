// Исчерпывающий корпус тестов нормализации имён — задача T-010 (ТЗ 11.1,
// 5.2): смешанный регистр, ß/ς, эмодзи, символы нулевой ширины, валидные и
// битые xn--, длины, составные зоны, unicode-каталоги зон, хосты, края.
//
// Ожидания зафиксированы по фактическому профилю UTS-46 из golang.org/x/net
// v0.61.0 (таблицы Unicode 15, nontransitional: const transitionalLookup =
// false в idna.go), версия зафиксирована в go.sum — поведение стабильно.
// Ядро конвейера (регистр, пробелы/точка, базовые xn--, длины) — в
// idn_test.go (T-009); здесь — группы корпуса ТЗ 11.1.
package idn

import (
	"strings"
	"testing"
)

// assertNormalize — успех: Normalize(in) без ошибки, результат равен want
// (Result сравним: все поля — скаляры).
func assertNormalize(t *testing.T, in string, want Result) {
	t.Helper()
	got, err := Normalize(in)
	if err != nil {
		t.Fatalf("Normalize(%q) = %v, want без ошибки", in, err)
	}
	if got != want {
		t.Errorf("Normalize(%q) = %+v, want %+v", in, got, want)
	}
}

// assertNormalizeError — отказ: ошибка *Error заданного класса.
func assertNormalizeError(t *testing.T, in string, want ErrorKind) {
	t.Helper()
	_, err := Normalize(in)
	if err == nil {
		t.Fatalf("Normalize(%q): нет ошибки, want %s", in, want)
	}
	kind, ok := KindOf(err)
	if !ok {
		t.Fatalf("Normalize(%q): ошибка %v не *idn.Error", in, err)
	}
	if kind != want {
		t.Errorf("Normalize(%q): kind = %s, want %s (err: %v)", in, kind, want, err)
	}
}

// TestNormalizeCaseMapping — смешанный регистр и UTS-46-маппинг (ТЗ 5.2
// п. 1, 11.1): верхний регистр → нижний, fullwidth-ASCII и альтернативные
// точки-разделители маппятся, A-метки канонизируются к нижнему регистру.
func TestNormalizeCaseMapping(t *testing.T) {
	tests := []struct {
		name string
		want Result
	}{
		// Кириллица: верхний/смешанный регистр и в метке, и в зоне.
		{"България.РФ", Result{"xn--80abgvm6a7d2b.xn--p1ai", "българия.рф", "xn--p1ai", "xn--80abgvm6a7d2b", true}},
		{"БЪЛГАРИЯ.рф", Result{"xn--80abgvm6a7d2b.xn--p1ai", "българия.рф", "xn--p1ai", "xn--80abgvm6a7d2b", true}},
		{"мАгаЗин.РФ", Result{"xn--80aairftm.xn--p1ai", "магазин.рф", "xn--p1ai", "xn--80aairftm", true}},
		// Греческий с тоносом: Πόλης → πόλης (регистр меняется, диакритика
		// и финальная сигма сохраняются).
		{"Πόλης.Γρ", Result{"xn--sxahok1c.xn--oxa2a", "πόλης.γρ", "xn--oxa2a", "xn--sxahok1c", true}},
		// CJK.
		{"日本.JP", Result{"xn--wgv71a.jp", "日本.jp", "jp", "xn--wgv71a", true}},
		// UTS-46 маппинг fullwidth-ASCII (U+FF25… → e…) — как помарка ввода.
		{"ＥＸＡＭＰＬＥ.com", Result{"example.com", "example.com", "com", "example", false}},
		// Альтернативные точки-разделители: U+FF0E (fullwidth) и U+FF61
		// (halfwidth идеографическая) маппятся в «.».
		{"пример．рф", Result{"xn--e1afmkfd.xn--p1ai", "пример.рф", "xn--p1ai", "xn--e1afmkfd", true}},
		{"пример｡рф", Result{"xn--e1afmkfd.xn--p1ai", "пример.рф", "xn--p1ai", "xn--e1afmkfd", true}},
		// Пробельные символы убираются все (ТЗ 5.2 п. 1), в т. ч. табуляция
		// внутри имени (CRLF-хвосты строк incoming — T-015).
		{"при\tмер.рф", Result{"xn--e1afmkfd.xn--p1ai", "пример.рф", "xn--p1ai", "xn--e1afmkfd", true}},
		// Uppercase punycode-метки канонизируются к нижнему регистру
		// (полностью и в смешанном виде) — зона та же.
		{"XN--BCHER-KVA.DE", Result{"xn--bcher-kva.de", "bücher.de", "de", "xn--bcher-kva", true}},
		{"xn--BCHER-kva.de", Result{"xn--bcher-kva.de", "bücher.de", "de", "xn--bcher-kva", true}},
		{"Xn--E1Afmkfd.Xn--P1ai", Result{"xn--e1afmkfd.xn--p1ai", "пример.рф", "xn--p1ai", "xn--e1afmkfd", true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertNormalize(t, tc.name, tc.want)
		})
	}
}

// TestNormalizeDeviations — deviation-символы UTS-46: ß (U+00DF) и финальная
// сигма ς (U+03C2). Transitional-обработка (наследие IDNA2003) отображает
// ß→ss и ς→σ; профиль Lookup в x/net — nontransitional (transitionalLookup =
// false), deviation сохраняется и кодируется в punycode. Кодируем фактическое
// поведение профиля из ТЗ 5.2 п. 2: ß и ss, ς и σ — РАЗНЫЕ имена, что важно
// для де-дупликации (strasse.de и xn--strae-oqa.de — два разных домена).
func TestNormalizeDeviations(t *testing.T) {
	tests := []struct {
		name string
		want Result
	}{
		{"Straße.de", Result{"xn--strae-oqa.de", "straße.de", "de", "xn--strae-oqa", true}},
		{"straße.de", Result{"xn--strae-oqa.de", "straße.de", "de", "xn--strae-oqa", true}},
		// Не схлопывается с ß: отдельное имя без punycode и бита is_idn.
		{"STRASSE.de", Result{"strasse.de", "strasse.de", "de", "strasse", false}},
		// Финальная сигма ς сохраняется и отличима от σ.
		{"πόλης.γρ", Result{"xn--sxahok1c.xn--oxa2a", "πόλης.γρ", "xn--oxa2a", "xn--sxahok1c", true}},
		{"πόλησ.γρ", Result{"xn--sxahoo6b.xn--oxa2a", "πόλησ.γρ", "xn--oxa2a", "xn--sxahoo6b", true}},
		// Верхний регистр: Σ→σ, Ρ→ρ — обычное отображение регистра, ς в
		// конце при этом не «восстанавливается» (ΞΑΝΘΗ → ξανθη).
		{"ΞΑΝΘΗ.ΓΡ", Result{"xn--mxamctd.xn--oxa2a", "ξανθη.γρ", "xn--oxa2a", "xn--mxamctd", true}},
		{"ΠΑΡΑΔΕΙΓΜΑ.ΓΡ", Result{"xn--mxaaaheg2az3al.xn--oxa2a", "παραδειγμα.γρ", "xn--oxa2a", "xn--mxaaaheg2az3al", true}},
		{"παράδειγμα.γρ", Result{"xn--hxajbheg2az3al.xn--oxa2a", "παράδειγμα.γρ", "xn--oxa2a", "xn--hxajbheg2az3al", true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertNormalize(t, tc.name, tc.want)
		})
	}
}

// TestNormalizeEmoji — эмодзи-домены. В x/net v0.61 профиль Lookup кодирует
// одиночные эмодзи в punycode (xn--e28h.com = 😀.com — такие имена реально
// регистрируются и резолвятся), отдельной политики «отклонять эмодзи» ТЗ 5.2
// не задаёт — зафиксировано поведение профиля из п. 2. Отказ дают
// ZWJ-последовательности (👨‍👩‍👦): CONTEXTJ-правило допускает U+200D только в
// арабском/индическом контексте, внутри последовательности эмодзи — ошибка.
func TestNormalizeEmoji(t *testing.T) {
	for _, tc := range []struct {
		name string
		want Result
	}{
		{"😀.com", Result{"xn--e28h.com", "😀.com", "com", "xn--e28h", true}},
		{"cafe☕.com", Result{"xn--cafe-tj3b.com", "cafe☕.com", "com", "xn--cafe-tj3b", true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertNormalize(t, tc.name, tc.want)
		})
	}
	// Готовая A-метка с эмодзи валидна и декодируется для отображения.
	t.Run("xn--e28h.com", func(t *testing.T) {
		assertNormalize(t, "xn--e28h.com", Result{"xn--e28h.com", "😀.com", "com", "xn--e28h", true})
	})
	// ZWJ-последовательность «семья»: U+200D вне допустимого контекста.
	t.Run("ZWJ-последовательность", func(t *testing.T) {
		assertNormalizeError(t, "\U0001F468\u200D\U0001F469\u200D\U0001F466.com", ErrInvalid)
	})
}

// TestNormalizeZeroWidth — символы нулевой ширины. По IdnaMappingTable
// ignored (выбрасываются маппингом в любой позиции): ZWSP U+200B, soft
// hyphen U+00AD, BOM/ZWNBSP U+FEFF, word joiner U+2060. CONTEXTJ: ZWNJ
// U+200C и ZWJ U+200D допустимы только в контексте (арабский — между
// буквами), вне его отклоняются; монгольский разделитель гласных U+180E —
// disallowed.
func TestNormalizeZeroWidth(t *testing.T) {
	for _, tc := range []struct {
		name string
		want Result
	}{
		// ZWSP в середине метки, на границе метки — выбрасывается.
		{"ex\u200Bample.com", Result{"example.com", "example.com", "com", "example", false}},
		{"example\u200B.com", Result{"example.com", "example.com", "com", "example", false}},
		// Soft hyphen и BOM (BOM в начале строки — частый артефакт UTF-8).
		{"a\u00ADb.c", Result{"ab.c", "ab.c", "c", "ab", false}},
		{"a\uFEFFb.c", Result{"ab.c", "ab.c", "c", "ab", false}},
		{"a\u2060b.c", Result{"ab.c", "ab.c", "c", "ab", false}},
		// BOM перед готовой A-меткой (начало строки файла) — выбрасывается,
		// punycode не затрагивается.
		{"\uFEFFxn--e1afmkfd.xn--p1ai", Result{"xn--e1afmkfd.xn--p1ai", "пример.рф", "xn--p1ai", "xn--e1afmkfd", true}},
		// ZWNJ между арабскими буквами — контекстное правило проходит
		// (плюс Bidi-правило RTL-метки).
		{"مثال\u200Cات.com", Result{"xn--mgbajb0kd0137b.com", "مثال\u200Cات.com", "com", "xn--mgbajb0kd0137b", true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertNormalize(t, tc.name, tc.want)
		})
	}
	for _, in := range []string{
		"ex\u200Cample.com", // ZWNJ вне контекста
		"ex\u200Dample.com", // ZWJ вне контекста
		"a\u180Eb.c",        // U+180E — disallowed
	} {
		t.Run(in, func(t *testing.T) {
			assertNormalizeError(t, in, ErrInvalid)
		})
	}
}

// TestNormalizePunycode — валидные xn-- (ТЗ 5.2 п. 3): декодируются,
// канонизируются и остаются A-метками.
func TestNormalizePunycode(t *testing.T) {
	tests := []struct {
		name string
		want Result
	}{
		// Кириллический гомоглон apple.com (аррӏе — все буквы кириллица,
		// ӏ — U+04CF): punycode валиден, имя похоже на чужое — но это
		// вопрос политики, не синтаксиса.
		{"xn--80ak6aa92e.com", Result{"xn--80ak6aa92e.com", "аррӏе.com", "com", "xn--80ak6aa92e", true}},
		{"XN--80AK6AA92E.COM", Result{"xn--80ak6aa92e.com", "аррӏе.com", "com", "xn--80ak6aa92e", true}},
		{"xn--e1afmkfd.xn--p1ai", Result{"xn--e1afmkfd.xn--p1ai", "пример.рф", "xn--p1ai", "xn--e1afmkfd", true}},
		// Deviation ß в готовом punycode: Unicode-форма — straße.de.
		{"xn--strae-oqa.de", Result{"xn--strae-oqa.de", "straße.de", "de", "xn--strae-oqa", true}},
		// Казахская метка.
		{"xn--80ao21a.ai", Result{"xn--80ao21a.ai", "қаз.ai", "ai", "xn--80ao21a", true}},
		// Завершающая точка после punycode убирается до конвейера.
		{"xn--e1afmkfd.xn--p1ai.", Result{"xn--e1afmkfd.xn--p1ai", "пример.рф", "xn--p1ai", "xn--e1afmkfd", true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertNormalize(t, tc.name, tc.want)
		})
	}
}

// TestNormalizePunycodeBroken — невалидные xn-- (ТЗ 5.2 п. 3): метка не
// декодируется или декодируется в недопустимое. Разбор причин — по декоду
// golang.org/x/net/idna (validateFromPunycode). Вырожденный xn-- (декод в
// пустоту → «пустая метка») закреплён в idn_test.go (TestNormalizeInvalid).
func TestNormalizePunycodeBroken(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind ErrorKind
	}{
		{"xn--a.com", ErrInvalid},          // декод в контрольный символ U+0080
		{"xn--0.com", ErrInvalid},          // декод в чистый ASCII «0» — неканоничная A-метка
		{"xn--ab.com", ErrInvalid},         // декод в контрольные U+0081 U+0080
		{"xn---p1ai.com", ErrInvalid},      // декод «-p1ai» с ведущим дефисом
		{"xn--zzzzzz.com", ErrInvalid},     // декод в ASCII «zzzzzz»: метка должна была быть без xn--
		{"xn--bcher--kva.com", ErrInvalid}, // декод «bchêer-» с хвостовым дефисом
		{"XN--BCHER--KVA.com", ErrInvalid}, // регистр A-метки не спасает битый punycode
		{"xn--p1ai-", ErrInvalid},          // хвостовой дефис после декодирования
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertNormalizeError(t, tc.name, tc.kind)
		})
	}
}

// TestNormalizeLengthPunycode — граница длины для unicode-метки: punycode
// удлиняет метку, поэтому 63 октета заканчиваются раньше, чем 63 руны
// (р×57 → метка ровно 63 октета, р×58 → 64). Punycode детерминирован
// (RFC 3492), р×57 стабильно даёт xn--p1aaa…a длиной 63. Границы в
// ASCII-форме (метка 63/64, имя 253/254) — TestNormalizeValid,
// TestNormalizeInvalid и TestNormalizeBoundaryLengths в idn_test.go.
func TestNormalizeLengthPunycode(t *testing.T) {
	ok := strings.Repeat("р", 57) + ".com"
	got, err := Normalize(ok)
	if err != nil {
		t.Fatalf("Normalize(р×57): %v, want без ошибки (метка ровно 63 октета)", err)
	}
	if !strings.HasPrefix(got.Label, xnPrefix) {
		t.Errorf("метка р×57 = %q, want punycode-метку (префикс %s)", got.Label, xnPrefix)
	}
	if len(got.Label) != maxLabelLen {
		t.Errorf("метка р×57: %d октет, want ровно %d", len(got.Label), maxLabelLen)
	}
	assertNormalizeError(t, strings.Repeat("р", 58)+".com", ErrLabelLen)
}

// TestNormalizeZonesPSL — отделение зоны по PSL (ТЗ 5.2 п. 5): составные
// ICANN-суффиксы, частные суффиксы, сами суффиксы без метки, IP-литералы.
func TestNormalizeZonesPSL(t *testing.T) {
	for _, tc := range []struct {
		name string
		want Result
	}{
		{"example.com.au", Result{"example.com.au", "example.com.au", "com.au", "example", false}},
		{"a.b.example.com.au", Result{"a.b.example.com.au", "a.b.example.com.au", "com.au", "a.b.example", false}},
		{"foo.appspot.com", Result{"foo.appspot.com", "foo.appspot.com", "appspot.com", "foo", false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertNormalize(t, tc.name, tc.want)
		})
	}
	for _, tc := range []struct {
		name string
		kind ErrorKind
	}{
		{"com.au", ErrNoLabel},      // сам ICANN-суффикс
		{"appspot.com", ErrNoLabel}, // сам частный суффикс PSL
		// IP-литерал: publicsuffix распознаёт адрес (netip.ParseAddr) и
		// возвращает его целиком как «суффикс» — числовая зона, таких TLD
		// не существует. Для incoming-строк это правильно: IP — не домен
		// зоны. Хосты из Location — отдельно, HostToASCII пропускает IP
		// (ТЗ 5.2 п. 8).
		{"192.0.2.1", ErrNumericZone}, // IPv4-литерал
		{"8.8.8.8", ErrNumericZone},   // IPv4-литерал
		// Числовые «зоны» — не домены: TLD из цифр не существует (IANA),
		// а правило «*» PSL отдаёт неизвестный числовый TLD как есть.
		// Метки вне диапазона IPv4 — даже не адрес (netip.ParseAddr),
		// но и не имя: числовая зона отклоняется.
		{"999.888.777.666", ErrNumericZone},
		{"18.06.1999", ErrNumericZone}, // дата выгрузки регистратора
		{"москва", ErrNoLabel},         // сам публичный суффикс — TLD москва
		{"xn--80adxhks", ErrNoLabel},   // TLD москва в punycode
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertNormalizeError(t, tc.name, tc.kind)
		})
	}
}

// TestNormalizeEdges — края входа: пустые, точки, пробелы с точками.
func TestNormalizeEdges(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind ErrorKind
	}{
		{"...", ErrEmpty},            // только завершающие точки
		{" . ", ErrEmpty},            // пробелы и точка
		{".example.com", ErrInvalid}, // ведущая точка — пустая метка
		// Пробел внутри убирается до конвейера → example..com → пустая метка.
		{"example. .com", ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertNormalizeError(t, tc.name, tc.kind)
		})
	}
}

// TestNormalizeZoneUnicode — unicode-имена каталогов зон (ТЗ 5.2 п. 6, ТЗ
// 11.1): нормализуются тем же конвейером, без требования принадлежности PSL.
func TestNormalizeZoneUnicode(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		{"РФ.", "xn--p1ai"},                // регистр и завершающая точка
		{"москва.", "xn--80adxhks"},        // TLD москва
		{" XN--80ADXHKS ", "xn--80adxhks"}, // пробелы и uppercase punycode
		{"日本.jp", "xn--wgv71a.jp"},         // метка + зона каталогом
		{"日本。jp", "xn--wgv71a.jp"},         // идеографическая точка U+3002 — разделитель меток
		{"сайт.рф", "xn--80aswg.xn--p1ai"}, // составное имя каталога: зона не обязана быть TLD
	} {
		t.Run(tc.in, func(t *testing.T) {
			got, err := NormalizeZone(tc.in)
			if err != nil {
				t.Fatalf("NormalizeZone(%q) = %v, want %q", tc.in, err, tc.want)
			}
			if got != tc.want {
				t.Errorf("NormalizeZone(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
	for _, in := range []string{"xn--p1ai-", "рф!!"} {
		t.Run(in, func(t *testing.T) {
			if _, err := NormalizeZone(in); err == nil {
				t.Errorf("NormalizeZone(%q): нет ошибки, want ошибка", in)
			}
		})
	}
}

// TestHostToASCIICorpus — хосты из Location/meta refresh (ТЗ 5.2 п. 8):
// unicode → punycode, IP-литералы, пустые, IPv6 — не имена.
func TestHostToASCIICorpus(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		// Основной сценарий ТЗ 5.2 п. 8: unicode-хост из Location/meta
		// refresh → punycode для DNS-запроса, Host и SNI.
		{"пример.рф", "xn--e1afmkfd.xn--p1ai"},             // unicode-метка и зона
		{"Straße.de", "xn--strae-oqa.de"},                  // deviation ß — как в Normalize
		{"255.255.255.255", "255.255.255.255"},             // IPv4-литерал
		{"0.0.0.0", "0.0.0.0"},                             // IPv4-литерал
		{"1.2.3.4.5", "1.2.3.4.5"},                         // пять меток — уже не IP, но валидное имя; зону HostToASCII не отделяет
		{"xn--e1afmkfd.xn--p1ai", "xn--e1afmkfd.xn--p1ai"}, // готовый punycode
	} {
		t.Run(tc.in, func(t *testing.T) {
			got, err := HostToASCII(tc.in)
			if err != nil {
				t.Fatalf("HostToASCII(%q) = %v, want %q", tc.in, err, tc.want)
			}
			if got != tc.want {
				t.Errorf("HostToASCII(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
	for _, tc := range []struct {
		in   string
		kind ErrorKind
	}{
		{"", ErrEmpty},   // пустой хост
		{"  ", ErrEmpty}, // только пробелы
	} {
		t.Run(tc.in, func(t *testing.T) {
			_, err := HostToASCII(tc.in)
			if err == nil {
				t.Fatalf("HostToASCII(%q): нет ошибки, want %s", tc.in, tc.kind)
			}
			if kind, ok := KindOf(err); !ok || kind != tc.kind {
				t.Errorf("HostToASCII(%q): kind = %s (%v), want %s", tc.in, kind, err, tc.kind)
			}
		})
	}
}

// TestToUnicodeCorpus — производная Unicode-форма (ТЗ 5.2 п. 7): emoji,
// deviation-символы и гомоглоны декодируются для отображения.
func TestToUnicodeCorpus(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		{"xn--e28h.com", "😀.com"},              // эмодзи
		{"xn--strae-oqa.de", "straße.de"},      // deviation ß сохраняется
		{"xn--sxahok1c.xn--oxa2a", "πόλης.γρ"}, // финальная сигма ς (U+03C2) переживает punycode
		{"xn--80ak6aa92e.com", "аррӏе.com"},    // кириллический гомоглон apple.com
	} {
		t.Run(tc.in, func(t *testing.T) {
			if got := ToUnicode(tc.in); got != tc.want {
				t.Errorf("ToUnicode(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

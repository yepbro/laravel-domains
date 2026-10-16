// Юнит-тесты валидации строк ingest (T-016): принадлежность зоне пути,
// типизированные причины отклонения, лог rejected (формат закреплён),
// счётчики и порог доли ошибок на файл.
package ingest

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// newValidator — валидатор одного файла с логом rejected в буфер
// (проверка записей) и заданным порогом доли ошибок.
func newValidator(t *testing.T, zone string, ratio float64) (*Validator, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	v, err := NewValidator(Options{
		File:          "incoming/example.com/add/list.txt",
		Zone:          zone,
		MaxErrorRatio: ratio,
		RejectLog:     NewRejectedLogger(&buf, slog.LevelInfo),
	})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	return v, &buf
}

func TestLineValid(t *testing.T) {
	tests := []struct {
		name     string
		zone     string
		line     string
		wantZone string
		label    string
		isIDN    bool
	}{
		// Домен из зоны пути; помарки ввода прощаются нормализацией
		// (ТЗ 5.2 п. 1: регистр, пробелы, завершающая точка).
		{"простой", "com", "example.com", "com", "example", false},
		{"регистр и точка", "com", "  EXAMPLE.Com. ", "com", "example", false},
		// IDN: строка в unicode, зона пути в punycode — и наоборот.
		{"unicode строка в punycode зоне", "xn--p1ai", "пример.рф", "xn--p1ai", "xn--e1afmkfd", true},
		{"punycode строка в punycode зоне", "xn--p1ai", "xn--e1afmkfd.xn--p1ai", "xn--p1ai", "xn--e1afmkfd", true},
		{"unicode зона каталога", "РФ.", "пример.рф", "xn--p1ai", "xn--e1afmkfd", true},
		// бит is_idn — по метке домена, а не по зоне (ТЗ 5.2 п. 9).
		{"ascii метка в idn зоне", "xn--p1ai", "example.xn--p1ai", "xn--p1ai", "example", false},
		// Составная зона по publicsuffix (ТЗ 5.2 п. 5).
		{"составная зона", "co.uk", "example.co.uk", "co.uk", "example", false},
		{"составная метка", "co.uk", "магазин.рф.co.uk", "co.uk", "xn--80aairftm.xn--p1ai", true},
		// Зона пути глубже публичного суффикса — тестовые зоны (T-014):
		// у example.crawl.test суффикс по списку test, зона пути crawl.test.
		{"зона вне списка суффиксов", "crawl.test", "example.crawl.test", "crawl.test", "example", false},
		{"составная метка вне списка", "crawl.test", "sub.example.crawl.test", "crawl.test", "sub.example", false},
		{"последняя метка как зона", "test", "crawl.test", "test", "crawl", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v, _ := newValidator(t, tc.zone, 0)
			entry, reason, ok := v.Line(1, tc.line)
			if !ok {
				t.Fatalf("Line(%q) отклонена: %s", tc.line, reason)
			}
			if reason != "" {
				t.Errorf("причина валидной строки: %q", reason)
			}
			if entry.Zone != tc.wantZone {
				t.Errorf("Zone = %q, want %q", entry.Zone, tc.wantZone)
			}
			if entry.Label != tc.label {
				t.Errorf("Label = %q, want %q", entry.Label, tc.label)
			}
			if entry.IsIDN != tc.isIDN {
				t.Errorf("IsIDN = %v, want %v", entry.IsIDN, tc.isIDN)
			}
		})
	}
}

func TestLineRejected(t *testing.T) {
	longName := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." +
		strings.Repeat("c", 63) + "." + strings.Repeat("d", 62) // 254 октета
	tests := []struct {
		name   string
		zone   string
		line   string
		reason Reason
	}{
		// Домен из другой зоны (ТЗ 5.2 п. 6).
		{"другая зона", "com", "example.org", ReasonWrongZone},
		{"субдомен другой зоны", "com", "sub.example.org", ReasonWrongZone},
		{"валидный idn не из зоны", "com", "пример.рф", ReasonWrongZone},
		// Вложенная публичная зона: example.co.uk принадлежит co.uk,
		// а не uk — простое разбиение по точке запрещено (п. 5).
		{"вложенная публичная зона", "uk", "example.co.uk", ReasonWrongZone},
		{"частный суффикс глубже", "io", "foo.github.io", ReasonWrongZone},
		// Классы idn.Error (ТЗ 5.2).
		{"пустая строка", "com", "", ReasonEmpty},
		{"только пробелы", "com", "   ", ReasonEmpty},
		{"только точка", "com", ".", ReasonEmpty},
		{"мусор", "com", "под_чёркивание.com", ReasonInvalid},
		{"битый punycode", "com", "xn--bcher--kva.com", ReasonInvalid},
		{"пустая метка", "com", "example..com", ReasonInvalid},
		{"метка длиннее 63", "com", strings.Repeat("a", 64) + ".com", ReasonLabelLen},
		{"имя длиннее 253", "com", longName, ReasonNameLen},
		// Метки перед зоной нет: имя — сам публичный суффикс.
		{"публичный суффикс", "com", "com", ReasonNoLabel},
		{"составной публичный суффикс", "com", "co.uk", ReasonNoLabel},
		// Зона пути вне списка суффиксов: имя совпадает с зоной.
		{"имя равно зоне пути", "crawl.test", "crawl.test", ReasonNoLabel},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v, _ := newValidator(t, tc.zone, 0)
			entry, reason, ok := v.Line(7, tc.line)
			if ok {
				t.Fatalf("Line(%q) принята: %+v", tc.line, entry)
			}
			if reason != tc.reason {
				t.Errorf("причина = %s, want %s", reason, tc.reason)
			}
			if entry != (Entry{}) {
				t.Errorf("отклонённая строка вернула запись: %+v", entry)
			}
		})
	}
}

// Формат лога rejected: текстовый slog, атрибут service=rejected, запись
// с атрибутами файл/номер строки/обрезанная строка/причина (ТЗ 5.1).
func TestRejectedLogFormat(t *testing.T) {
	v, buf := newValidator(t, "com", 0)
	if _, _, ok := v.Line(3, "example.org"); ok {
		t.Fatal("example.org в зоне com должна быть отклонена")
	}
	out := buf.String()
	for _, want := range []string{
		`msg="строка отклонена"`,
		`service=rejected`,
		`file=incoming/example.com/add/list.txt`,
		`line=3`,
		`text=example.org`,
		`reason=wrong_zone`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("запись лога rejected не содержит %s:\n%s", want, out)
		}
	}
}

// Длинный мусор обрезается до 255 рун с многоточием (вход недоверенный).
func TestRejectedLogTruncates(t *testing.T) {
	v, buf := newValidator(t, "com", 0)
	v.Line(1, strings.Repeat("ё", 300)+".com") // невалидная метка любой длины
	want := "text=" + strings.Repeat("ё", maxLoggedRunes) + "…"
	if !strings.Contains(buf.String(), want) {
		t.Errorf("строка в логе не обрезана до %d рун", maxLoggedRunes)
	}
}

// Порог доли ошибок: доля, равная порогу, файл не роняет; превышение —
// файл ошибочен целиком (ТЗ 5.1; сигнал failed/ для T-021).
func TestErrorRatioBoundary(t *testing.T) {
	run := func(t *testing.T, ratio float64, bad, good int) FileResult {
		t.Helper()
		v, _ := newValidator(t, "com", ratio)
		for i := 0; i < good; i++ {
			v.Line(i+1, "example"+string(rune('a'+i%26))+".com")
		}
		for i := 0; i < bad; i++ {
			v.Line(good+i+1, "example.org") // не из зоны
		}
		return v.Finish()
	}
	t.Run("доля равна порогу", func(t *testing.T) {
		if res := run(t, 0.5, 2, 2); res.Failed {
			t.Errorf("2 из 4 при пороге 0.5: файл не должен быть ошибочным: %+v", res)
		}
	})
	t.Run("доля выше порога", func(t *testing.T) {
		if res := run(t, 0.5, 3, 1); !res.Failed {
			t.Errorf("3 из 4 при пороге 0.5: файл должен быть ошибочным: %+v", res)
		}
	})
	t.Run("порог по умолчанию 10 процентов", func(t *testing.T) {
		if res := run(t, 0, 1, 9); res.Failed {
			t.Errorf("1 из 10 при пороге 0.1: файл не должен быть ошибочным: %+v", res)
		}
		if res := run(t, 0, 2, 8); !res.Failed {
			t.Errorf("2 из 10 при пороге 0.1: файл должен быть ошибочным: %+v", res)
		}
	})
	t.Run("файл без строк", func(t *testing.T) {
		v, _ := newValidator(t, "com", 0.5)
		if res := v.Finish(); res.Failed || res.Total != 0 {
			t.Errorf("пустой файл не ошибочен, получено %+v", res)
		}
	})
}

// Счётчики файла: всего/принято/отклонено и распределение причин
// (отчёт ingest_files, ТЗ 5.1, T-021).
func TestFinishStats(t *testing.T) {
	v, _ := newValidator(t, "com", 0)
	lines := []string{"example.com", "example.org", "", "example..com", "shop.com"}
	for i, line := range lines {
		v.Line(i+1, line)
	}
	res := v.Finish()
	if res.Total != 5 || res.Accepted != 2 || res.Rejected != 3 {
		t.Errorf("счётчики: total=%d accepted=%d rejected=%d, want 5/2/3",
			res.Total, res.Accepted, res.Rejected)
	}
	wantReasons := map[Reason]int{ReasonWrongZone: 1, ReasonEmpty: 1, ReasonInvalid: 1}
	if len(res.Reasons) != len(wantReasons) {
		t.Fatalf("причины %+v, want %+v", res.Reasons, wantReasons)
	}
	for r, n := range wantReasons {
		if res.Reasons[r] != n {
			t.Errorf("причина %s: %d, want %d", r, res.Reasons[r], n)
		}
	}
}

func TestNewValidator(t *testing.T) {
	t.Run("зона каталога нормализуется", func(t *testing.T) {
		v, err := NewValidator(Options{Zone: "РФ"})
		if err != nil {
			t.Fatal(err)
		}
		if v.zone != "xn--p1ai" {
			t.Errorf("зона = %q, want xn--p1ai", v.zone)
		}
	})
	t.Run("кривая зона каталога", func(t *testing.T) {
		if _, err := NewValidator(Options{Zone: "под_чёркивание.рф"}); err == nil {
			t.Error("невалидная зона должна давать ошибку")
		}
	})
	t.Run("порог вне диапазона", func(t *testing.T) {
		for _, ratio := range []float64{-0.1, 1.0001} {
			if _, err := NewValidator(Options{Zone: "com", MaxErrorRatio: ratio}); err == nil {
				t.Errorf("порог %v должен отклоняться", ratio)
			}
		}
	})
	t.Run("без логгера", func(t *testing.T) {
		v, err := NewValidator(Options{Zone: "com"})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, ok := v.Line(1, "example.org"); ok {
			t.Fatal("строка не из зоны должна отклоняться и без логгера")
		}
	})
}

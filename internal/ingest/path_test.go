// Юнит-тесты разбора пути входного файла <зона>/add|delete/*.txt (T-014,
// ТЗ 5.1): корректные пути, нормализация имени каталога зоны в unicode и
// punycode в одну зону (ТЗ 11.4 п. 1), типизированные ошибки структуры,
// расширения и невалидной зоны.
package ingest

import (
	"errors"
	"strings"
	"testing"

	"crawler/internal/idn"
	"crawler/internal/model"
)

func TestParsePathValid(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		zone   string       // want Zone (punycode)
		uni    string       // want ZoneUnicode
		action model.Action // want Action
		clean  string       // want Path (после path.Clean)
	}{
		{name: "ascii зона add", path: "com/add/list.txt", zone: "com", uni: "com", action: model.ActionAdd, clean: "com/add/list.txt"},
		{name: "ascii зона delete", path: "net/delete/2026-10-17.txt", zone: "net", uni: "net", action: model.ActionDelete, clean: "net/delete/2026-10-17.txt"},
		{name: "составная зона", path: "co.uk/add/list.txt", zone: "co.uk", uni: "co.uk", action: model.ActionAdd, clean: "co.uk/add/list.txt"},
		{name: "зона punycode", path: "xn--p1ai/add/list.txt", zone: "xn--p1ai", uni: "рф", action: model.ActionAdd, clean: "xn--p1ai/add/list.txt"},
		{name: "зона unicode", path: "рф/delete/list.txt", zone: "xn--p1ai", uni: "рф", action: model.ActionDelete, clean: "рф/delete/list.txt"},
		{name: "регистр зоны и .TXT — зона приводится, файл пропускается", path: "COM/add/LIST.TXT", zone: "com", uni: "com", action: model.ActionAdd, clean: "COM/add/LIST.TXT"},
		{name: "точки в имени файла", path: "com/add/2026.10.17.txt", zone: "com", uni: "com", action: model.ActionAdd, clean: "com/add/2026.10.17.txt"},
		{name: "очистка разделителей", path: "com//add/./list.txt", zone: "com", uni: "com", action: model.ActionAdd, clean: "com/add/list.txt"},
		{name: "хвостовой слэш", path: "com/add/list.txt/", zone: "com", uni: "com", action: model.ActionAdd, clean: "com/add/list.txt"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParsePath(tc.path)
			if err != nil {
				t.Fatalf("ParsePath(%q) = %v, want без ошибки", tc.path, err)
			}
			if got.Zone != tc.zone {
				t.Errorf("Zone = %q, want %q", got.Zone, tc.zone)
			}
			if got.ZoneUnicode != tc.uni {
				t.Errorf("ZoneUnicode = %q, want %q", got.ZoneUnicode, tc.uni)
			}
			if got.Action != tc.action {
				t.Errorf("Action = %d, want %d", got.Action, tc.action)
			}
			if got.Path != tc.clean {
				t.Errorf("Path = %q, want %q (path.Clean)", got.Path, tc.clean)
			}
		})
	}
}

// TestParsePathZoneUnicodePunycode — ядро ТЗ 11.4 п. 1: unicode- и
// punycode-имя каталога — одна зона, без дублей.
func TestParsePathZoneUnicodePunycode(t *testing.T) {
	uni, err := ParsePath("москва/add/новые.txt")
	if err != nil {
		t.Fatalf("unicode-каталог: %v", err)
	}
	puny, err := ParsePath("xn--80adxhks/add/new.txt")
	if err != nil {
		t.Fatalf("punycode-каталог: %v", err)
	}
	const want = "xn--80adxhks"
	if uni.Zone != want || puny.Zone != want {
		t.Errorf("зоны %q и %q, want обе %q (одна зона без дублей)", uni.Zone, puny.Zone, want)
	}
	if uni.ZoneUnicode != "москва" || puny.ZoneUnicode != "москва" {
		t.Errorf("ZoneUnicode = %q и %q, want обе «москва»", uni.ZoneUnicode, puny.ZoneUnicode)
	}
}

func TestParsePathErrors(t *testing.T) {
	tests := []struct {
		name string
		path string
		kind PathErrorKind
	}{
		{name: "пустой путь", path: "", kind: ErrPathStructure},
		{name: "только файл, без зоны", path: "list.txt", kind: ErrPathStructure},
		{name: "нет add/delete", path: "com/list.txt", kind: ErrPathStructure},
		{name: "неизвестный каталог операции", path: "com/patch/list.txt", kind: ErrPathStructure},
		{name: "операция в верхнем регистре", path: "com/Add/list.txt", kind: ErrPathStructure},
		{name: "лишняя вложенность", path: "com/add/sub/list.txt", kind: ErrPathStructure},
		{name: "абсолютный путь", path: "/com/add/list.txt", kind: ErrPathStructure},
		{name: "выход за incoming", path: "../com/add/list.txt", kind: ErrPathStructure},
		{name: "глубокий выход", path: "com/../../add/list.txt", kind: ErrPathStructure},
		{name: "каталог операции вместо файла", path: "com/add/", kind: ErrPathStructure},
		{name: "без расширения", path: "com/add/list", kind: ErrPathNotTXT},
		{name: "другое расширение", path: "com/add/list.csv", kind: ErrPathNotTXT},
		{name: "временное имя", path: "com/add/list.txt.part", kind: ErrPathNotTXT},
		{name: "битая punycode-метка зоны", path: "xn--/add/list.txt", kind: ErrPathZone},
		{name: "запрещённый символ в зоне", path: "не%зона/add/list.txt", kind: ErrPathZone},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, err := ParsePath(tc.path)
			if err == nil {
				t.Fatalf("ParsePath(%q) = %+v, want ошибка %s", tc.path, f, tc.kind)
			}
			var pe *PathError
			if !errors.As(err, &pe) {
				t.Fatalf("ошибка %v не *PathError", err)
			}
			if pe.Kind != tc.kind {
				t.Errorf("Kind = %s, want %s", pe.Kind, tc.kind)
			}
			if pe.Path != tc.path {
				t.Errorf("Path = %q, want исходный %q", pe.Path, tc.path)
			}
			if kind, ok := PathKindOf(err); !ok || kind != tc.kind {
				t.Errorf("PathKindOf = (%s, %v), want (%s, true)", kind, ok, tc.kind)
			}
		})
	}
}

// TestParsePathZoneErrorUnwrap — причина ошибки зоны доступна как *idn.Error
// (класс — в отчёт об ошибке файла, ТЗ 5.1).
func TestParsePathZoneErrorUnwrap(t *testing.T) {
	_, err := ParsePath("xn--/add/list.txt")
	if err == nil {
		t.Fatal("ожидалась ошибка нормализации зоны")
	}
	var ie *idn.Error
	if !errors.As(err, &ie) {
		t.Fatalf("причина недоступна как *idn.Error: %v", err)
	}
	if ie.Kind != idn.ErrInvalid {
		t.Errorf("класс причины = %s, want %s", ie.Kind, idn.ErrInvalid)
	}
}

func TestPathErrorMessages(t *testing.T) {
	_, err := ParsePath("com/patch/list.txt")
	want := `ingest: структура пути не <зона>/add|delete/*.txt: "com/patch/list.txt"`
	if err == nil || err.Error() != want {
		t.Errorf("Error() = %v, want %s", err, want)
	}

	_, err = ParsePath("xn--/add/list.txt")
	if err == nil || !strings.HasPrefix(err.Error(), `ingest: невалидное имя каталога зоны: "xn--/add/list.txt": `) {
		t.Errorf("Error() = %v, want причина нормализации в конце", err)
	}
}

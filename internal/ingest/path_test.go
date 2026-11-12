// Unit tests of parsing the input file path <zone>/add|delete/*.txt (T-014,
// spec 5.1): correct paths, normalization of the zone directory name in
// unicode and punycode into one zone (spec 11.4 item 1), typed errors of
// structure, extension and invalid zone.
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
		clean  string       // want Path (after path.Clean)
	}{
		{name: "an ASCII zone add", path: "com/add/list.txt", zone: "com", uni: "com", action: model.ActionAdd, clean: "com/add/list.txt"},
		{name: "an ASCII zone delete", path: "net/delete/2026-10-17.txt", zone: "net", uni: "net", action: model.ActionDelete, clean: "net/delete/2026-10-17.txt"},
		{name: "a composite zone", path: "co.uk/add/list.txt", zone: "co.uk", uni: "co.uk", action: model.ActionAdd, clean: "co.uk/add/list.txt"},
		{name: "a punycode zone", path: "xn--p1ai/add/list.txt", zone: "xn--p1ai", uni: "рф", action: model.ActionAdd, clean: "xn--p1ai/add/list.txt"},
		{name: "a unicode zone", path: "рф/delete/list.txt", zone: "xn--p1ai", uni: "рф", action: model.ActionDelete, clean: "рф/delete/list.txt"},
		{name: "zone case and .TXT — the zone is normalized, the file is skipped", path: "COM/add/LIST.TXT", zone: "com", uni: "com", action: model.ActionAdd, clean: "COM/add/LIST.TXT"},
		{name: "dots in the file name", path: "com/add/2026.10.17.txt", zone: "com", uni: "com", action: model.ActionAdd, clean: "com/add/2026.10.17.txt"},
		{name: "separator cleanup", path: "com//add/./list.txt", zone: "com", uni: "com", action: model.ActionAdd, clean: "com/add/list.txt"},
		{name: "a trailing slash", path: "com/add/list.txt/", zone: "com", uni: "com", action: model.ActionAdd, clean: "com/add/list.txt"},
		// Archives of supported formats (T-133) — names like txt.
		{name: "zip", path: "com/add/zone.zip", zone: "com", uni: "com", action: model.ActionAdd, clean: "com/add/zone.zip"},
		{name: "tar", path: "net/delete/plain.tar", zone: "net", uni: "net", action: model.ActionDelete, clean: "net/delete/plain.tar"},
		{name: "tar.gz", path: "com/add/wrapped.tar.gz", zone: "com", uni: "com", action: model.ActionAdd, clean: "com/add/wrapped.tar.gz"},
		{name: "tgz", path: "com/add/short.tgz", zone: "com", uni: "com", action: model.ActionAdd, clean: "com/add/short.tgz"},
		{name: "gz", path: "com/add/list.gz", zone: "com", uni: "com", action: model.ActionAdd, clean: "com/add/list.gz"},
		{name: "an uppercase archive", path: "com/add/ZONE.ZIP", zone: "com", uni: "com", action: model.ActionAdd, clean: "com/add/ZONE.ZIP"},
		// Zone-less files (add|delete/<file>): line zones are derived from
		// the names (validate.go), the file may mix zones.
		{name: "a zone-less add", path: "add/list.txt", zone: "", uni: "", action: model.ActionAdd, clean: "add/list.txt"},
		{name: "a zone-less delete", path: "delete/old.txt", zone: "", uni: "", action: model.ActionDelete, clean: "delete/old.txt"},
		{name: "a zone-less .TXT", path: "add/LIST.TXT", zone: "", uni: "", action: model.ActionAdd, clean: "add/LIST.TXT"},
		{name: "a zone-less archive", path: "add/zone.zip", zone: "", uni: "", action: model.ActionAdd, clean: "add/zone.zip"},
		{name: "a zone-less separator cleanup", path: "add//./list.txt", zone: "", uni: "", action: model.ActionAdd, clean: "add/list.txt"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParsePath(tc.path)
			if err != nil {
				t.Fatalf("ParsePath(%q) = %v, want no error", tc.path, err)
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

// TestParsePathZoneUnicodePunycode — the core of spec 11.4 item 1: the
// unicode and punycode directory names are one zone, without duplicates.
func TestParsePathZoneUnicodePunycode(t *testing.T) {
	uni, err := ParsePath("москва/add/новые.txt")
	if err != nil {
		t.Fatalf("the unicode directory: %v", err)
	}
	puny, err := ParsePath("xn--80adxhks/add/new.txt")
	if err != nil {
		t.Fatalf("the punycode directory: %v", err)
	}
	const want = "xn--80adxhks"
	if uni.Zone != want || puny.Zone != want {
		t.Errorf("the zones %q and %q, want both %q (one zone, no duplicates)", uni.Zone, puny.Zone, want)
	}
	if uni.ZoneUnicode != "москва" || puny.ZoneUnicode != "москва" {
		t.Errorf("ZoneUnicode = %q and %q, want both «москва»", uni.ZoneUnicode, puny.ZoneUnicode)
	}
}

func TestParsePathErrors(t *testing.T) {
	tests := []struct {
		name string
		path string
		kind PathErrorKind
	}{
		{name: "an empty path", path: "", kind: ErrPathStructure},
		{name: "a file only, no zone", path: "list.txt", kind: ErrPathStructure},
		{name: "no add/delete", path: "com/list.txt", kind: ErrPathStructure},
		{name: "zone-less without add/delete", path: "patch/list.txt", kind: ErrPathStructure},
		{name: "a zone-less operation in uppercase", path: "Add/list.txt", kind: ErrPathStructure},
		{name: "zone-less extra nesting", path: "add/sub/list.txt", kind: ErrPathStructure},
		{name: "a zone-less operation directory instead of a file", path: "add/", kind: ErrPathStructure},
		{name: "zone-less without an extension", path: "add/list", kind: ErrPathNotTXT},
		{name: "zone-less another extension", path: "add/list.csv", kind: ErrPathNotTXT},
		{name: "an unknown operation directory", path: "com/patch/list.txt", kind: ErrPathStructure},
		{name: "an operation in uppercase", path: "com/Add/list.txt", kind: ErrPathStructure},
		{name: "extra nesting", path: "com/add/sub/list.txt", kind: ErrPathStructure},
		{name: "an absolute path", path: "/com/add/list.txt", kind: ErrPathStructure},
		{name: "escaping beyond incoming", path: "../com/add/list.txt", kind: ErrPathStructure},
		{name: "a deep escape", path: "com/../../add/list.txt", kind: ErrPathStructure},
		{name: "an operation directory instead of a file", path: "com/add/", kind: ErrPathStructure},
		{name: "without an extension", path: "com/add/list", kind: ErrPathNotTXT},
		{name: "another extension", path: "com/add/list.csv", kind: ErrPathNotTXT},
		{name: "a temporary name", path: "com/add/list.txt.part", kind: ErrPathNotTXT},
		{name: "an unsupported rar archive", path: "com/add/zone.rar", kind: ErrPathNotTXT},
		{name: "an unsupported 7z archive", path: "com/add/zone.7z", kind: ErrPathNotTXT},
		{name: "an archive under a temporary name", path: "com/add/zone.zip.part", kind: ErrPathNotTXT},
		{name: "a broken punycode zone label", path: "xn--/add/list.txt", kind: ErrPathZone},
		{name: "a forbidden character in the zone", path: "не%зона/add/list.txt", kind: ErrPathZone},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, err := ParsePath(tc.path)
			if err == nil {
				t.Fatalf("ParsePath(%q) = %+v, want the error %s", tc.path, f, tc.kind)
			}
			var pe *PathError
			if !errors.As(err, &pe) {
				t.Fatalf("the error %v is not a *PathError", err)
			}
			if pe.Kind != tc.kind {
				t.Errorf("Kind = %s, want %s", pe.Kind, tc.kind)
			}
			if pe.Path != tc.path {
				t.Errorf("Path = %q, want the original %q", pe.Path, tc.path)
			}
			if kind, ok := PathKindOf(err); !ok || kind != tc.kind {
				t.Errorf("PathKindOf = (%s, %v), want (%s, true)", kind, ok, tc.kind)
			}
		})
	}
}

// TestParsePathZoneErrorUnwrap — the zone error cause is available as
// *idn.Error (the class goes into the file error report, spec 5.1).
func TestParsePathZoneErrorUnwrap(t *testing.T) {
	_, err := ParsePath("xn--/add/list.txt")
	if err == nil {
		t.Fatal("a zone normalization error is expected")
	}
	var ie *idn.Error
	if !errors.As(err, &ie) {
		t.Fatalf("the cause is not available as *idn.Error: %v", err)
	}
	if ie.Kind != idn.ErrInvalid {
		t.Errorf("the cause kind = %s, want %s", ie.Kind, idn.ErrInvalid)
	}
}

func TestPathErrorMessages(t *testing.T) {
	_, err := ParsePath("com/patch/list.txt")
	want := `ingest: the path structure is not <zone>/add|delete/<file> and not add|delete/<file>: "com/patch/list.txt"`
	if err == nil || err.Error() != want {
		t.Errorf("Error() = %v, want %s", err, want)
	}

	_, err = ParsePath("xn--/add/list.txt")
	if err == nil || !strings.HasPrefix(err.Error(), `ingest: an invalid zone directory name: "xn--/add/list.txt": `) {
		t.Errorf("Error() = %v, want the normalization cause at the end", err)
	}
}

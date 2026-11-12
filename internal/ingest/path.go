// Package ingest — ingestion of domain files from the watched incoming
// directory (spec 5.1 of the main spec).
//
// The ingest pipeline (path parsing → line parsing → row validation →
// load and apply) is assembled by tasks T-014…T-021; T-014 introduces
// the first stages:
//
//   - ParsePath (path.go): parses the path of a ready file relative to
//     incoming under the <zone>/add|delete/<file> scheme (*.txt or an
//     archive of a supported format — T-133) — the normalized zone and
//     the file action; a zone directory name in unicode or punycode
//     normalizes into one zone, so no zone duplicates in two forms arise
//     (spec 5.1, 11.4 item 1). The second scheme is add|delete/<file>
//     without a zone directory: a zone-less file may mix zones, and the
//     zone of each line is derived from the name itself (internal/idn:
//     the TLD is the last label, validate.go);
//   - EnsureZone (zones.go): the zone is looked up by punycode name, a
//     missing one is created automatically on the first file with
//     is_active = true (spec 5.1).
//
// Ready files are delivered by the internal/watch watcher (T-013) as
// absolute paths; ingest works with the path relative to incoming
// ("/" separator, as in filepath.Rel) — the ingest pipeline assembly
// computes it, and the same path is written to the ingest_files log
// (T-021).
package ingest

import (
	"errors"
	"fmt"
	"path"
	"strings"

	"crawler/internal/idn"
	"crawler/internal/model"
)

// PathErrorKind — a class of input file path parsing errors: a stable
// reason for the file error report (failed/, spec 5.1 — T-021).
type PathErrorKind string

const (
	// ErrPathStructure — the path is neither <zone>/add|delete/<file> nor
	// add|delete/<file>: no add|delete directory (or it is not lowercase),
	// extra nesting, an absolute path, or an escape beyond incoming.
	ErrPathStructure PathErrorKind = "structure"
	// ErrPathNotTXT — the file name is not *.txt and not an archive of a
	// supported format (.zip/.tar/.tar.gz/.tgz/.gz — T-133): per spec 5.1,
	// file readiness is the appearance of a tracked name (temporary
	// *.tmp/*.part files are not delivered by the watcher).
	ErrPathNotTXT PathErrorKind = "not_txt"
	// ErrPathZone — the zone directory name does not normalize (spec 5.2
	// item 6); the cause is *idn.Error, available via errors.As/Unwrap.
	ErrPathZone PathErrorKind = "zone"
)

// pathKindDescriptions — class texts for Error() and String().
var pathKindDescriptions = map[PathErrorKind]string{
	ErrPathStructure: "the path structure is not <zone>/add|delete/<file> and not add|delete/<file>",
	ErrPathNotTXT:    "the file name is not *.txt and not an archive of a supported format",
	ErrPathZone:      "an invalid zone directory name",
}

// String returns the class text (the idn.ErrorKind.String style).
func (k PathErrorKind) String() string {
	if d, ok := pathKindDescriptions[k]; ok {
		return d
	}
	return string(k)
}

// PathError — a typed path parsing error: the class, the original path
// and the cause (for ErrPathZone — *idn.Error).
type PathError struct {
	Kind PathErrorKind // class — the file failure reason
	Path string        // the original relative path
	Err  error         // the cause; nil for structure errors
}

func (e *PathError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("ingest: %s: %q: %v", e.Kind, e.Path, e.Err)
	}
	return fmt.Sprintf("ingest: %s: %q", e.Kind, e.Path)
}

// Unwrap gives access to the cause (errors.Is/As along the chain — up to *idn.Error).
func (e *PathError) Unwrap() error { return e.Err }

// PathKindOf returns the path parsing error class; ok = false if err is
// not a *PathError (a foreign error).
func PathKindOf(err error) (PathErrorKind, bool) {
	var e *PathError
	if errors.As(err, &e) {
		return e.Kind, true
	}
	return "", false
}

// File — an input file path parsed under the spec 5.1 scheme
// <zone>/add|delete/*.txt or the zone-less add|delete/<file>.
type File struct {
	// Zone — the normalized zone in ASCII form (punycode, spec 5.2 item 6):
	// the zones.name lookup key; the unicode and punycode directory names
	// give one value (spec 11.4 item 1). The empty string means a zone-less
	// file (add|delete/<file> without a zone directory): the zones of the
	// lines are derived from the names themselves (validate.go), the file
	// may mix zones.
	Zone string
	// ZoneUnicode — the unicode form of the zone for display (spec 5.2
	// item 7): the value of zones.name_unicode when the zone is created;
	// for an ASCII zone it equals Zone. Empty for a zone-less file.
	ZoneUnicode string
	// Action — the file action: add — add or update domains, delete —
	// remove them (spec 5.1; model.Action, the ingest_files.action column).
	Action model.Action
	// Path — the path relative to incoming after cleaning (path.Clean: no
	// "."/".." and no extra separators) — written to the ingest_files log
	// (T-021).
	Path string
}

// ParsePath parses the path of a ready file relative to incoming:
// <zone>/add|delete/<file> → the normalized zone and the action (spec 5.1;
// the file is *.txt or an archive of a supported format, T-133); or
// add|delete/<file> → a zone-less file (Zone = "": the zones of the lines
// come from the names, mixed zones in one file are allowed).
//
// Rules:
//
//   - two or exactly three components: [zone directory,] operation
//     directory, file name; extra nesting or a missing add/delete —
//     ErrPathStructure;
//   - the operation directory name is strictly "add" or "delete" in
//     lowercase: the spec 5.1 directory structure is fixed, and the Linux
//     filesystem is case-sensitive — any other name is a different
//     structure, not a spelling variant;
//   - the file name is *.txt or an archive of a supported format (.zip,
//     .tar, .tar.gz, .tgz, .gz — T-133), case-insensitive, mirroring the
//     watcher readiness filter (internal/watch, spec 5.1): everything it
//     delivers is parsed by ingest the same way; otherwise —
//     ErrPathNotTXT;
//   - the zone directory name is normalized by idn.NormalizeZone
//     (multi-level names like crawl.test are syntactically valid —
//     working directories are TLD-only, the gate for multi-level paths is
//     the allowlist in the validation pipeline, the tld-only-zones spec
//     §5.1); a normalization error — ErrPathZone with a *idn.Error cause;
//   - absolute paths and ".." beyond incoming — ErrPathStructure.
func ParsePath(rel string) (File, error) {
	if rel == "" || path.IsAbs(rel) {
		return File{}, &PathError{Kind: ErrPathStructure, Path: rel}
	}
	p := path.Clean(rel)
	if p == "." || p == ".." || strings.HasPrefix(p, "../") {
		return File{}, &PathError{Kind: ErrPathStructure, Path: rel}
	}

	parts := strings.Split(p, "/")
	var zoneDir, opDir, name string
	switch len(parts) {
	case 3: // <zone>/add|delete/<file> — spec 5.1
		zoneDir, opDir, name = parts[0], parts[1], parts[2]
	case 2: // add|delete/<file> — zone-less, line zones come from the names
		opDir, name = parts[0], parts[1]
	default:
		return File{}, &PathError{Kind: ErrPathStructure, Path: rel}
	}

	var action model.Action
	switch opDir {
	case "add":
		action = model.ActionAdd
	case "delete":
		action = model.ActionDelete
	default:
		return File{}, &PathError{Kind: ErrPathStructure, Path: rel}
	}

	// path.Ext returns the tail from the last dot; readiness per spec 5.1
	// is a *.txt name, case does not matter (same as the watcher filter),
	// or an archive of a supported format (.zip, .tar, .tar.gz, .tgz, .gz —
	// T-133): everything the watcher delivers, ingest parses the same way.
	if !fileNameAccepted(name) {
		return File{}, &PathError{Kind: ErrPathNotTXT, Path: rel}
	}

	// A zone-less file — without a zone directory: File.Zone = "", the zones
	// of the lines are derived from the names by the validator (validate.go).
	if zoneDir == "" {
		return File{Action: action, Path: p}, nil
	}

	// The zone is normalized the same way as a domain (spec 5.2 item 6):
	// the unicode directory "москва" and the punycode "xn--80adxhks" are
	// one zone.
	zone, err := idn.NormalizeZone(zoneDir)
	if err != nil {
		return File{}, &PathError{Kind: ErrPathZone, Path: rel, Err: err}
	}
	return File{
		Zone:        zone,
		ZoneUnicode: idn.ToUnicode(zone),
		Action:      action,
		Path:        p,
	}, nil
}

// fileNameAccepted — an input file name: *.txt or an archive of a
// supported format (T-133), case-insensitive — mirrors the watcher
// readiness filter (internal/watch.wantFile): ingest parses everything
// it delivers.
func fileNameAccepted(name string) bool {
	return strings.EqualFold(path.Ext(name), ".txt") || isArchiveName(name)
}

// End-to-end unit tests of archive ingest (T-133; spec 5.1 extension,
// 3.5) without a DB: synthetic archives (archive/zip, archive/tar,
// compress/gzip) in temporary directories, the pipeline environment —
// pipeline_test.go (pipeTest). Scenarios: streaming processing of the
// members of all formats, the zone from the archive directory (not from
// a member path), skipping junk entries with a Warn, the limits (size /
// number of entries), a corrupt archive → failed/, idempotency of
// re-submitting (a duplicate by the archive's sha256), an empty archive,
// a nested archive is not unpacked, an infrastructure failure → the
// archive stays.
package ingest

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"crawler/internal/model"

	"github.com/jackc/pgx/v5/pgconn"
)

// --- synthetic archives -----------------------------------------------------

// archEntry — an entry of a synthetic archive: the name (for directories
// — with a trailing "/") and the content.
type archEntry struct {
	name string
	data string
}

// zipBytes assembles a zip in memory (the entry order is fixed).
func zipBytes(t *testing.T, entries ...archEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, e := range entries {
		w, err := zw.Create(e.name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(e.data)); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// buildZip — a zip file at path (creates the directories itself).
func buildZip(t *testing.T, path string, entries ...archEntry) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, zipBytes(t, entries...), 0o644); err != nil {
		t.Fatal(err)
	}
}

// tarBytes assembles a tar (raw or gzip-wrapped) in memory.
func tarBytes(t *testing.T, gz bool, entries ...archEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	if gz {
		zw := gzip.NewWriter(&buf)
		tw := tar.NewWriter(zw)
		writeTarEntries(t, tw, entries)
		if err := tw.Close(); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}
	tw := tar.NewWriter(&buf)
	writeTarEntries(t, tw, entries)
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func writeTarEntries(t *testing.T, tw *tar.Writer, entries []archEntry) {
	t.Helper()
	for _, e := range entries {
		if strings.HasSuffix(e.name, "/") { // a directory
			if err := tw.WriteHeader(&tar.Header{Name: e.name, Typeflag: tar.TypeDir, Mode: 0o755}); err != nil {
				t.Fatal(err)
			}
			continue
		}
		hdr := &tar.Header{Typeflag: tar.TypeReg, Name: e.name, Mode: 0o644, Size: int64(len(e.data))}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(e.data)); err != nil {
			t.Fatal(err)
		}
	}
}

// buildTar — a tar file (.tar) or gzip-wrapped (.tar.gz/.tgz) at path.
func buildTar(t *testing.T, path string, gz bool, entries ...archEntry) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, tarBytes(t, gz, entries...), 0o644); err != nil {
		t.Fatal(err)
	}
}

// buildGz — a .gz file with the single stream content.
func buildGz(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	fh, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	zw := gzip.NewWriter(fh)
	if _, err := zw.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fh.Close(); err != nil {
		t.Fatal(err)
	}
}

// addExecs — a stub of a successful add application: the insert chunk /
// revive answers go into the pool fields (a Query over the staging
// model, T-205); two Stage cleanups remain as Execs — before loading and
// in the application transaction.
func addExecs(pool *pipePool, n, r int) []pipeExec {
	pool.inserted, pool.revived = int64(n), int64(r)
	return []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")},                      // the cleanup before loading (Stage)
		{tag: pgconn.NewCommandTag(fmt.Sprintf("DELETE %d", n+r+1))}, // the cleanup after the application
	}
}

// newPipe — a pipeline over the test's environment with parameter
// tweaks.
func (pt *pipeTest) newPipe(t *testing.T, mutate func(*PipelineOptions)) *Pipeline {
	t.Helper()
	opts := PipelineOptions{
		Dir:       pt.dir,
		BatchSize: 10,
		Now:       func() time.Time { return pt.now },
	}
	if mutate != nil {
		mutate(&opts)
	}
	pipe, err := NewPipeline(pt.zones, pt.files, pt.pool, opts)
	if err != nil {
		t.Fatal(err)
	}
	return pipe
}

// --- format helpers ---------------------------------------------------------

// TestArchiveKindOf — the format detection by extension (T-133): the
// case does not matter, .tar.gz before .gz, unsupported ones — not an
// archive.
func TestArchiveKindOf(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want archiveKind
	}{
		{"txt", "list.txt", archiveNone},
		{"zip", "zone.zip", archiveZip},
		{"an uppercase zip", "ZONE.ZIP", archiveZip},
		{"tar", "plain.tar", archiveTar},
		{"tar.gz", "wrapped.tar.gz", archiveTarGz},
		{"an uppercase tar.gz", "WRAPPED.TAR.GZ", archiveTarGz},
		{"tgz", "short.tgz", archiveTarGz},
		{"gz", "list.gz", archiveGz},
		{"an uppercase gz", "LIST.GZ", archiveGz},
		{"rar is not supported", "zone.rar", archiveNone},
		{"7z is not supported", "zone.7z", archiveNone},
		{"a full path", "com/add/zone.tar.gz", archiveTarGz},
		{"zip.tmp is not an archive", "zone.zip.tmp", archiveNone},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := archiveKindOf(tc.in); got != tc.want {
				t.Errorf("archiveKindOf(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

// TestGzMemberName — the name of the single member of a .gz: the
// archive without the extension (T-133), the extension case does not
// matter.
func TestGzMemberName(t *testing.T) {
	for in, want := range map[string]string{
		"com/add/zone.gz": "zone",
		"com/add/LIST.GZ": "LIST",
		"net/delete/x.gz": "x",
	} {
		if got := gzMemberName(in); got != want {
			t.Errorf("gzMemberName(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestJunkMember — archive entries without domain lists (T-133): hidden
// files and trees, macOS junk; ordinary paths — members.
func TestJunkMember(t *testing.T) {
	for _, name := range []string{
		".DS_Store", "._one.txt", ".hidden", ".hidden/x.txt", "a/.b/c.txt",
		"__MACOSX/three.txt", "x/__MACOSX/._y",
	} {
		if !junkMember(name) {
			t.Errorf("junkMember(%q) = false, want a skip", name)
		}
	}
	for _, name := range []string{
		"one.txt", "sub/one.txt", "ru/two.txt", "list", "data-2026.txt.gz",
	} {
		if junkMember(name) {
			t.Errorf("junkMember(%q) = true, want it processed", name)
		}
	}
}

// --- scenarios --------------------------------------------------------------

// TestPipelineArchiveZip — a good zip (T-133): the members are read
// as a stream and go through the same pipeline; the zone — from the
// archive directory, the members' internal paths (subdirectories, the
// "foreign" ru/) do not affect the zone; directories, hidden files and
// macOS junk are skipped with a Warn; the accounting unit — the archive:
// one ingest_files row with the sums over the members, the rejected log
// addresses "archive!member:line".
func TestPipelineArchiveZip(t *testing.T) {
	pt := newPipeTest(t)
	entries := []archEntry{
		{"sub/one.txt", "a.com\nb.com\n"},        // a nested path — the zone is not from it
		{"ru/two.txt", "# комментарий\nc.com\n"}, // a "foreign" directory — the zone is com anyway
		{"three.txt", "\nd.com\na.com\n"},        // a duplicate row, an empty row
		// Junk: directories, hidden files, macOS junk — skipped.
		{"data/", ""},
		{"__MACOSX/three.txt", "junk"},
		{"._one.txt", "junk"},
		{".DS_Store", "junk"},
		{".hidden/x.txt", "junk"},
	}
	abs := filepath.Join(pt.dir, "com", "add", "zone.zip")
	buildZip(t, abs, entries...)
	raw, err := os.ReadFile(abs)
	if err != nil {
		t.Fatal(err)
	}
	sha := sha256.Sum256(raw)

	pt.pool.execs = addExecs(pt.pool, 4, 0) // the labels a, b, c, d (a duplicate a.com — one)
	pt.pool.uniques = 4

	var logBuf bytes.Buffer
	pipe := pt.newPipe(t, func(o *PipelineOptions) {
		o.Log = slog.New(slog.NewTextHandler(&logBuf, nil))
	})

	out, err := pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Kind != OutcomeApplied {
		t.Fatalf("the outcome = %s, want applied", out.Kind)
	}

	rep := out.Report
	if rep.Zone != "com" || rep.ZoneID != 1 || rep.Action != model.ActionAdd {
		t.Errorf("the report = %+v, want the zone com (id 1), the action add", rep)
	}
	// 7 physical rows (2+2+3), 2 skipped (a comment, an empty one),
	// 5 valid, 0 rejected: the member path ru/ did not affect the zone.
	if rep.LinesTotal != 7 || rep.Skipped != 2 || rep.Accepted != 5 || rep.Rejected != 0 {
		t.Errorf("the line counters = %+v, want 7/2/5/0", rep)
	}
	if rep.Members != 3 {
		t.Errorf("members = %d, want 3 (junk not counted)", rep.Members)
	}
	if rep.Added != 4 || rep.Existed != 0 {
		t.Errorf("the apply counters = %+v, want added 4, existed 0", rep)
	}
	if string(rep.SHA256) != string(sha[:]) {
		t.Errorf("sha256 = %x, want the hash of the archive bytes %x", rep.SHA256, sha)
	}

	if len(pt.files.records) != 1 {
		t.Fatalf("journal records = %d, want 1 (the accounting unit is the archive)", len(pt.files.records))
	}
	rec := pt.files.records[0]
	if rec.Path != "com/add/zone.zip" || rec.LinesTotal != 7 || rec.Added != 4 || rec.Rejected != 0 {
		t.Errorf("the journal record = %+v, want the sums over the archive members", rec)
	}

	want := []string{
		"exec:" + clearStageSQL,
		"copy:5", // the rows of all the members — one batch
		"begin",
		"query:" + addChunkBoundSQL,
		"query:" + insertAddSQL,
		"query:" + addChunkBoundSQL,
		"exec:" + zoneDeltaAddSQL,
		"query:" + reviveAddSQL,
		"query:" + countAddSQL,
		"exec:" + clearStageSQL,
		"commit",
	}
	if got := strings.Join(pt.pool.ops, "\n"); got != strings.Join(want, "\n") {
		t.Errorf("the call order =\n%s\nwant:\n%s", got, strings.Join(want, "\n"))
	}

	if exists(t, abs) || !exists(t, filepath.Join(pt.dir, DirProcessed, "com/add/zone.zip")) {
		t.Error("the archive must go to processed/com/add/zone.zip")
	}

	// The junk is skipped with a Warn (spec 5.1: the observable part of
	// the behavior).
	log := logBuf.String()
	for _, frag := range []string{
		"archive entry skipped (a directory)",
		"entry=data/",
		"archive entry skipped (hidden or macOS junk)",
		"entry=__MACOSX/three.txt",
		"entry=._one.txt",
		"entry=.DS_Store",
		"entry=.hidden/x.txt",
	} {
		if !strings.Contains(log, frag) {
			t.Errorf("the log does not contain %q:\n%s", frag, log)
		}
	}
}

// TestPipelineArchiveTarGzTgz — tar and gzip-wrapped tars (T-133):
// the double opening (gzip over tar) for the .tar.gz/.tgz names; the
// members go through the same pipeline, directories are skipped.
func TestPipelineArchiveTarGzTgz(t *testing.T) {
	for _, tc := range []struct {
		name string
		file string
		gz   bool
	}{
		{"tar", "net/add/plain.tar", false},
		{"tar.gz", "net/add/wrapped.tar.gz", true},
		{"tgz", "net/add/short.tgz", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pt := newPipeTest(t)
			entries := []archEntry{
				{"dir/", ""},
				{"list", "a.net\nb.net\n"},
			}
			abs := filepath.Join(pt.dir, filepath.FromSlash(tc.file))
			buildTar(t, abs, tc.gz, entries...)

			pt.pool.execs = addExecs(pt.pool, 2, 0)
			pt.pool.uniques = 2

			out, err := pt.pipe.Process(context.Background(), abs)
			if err != nil {
				t.Fatalf("Process: %v", err)
			}
			if out.Kind != OutcomeApplied {
				t.Fatalf("the outcome = %s, want applied", out.Kind)
			}
			rep := out.Report
			if rep.LinesTotal != 2 || rep.Accepted != 2 || rep.Rejected != 0 || rep.Members != 1 {
				t.Errorf("the counters = %+v, want 2 lines, 2 valid, 1 member", rep)
			}
			if len(pt.files.records) != 1 || pt.files.records[0].Path != tc.file {
				t.Errorf("the journal = %+v, want one record %s", pt.files.records, tc.file)
			}
			if !exists(t, filepath.Join(pt.dir, DirProcessed, filepath.FromSlash(tc.file))) {
				t.Errorf("the archive was not moved to processed/%s", tc.file)
			}
		})
	}
}

// TestPipelineArchiveGz — a single .gz (T-133): the only member named
// as the archive without the extension; the rejected log addresses
// "archive!member:line"; isolated junk inside a member does not sink the
// archive (the threshold as for txt).
func TestPipelineArchiveGz(t *testing.T) {
	pt := newPipeTest(t)
	var b strings.Builder
	b.WriteString("a.com\n")
	b.WriteString("%%%%\n") // junk: 1 of 13 < the 0.1 threshold
	for _, d := range []string{"b.com", "c.com", "d.com", "e.com", "f.com", "g.com", "h.com", "i.com", "j.com", "k.com", "l.com"} {
		b.WriteString(d + "\n")
	}
	abs := filepath.Join(pt.dir, "com", "add", "list.gz")
	buildGz(t, abs, b.String())
	raw, err := os.ReadFile(abs)
	if err != nil {
		t.Fatal(err)
	}
	sha := sha256.Sum256(raw)

	pt.pool.execs = addExecs(pt.pool, 12, 0)
	pt.pool.uniques = 12

	var rejBuf strings.Builder
	pipe := pt.newPipe(t, func(o *PipelineOptions) {
		o.RejectLog = NewRejectedLogger(&rejBuf, slog.LevelInfo)
	})

	out, err := pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Kind != OutcomeApplied {
		t.Fatalf("the outcome = %s, want applied", out.Kind)
	}
	rep := out.Report
	if rep.LinesTotal != 13 || rep.Accepted != 12 || rep.Rejected != 1 || rep.Members != 1 {
		t.Errorf("the counters = %+v, want 13 lines, 12 valid, 1 rejected, 1 member", rep)
	}
	if rep.Reasons[ReasonInvalid] != 1 {
		t.Errorf("the reasons = %v, want invalid: 1", rep.Reasons)
	}
	if string(rep.SHA256) != string(sha[:]) {
		t.Errorf("sha256 = %x, want the hash of the archive bytes", rep.SHA256)
	}
	if rep.Added != 12 {
		t.Errorf("added = %d, want 12", rep.Added)
	}
	// The rejection's addressing — "archive!member": the member is the
	// name without the extension.
	logged := rejBuf.String()
	for _, want := range []string{
		"file=com/add/list.gz!list",
		"line=2",
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("the rejected log = %q, want %q", logged, want)
		}
	}
	if !exists(t, filepath.Join(pt.dir, DirProcessed, "com/add/list.gz")) {
		t.Error("the archive was not moved to processed/")
	}
}

// TestPipelineArchiveDelete — an archive in delete/ (T-133): the action
// comes from the archive directory, the delete application over the
// members' rows.
func TestPipelineArchiveDelete(t *testing.T) {
	pt := newPipeTest(t)
	abs := filepath.Join(pt.dir, "net", "delete", "x.tar")
	buildTar(t, abs, false, archEntry{"list", "a.net\n"})

	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")}, // the cleanup before loading
		{tag: pgconn.NewCommandTag("DELETE 1")}, // the cleanup after the application
	}
	pt.pool.delDeleted = 1

	out, err := pt.pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Kind != OutcomeApplied {
		t.Fatalf("the outcome = %s, want applied", out.Kind)
	}
	if rep := out.Report; rep.Action != model.ActionDelete || rep.Deleted != 1 {
		t.Errorf("the delete counters = %+v, want deleted 1", rep)
	}
	if len(pt.files.records) != 1 || pt.files.records[0].Deleted != 1 {
		t.Errorf("the journal = %+v, want deleted 1", pt.files.records)
	}
}

// TestPipelineArchiveMemberErrorRatio — a member over
// ingest.max_error_ratio (T-133): the archive is bad as a whole — no
// application runs, the batch is discarded, failed/ with a log report;
// the reason addresses the member.
func TestPipelineArchiveMemberErrorRatio(t *testing.T) {
	pt := newPipeTest(t)
	entries := []archEntry{
		{"good.txt", "a.com\nb.com\n"},
		{"bad.txt", "x.org\ny.org\nz.org\n"}, // 3/3 wrong_zone > 0.1
	}
	abs := filepath.Join(pt.dir, "com", "add", "bad.zip")
	buildZip(t, abs, entries...)

	out, err := pt.pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v (a bad archive is not a service error)", err)
	}
	if out.Kind != OutcomeFailed {
		t.Fatalf("the outcome = %s, want failed", out.Kind)
	}
	if out.Err == nil || !strings.Contains(out.Err.Error(), "com/add/bad.zip!bad.txt") ||
		!strings.Contains(out.Err.Error(), "the share of rejected lines 3 of 3 is above ingest.max_error_ratio (0.1)") {
		t.Errorf("the reason = %v, want the bad.txt member's rejection share 3 of 3", out.Err)
	}
	if out.Report.Rejected != 3 || out.Report.Accepted != 2 || out.Report.Members != 2 {
		t.Errorf("the counters = %+v, want rejected 3, accepted 2, 2 members", out.Report)
	}
	if len(pt.files.records) != 0 {
		t.Error("the bad archive's report is not written to the journal")
	}
	// The loading happened (the hash is known after reading), the
	// application — no.
	want := []string{
		"exec:" + clearStageSQL,
		"copy:2",
		"exec:" + clearStageSQL,
	}
	if got := strings.Join(pt.pool.ops, "\n"); got != strings.Join(want, "\n") {
		t.Errorf("the calls =\n%s\nwant:\n%s", got, strings.Join(want, "\n"))
	}
	failed := filepath.Join(pt.dir, DirFailed, "com/add/bad.zip")
	if exists(t, abs) || !exists(t, failed) {
		t.Fatalf("the archive must be in %s", failed)
	}
	report := readFile(t, failed+".report")
	for _, frag := range []string{
		"file: com/add/bad.zip",
		"archive members: 2",
		"verdict: bad —",
		"wrong_zone: 3",
	} {
		if !strings.Contains(report, frag) {
			t.Errorf("the log report does not contain %q:\n%s", frag, report)
		}
	}
}

// TestPipelineArchiveBroken — a corrupt archive (T-133): unreadable or
// damaged — entirely into failed/ with a log report, no journal record.
func TestPipelineArchiveBroken(t *testing.T) {
	for _, tc := range []struct {
		name    string
		rel     string
		content func(t *testing.T, path string)
		reason  string
	}{
		{
			name: "zip junk bytes",
			rel:  "com/add/g.zip",
			content: func(t *testing.T, path string) {
				if err := os.WriteFile(path, []byte("это не zip, совсем не zip"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			reason: "unreadable",
		},
		{
			name: "gz junk bytes",
			rel:  "com/add/g.gz",
			content: func(t *testing.T, path string) {
				if err := os.WriteFile(path, []byte("garbage"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			reason: "unreadable",
		},
		{
			name: "an empty gz",
			rel:  "com/add/empty.gz",
			content: func(t *testing.T, path string) {
				if err := os.WriteFile(path, nil, 0o644); err != nil {
					t.Fatal(err)
				}
			},
			reason: "unreadable",
		},
		{
			name: "a truncated tar.gz",
			rel:  "com/add/cut.tar.gz",
			content: func(t *testing.T, path string) {
				full := tarBytes(t, true, archEntry{"list", strings.Repeat("a.com\n", 40)})
				if err := os.WriteFile(path, full[:len(full)/2], 0o644); err != nil {
					t.Fatal(err)
				}
			},
			reason: "corrupt",
		},
		{
			name: "a tar.gz with a broken CRC",
			rel:  "com/add/badcrc.tar.gz",
			content: func(t *testing.T, path string) {
				// The data is intact, the CRC32 in the gzip trailer is
				// corrupted: without draining the stream after the tar EOF
				// the error goes unnoticed (the T-133 review, R4).
				full := tarBytes(t, true, archEntry{"list", "a.com\nb.com\n"})
				full[len(full)-8] ^= 0xff
				if err := os.WriteFile(path, full, 0o644); err != nil {
					t.Fatal(err)
				}
			},
			reason: "corrupt",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pt := newPipeTest(t)
			abs := filepath.Join(pt.dir, filepath.FromSlash(tc.rel))
			if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
				t.Fatal(err)
			}
			tc.content(t, abs)

			out, err := pt.pipe.Process(context.Background(), abs)
			if err != nil {
				t.Fatalf("Process: %v (a corrupt archive is not a service error)", err)
			}
			if out.Kind != OutcomeFailed {
				t.Fatalf("the outcome = %s, want failed", out.Kind)
			}
			if out.Err == nil || !strings.Contains(out.Err.Error(), tc.reason) {
				t.Errorf("the reason = %v, want %q", out.Err, tc.reason)
			}
			if len(pt.files.records) != 0 {
				t.Error("no journal record is written for a corrupt archive")
			}
			failed := filepath.Join(pt.dir, DirFailed, filepath.FromSlash(tc.rel))
			if exists(t, abs) || !exists(t, failed) {
				t.Fatalf("the archive must be in %s", failed)
			}
			if !exists(t, failed+".report") {
				t.Error("the log report was not written next to the corrupt archive")
			}
		})
	}
}

// TestPipelineArchiveLimits — the zip-bomb protection limits (T-133):
// the number of entries and the uncompressed size; for zip the size is
// checked before reading by the header, for tar/gz — by a counter over
// the stream; exceeding — failed.
func TestPipelineArchiveLimits(t *testing.T) {
	t.Run("a zip with more entries than archive_max_files", func(t *testing.T) {
		pt := newPipeTest(t)
		abs := filepath.Join(pt.dir, "com", "add", "many.zip")
		buildZip(t, abs,
			archEntry{"a.txt", "a.com\n"},
			archEntry{"b.txt", "b.com\n"},
			archEntry{"c.txt", "c.com\n"},
		)
		pipe := pt.newPipe(t, func(o *PipelineOptions) { o.ArchiveMaxFiles = 2 })

		out, err := pipe.Process(context.Background(), abs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if out.Kind != OutcomeFailed {
			t.Fatalf("the outcome = %s, want failed", out.Kind)
		}
		if out.Err == nil || !strings.Contains(out.Err.Error(), "ingest.archive_max_files (2)") {
			t.Errorf("the reason = %v, want the archive_max_files excess", out.Err)
		}
		// The check over the central directory — before loading: the only
		// staging access is the cleanup of the discarded batch, no
		// application.
		if got := strings.Join(pt.pool.ops, "\n"); got != "exec:"+clearStageSQL {
			t.Errorf("the calls = %s, want a single batch cleanup", got)
		}
		if !exists(t, filepath.Join(pt.dir, DirFailed, "com/add/many.zip")) {
			t.Error("the archive was not moved to failed/")
		}
	})

	t.Run("a tar with more entries than archive_max_files", func(t *testing.T) {
		pt := newPipeTest(t)
		abs := filepath.Join(pt.dir, "com", "add", "many.tar")
		buildTar(t, abs, false,
			archEntry{"a.txt", "a.com\n"},
			archEntry{"b.txt", "b.com\n"},
			archEntry{"c.txt", "c.com\n"},
		)
		pipe := pt.newPipe(t, func(o *PipelineOptions) { o.ArchiveMaxFiles = 2 })

		out, err := pipe.Process(context.Background(), abs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if out.Kind != OutcomeFailed || out.Err == nil ||
			!strings.Contains(out.Err.Error(), "ingest.archive_max_files (2)") {
			t.Fatalf("the outcome = %s (%v), want failed by archive_max_files", out.Kind, out.Err)
		}
	})

	t.Run("a zip's size by the header before reading", func(t *testing.T) {
		pt := newPipeTest(t)
		abs := filepath.Join(pt.dir, "com", "add", "big.zip")
		buildZip(t, abs, archEntry{"big.txt", strings.Repeat("a.com\n", 20)}) // 120 bytes
		pipe := pt.newPipe(t, func(o *PipelineOptions) { o.ArchiveMaxUncompressed = 64 })

		out, err := pipe.Process(context.Background(), abs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if out.Kind != OutcomeFailed {
			t.Fatalf("the outcome = %s, want failed", out.Kind)
		}
		if out.Err == nil || !strings.Contains(out.Err.Error(), "ingest.archive_max_uncompressed") ||
			!strings.Contains(out.Err.Error(), "big.txt") {
			t.Errorf("the reason = %v, want the member big.txt's uncompressed size excess", out.Err)
		}
		// The header is checked before reading the member: no rows in
		// staging — the batch cleanup by the loading and the discard, no
		// COPY.
		if got := strings.Join(pt.pool.ops, "\n"); got != "exec:"+clearStageSQL+"\nexec:"+clearStageSQL {
			t.Errorf("the calls = %s, want two batch cleanups without COPY", got)
		}
	})

	t.Run("a tar.gz's size counted over the stream", func(t *testing.T) {
		pt := newPipeTest(t)
		abs := filepath.Join(pt.dir, "com", "add", "stream.tar.gz")
		buildTar(t, abs, true, archEntry{"list", strings.Repeat("a.com\n", 60)}) // 360 bytes
		pipe := pt.newPipe(t, func(o *PipelineOptions) { o.ArchiveMaxUncompressed = 100 })

		out, err := pipe.Process(context.Background(), abs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if out.Kind != OutcomeFailed {
			t.Fatalf("the outcome = %s, want failed", out.Kind)
		}
		if out.Err == nil || !strings.Contains(out.Err.Error(), "exceeded ingest.archive_max_uncompressed (100 bytes)") {
			t.Errorf("the reason = %v, want the excess counted over the stream", out.Err)
		}
		if !exists(t, filepath.Join(pt.dir, DirFailed, "com/add/stream.tar.gz")) {
			t.Error("the archive was not moved to failed/")
		}
	})

	t.Run("the entry limit counts directories", func(t *testing.T) {
		pt := newPipeTest(t)
		// A metadata bomb: a tar.gz of thousands of empty directories
		// compresses by orders of magnitude — directory entries must
		// count against the limit (like zip over the central directory).
		entries := make([]archEntry, 0, 2001)
		for i := 0; i < 2000; i++ {
			entries = append(entries, archEntry{name: fmt.Sprintf("d%04d/", i)})
		}
		entries = append(entries, archEntry{"list", "a.com\n"})
		abs := filepath.Join(pt.dir, "com", "add", "bomb.tar.gz")
		buildTar(t, abs, true, entries...)

		out, err := pt.pipe.Process(context.Background(), abs) // the default archive_max_files = 1000
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if out.Kind != OutcomeFailed {
			t.Fatalf("the outcome = %s, want failed", out.Kind)
		}
		if out.Err == nil || !strings.Contains(out.Err.Error(), "ingest.archive_max_files (1000)") {
			t.Errorf("the reason = %v, want the entry count excess", out.Err)
		}
		if !exists(t, filepath.Join(pt.dir, DirFailed, "com/add/bomb.tar.gz")) {
			t.Error("the archive was not moved to failed/")
		}
	})

	t.Run("the size limit counts stream headers", func(t *testing.T) {
		pt := newPipeTest(t)
		entries := make([]archEntry, 0, 3000)
		for i := 0; i < 3000; i++ {
			entries = append(entries, archEntry{name: fmt.Sprintf("d%04d/", i)})
		}
		abs := filepath.Join(pt.dir, "com", "add", "bomb.tar.gz")
		buildTar(t, abs, true, entries...) // the uncompressed stream ≈ 3000×512 ≈ 1.5 MB

		pipe := pt.newPipe(t, func(o *PipelineOptions) {
			o.ArchiveMaxFiles = 100000 // the entries do not limit
			o.ArchiveMaxUncompressed = 4096
		})
		out, err := pipe.Process(context.Background(), abs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if out.Kind != OutcomeFailed {
			t.Fatalf("the outcome = %s, want failed", out.Kind)
		}
		if out.Err == nil || !strings.Contains(out.Err.Error(), "exceeded ingest.archive_max_uncompressed (4096 bytes)") {
			t.Errorf("the reason = %v, want the stream size excess (headers included)", out.Err)
		}
	})
}

// TestPipelineArchiveDuplicate — re-submitting the same archive (T-133,
// spec 5.1): the same triple (zone, action, sha256) is already in the
// journal — a duplicate, not applied, the report is not duplicated, the
// file goes to processed/. The journal for the second processing is
// seeded with the first one's record (the fake's contract: RecordFile
// does not register the content for FilesBySHA256).
func TestPipelineArchiveDuplicate(t *testing.T) {
	pt := newPipeTest(t)
	abs := filepath.Join(pt.dir, "com", "add", "zone.zip")
	buildZip(t, abs, archEntry{"one.txt", "a.com\nb.com\n"})
	raw, err := os.ReadFile(abs)
	if err != nil {
		t.Fatal(err)
	}
	sha := sha256.Sum256(raw)

	pt.pool.execs = addExecs(pt.pool, 2, 0)
	pt.pool.uniques = 2

	out, err := pt.pipe.Process(context.Background(), abs)
	if err != nil || out.Kind != OutcomeApplied {
		t.Fatalf("the first processing: %v, the outcome %s", err, out.Kind)
	}
	if !reflect.DeepEqual(pt.files.records[0].SHA256, sha[:]) {
		t.Fatalf("the journal = %x, want the archive's sha256 %x", pt.files.records[0].SHA256, sha)
	}

	// A repeated delivery of the same content (the file came back to
	// incoming; the first processing's record is already in the journal).
	zone, action := int16(1), model.ActionAdd
	path := "com/add/zone.zip"
	processed := pt.now
	pt.files = newFakeFileStore(model.IngestFile{
		ID: 1, ZoneID: &zone, Action: &action, Path: &path,
		SHA256: sha[:], ProcessedAt: &processed,
	})
	pipe := pt.newPipe(t, nil)
	pt.pipe = pipe

	buildZip(t, abs, archEntry{"one.txt", "a.com\nb.com\n"})
	out, err = pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Kind != OutcomeDuplicate {
		t.Fatalf("the outcome = %s, want duplicate", out.Kind)
	}
	if len(pt.files.records) != 0 {
		t.Errorf("journal records = %d, want 0 (a duplicate does not duplicate the report)", len(pt.files.records))
	}
	// The loading happened (the hash is known after reading), the
	// application — no.
	want := []string{
		"exec:" + clearStageSQL,
		"copy:2",
		"exec:" + clearStageSQL,
	}
	tail := pt.pool.ops[len(pt.pool.ops)-len(want):]
	if strings.Join(tail, "\n") != strings.Join(want, "\n") {
		t.Errorf("the reprocessing calls = %v, want cleanup → COPY → discard", pt.pool.ops)
	}
	// Both copies — in the processed/ archive (a name collision — the -2
	// suffix).
	if !exists(t, filepath.Join(pt.dir, DirProcessed, "com/add/zone.zip")) ||
		!exists(t, filepath.Join(pt.dir, DirProcessed, "com/add/zone-2.zip")) {
		t.Error("both deliveries must be in processed/ (the second one with a suffix)")
	}
}

// TestPipelineArchiveEmpty — an empty archive (T-133): not counted as
// bad (like an empty txt file, T-016) — applied with an empty batch and
// a report. An empty tar is a valid container without entries; empty
// zip/tar are read, an empty .gz is corrupt (no header,
// TestPipelineArchiveBroken).
func TestPipelineArchiveEmpty(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rel   string
		build func(t *testing.T, path string)
	}{
		{"an empty zip", "com/add/empty.zip", func(t *testing.T, p string) { buildZip(t, p) }},
		{"an empty tar", "com/add/empty.tar", func(t *testing.T, p string) { buildTar(t, p, false) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pt := newPipeTest(t)
			abs := filepath.Join(pt.dir, filepath.FromSlash(tc.rel))
			tc.build(t, abs)

			pt.pool.execs = addExecs(pt.pool, 0, 0)

			out, err := pt.pipe.Process(context.Background(), abs)
			if err != nil {
				t.Fatalf("Process: %v", err)
			}
			if out.Kind != OutcomeApplied {
				t.Fatalf("the outcome = %s, want applied", out.Kind)
			}
			rep := out.Report
			if rep.LinesTotal != 0 || rep.Accepted != 0 || rep.Members != 0 || rep.Added != 0 {
				t.Errorf("the counters = %+v, want zeros (an empty archive)", rep)
			}
			if len(pt.files.records) != 1 {
				t.Error("the empty archive's report must be written to the journal")
			}
			// COPY with an empty chunk does not run (db.CopyFrom: an empty
			// chunk is a no-op); applying an empty batch — the bound and
			// the count without an insert and zone deltas (T-205).
			want := []string{
				"exec:" + clearStageSQL,
				"begin",
				"query:" + addChunkBoundSQL,
				"query:" + reviveAddSQL,
				"query:" + countAddSQL,
				"exec:" + clearStageSQL,
				"commit",
			}
			if got := strings.Join(pt.pool.ops, "\n"); got != strings.Join(want, "\n") {
				t.Errorf("the calls =\n%s\nwant:\n%s", got, strings.Join(want, "\n"))
			}
		})
	}
}

// TestPipelineArchivePaddedTarSHA — the sha256 of a .tar with GNU
// padding (T-133): tar.Reader stops at the EOF blocks without reading
// the tail — the pipeline drains the stream, and the hash equals the
// sha256 of the whole file (the ingest_files.sha256 contract, checked
// by the operator with sha256sum).
func TestPipelineArchivePaddedTarSHA(t *testing.T) {
	pt := newPipeTest(t)
	raw := tarBytes(t, false, archEntry{"list", "a.com\nb.com\n"})
	// GNU tar pads the archive with zeros up to the record size (a 10
	// KiB block).
	if r := len(raw) % 10240; r != 0 {
		raw = append(raw, make([]byte, 10240-r)...)
	}
	abs := filepath.Join(pt.dir, "com", "add", "padded.tar")
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(abs, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	sha := sha256.Sum256(raw)

	pt.pool.execs = addExecs(pt.pool, 2, 0)
	pt.pool.uniques = 2

	out, err := pt.pipe.Process(context.Background(), abs)
	if err != nil || out.Kind != OutcomeApplied {
		t.Fatalf("processing: %v, the outcome %s", err, out.Kind)
	}
	if string(out.Report.SHA256) != string(sha[:]) {
		t.Errorf("sha256 = %x, want the hash of the whole file %x", out.Report.SHA256, sha)
	}
	if !reflect.DeepEqual(pt.files.records[0].SHA256, sha[:]) {
		t.Errorf("the journal: sha256 = %x, want %x", pt.files.records[0].SHA256, sha)
	}
}

// TestPipelineArchiveNestedNotUnpacked — a nested archive is not
// unpacked (T-133): an archive member is binary junk, the error share
// sinks the archive as a whole.
func TestPipelineArchiveNestedNotUnpacked(t *testing.T) {
	pt := newPipeTest(t)
	inner := zipBytes(t, archEntry{"deep.txt", "a.com\n"})
	abs := filepath.Join(pt.dir, "com", "add", "outer.zip")
	buildZip(t, abs, archEntry{"inner.zip", string(inner)})

	out, err := pt.pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v (a bad archive is not a service error)", err)
	}
	if out.Kind != OutcomeFailed {
		t.Fatalf("the outcome = %s, want failed", out.Kind)
	}
	if out.Err == nil || !strings.Contains(out.Err.Error(), "inner.zip") ||
		!strings.Contains(out.Err.Error(), "ingest.max_error_ratio") {
		t.Errorf("the reason = %v, want the inner.zip member's rejection share", out.Err)
	}
	if out.Report.Accepted != 0 { // the inner archive's domains were not applied
		t.Errorf("accepted = %d, want 0 (a nested archive is junk, not a list)", out.Report.Accepted)
	}
	if !exists(t, filepath.Join(pt.dir, DirFailed, "com/add/outer.zip")) {
		t.Error("the archive was not moved to failed/")
	}
}

// TestPipelineArchiveStageError — a COPY failure (T-133): an
// infrastructure failure — the archive stays in incoming
// (at-least-once, spec 3.4), the batch is discarded.
func TestPipelineArchiveStageError(t *testing.T) {
	pt := newPipeTest(t)
	abs := filepath.Join(pt.dir, "com", "add", "zone.zip")
	buildZip(t, abs, archEntry{"one.txt", "a.com\n"})
	pt.pool.copyErr = errors.New("COPY rejected")

	out, err := pt.pipe.Process(context.Background(), abs)
	if err == nil || !errors.Is(err, pt.pool.copyErr) {
		t.Fatalf("err = %v, want the COPY error to the caller", err)
	}
	if out.Kind != OutcomeLeft {
		t.Errorf("the outcome = %s, want left", out.Kind)
	}
	if !exists(t, abs) {
		t.Error("the archive must stay in incoming")
	}
	if len(pt.files.records) != 0 {
		t.Error("no journal record is written on a load failure")
	}
}

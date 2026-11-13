// Integration tests of the file ingest pipeline (T-023; spec 11.2,
// 5.1): the path "a file on disk in incoming → the internal/ingest
// pipeline → records in zones/domains/ingest_files + moving the file
// to processed/failed" against live PostgreSQL with the real migration
// schema.
//
// Level: the end-to-end Pipeline (T-021) with real SQL stores
// (SQLZoneStore, SQLFileStore, COPY into staging, applying add/delete)
// — their behavior against the live DB is what is checked here.
// Directory watching (fsnotify, file stability) — the T-013 unit
// territory: the pipeline receives a ready path, the way the watcher
// (cmd/ingest) produces it.
//
// Isolation: each check applies migrations in its own schema
// (migratedSchema, like the schema tests) — a repeated run is
// idempotent, the working crawler schema is not touched. The incoming
// directory — t.TempDir.
package integration

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/idn"
	"crawler/internal/ingest"
	"crawler/internal/model"
)

// ingestEnv — the ingest check environment: a schema with migrations,
// an incoming directory (t.TempDir) and a pipeline on real SQL stores —
// the same build cmd/ingest makes.
type ingestEnv struct {
	pool *pgxpool.Pool
	dir  string // incoming
	pipe *ingest.Pipeline
}

func newIngestEnv(t *testing.T) *ingestEnv {
	t.Helper()

	pool := migratedSchema(t)
	dir := t.TempDir()
	pipe, err := ingest.NewPipeline(
		ingest.NewZoneStore(pool),
		ingest.NewFileStore(pool),
		pool,
		ingest.PipelineOptions{
			Dir: dir,
			// A small COPY batch: a file of dozens of lines is loaded
			// by several batches, like big files in production.
			BatchSize: 4,
			// Test zones are multi-level (crawl.test, the IDN one in
			// acceptance): ingest directories are allowed through an
			// allowlist (the tld-only-zones spec §5.1), like the
			// testbed config in production.
			AllowedZonePaths: []string{"crawl.test", accIDNZone},
		},
	)
	if err != nil {
		t.Fatalf("ingest.NewPipeline: %v", err)
	}
	return &ingestEnv{pool: pool, dir: dir, pipe: pipe}
}

// putFile writes the file rel (relative to incoming) with the content
// and returns the absolute path.
func (e *ingestEnv) putFile(t *testing.T, rel, content string) string {
	t.Helper()
	abs := filepath.Join(e.dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(abs, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return abs
}

// process writes a file and runs it through the pipeline. A returned
// error — only an infrastructure failure (the file stayed in incoming);
// the planned outcomes (applied/duplicate/failed) are checked by the
// Outcome.
func (e *ingestEnv) process(t *testing.T, rel, content string) ingest.Outcome {
	t.Helper()
	out, err := e.pipe.Process(context.Background(), e.putFile(t, rel, content))
	if err != nil {
		t.Fatalf("Process(%s): %v", rel, err)
	}
	return out
}

// fileContent assembles file content with a trailing newline.
func fileContent(lines ...string) string {
	return strings.Join(lines, "\n") + "\n"
}

// domainState — the observable domains columns.
type domainState struct {
	label      string
	status     int16
	checkState int16
	flags      int16
	queuedAt   *time.Time
}

// domain reads a domain by the zone punycode name and the label.
func (e *ingestEnv) domain(t *testing.T, zone, label string) (domainState, bool) {
	t.Helper()
	var d domainState
	err := e.pool.QueryRow(context.Background(), `
		SELECT d.label, d.status, d.check_state, d.flags, d.queued_at
		FROM domains AS d JOIN zones AS z ON z.id = d.zone_id
		WHERE z.name = $1 AND d.label = $2`, zone, label,
	).Scan(&d.label, &d.status, &d.checkState, &d.flags, &d.queuedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return d, false
	}
	if err != nil {
		t.Fatalf("reading the domain %s.%s: %v", label, zone, err)
	}
	return d, true
}

// zoneState — the observable zones columns.
type zoneState struct {
	id       int16
	name     string
	unicode  string // '' when NULL
	isActive bool
}

// zone reads a zones row by the punycode name.
func (e *ingestEnv) zone(t *testing.T, name string) (zoneState, bool) {
	t.Helper()
	var z zoneState
	err := e.pool.QueryRow(context.Background(),
		"SELECT id, name, COALESCE(name_unicode, ''), is_active FROM zones WHERE name = $1", name,
	).Scan(&z.id, &z.name, &z.unicode, &z.isActive)
	if errors.Is(err, pgx.ErrNoRows) {
		return z, false
	}
	if err != nil {
		t.Fatalf("reading the zone %s: %v", name, err)
	}
	return z, true
}

// fileRecordState — the ingest_files report columns. The pipeline
// always writes them (nullable in the spec 6.1 schema — for manual
// inserts); NULL in a live record — a check error.
type fileRecordState struct {
	zoneID     int16
	action     int16
	path       string
	linesTotal int64
	added      int64
	existed    int64
	deleted    int64
	rejected   int64
}

// fileBySHA reads the ingest_files journal row by the content hash.
func (e *ingestEnv) fileBySHA(t *testing.T, sha []byte) (fileRecordState, bool) {
	t.Helper()
	var f fileRecordState
	err := e.pool.QueryRow(context.Background(), `
		SELECT zone_id, action, path, lines_total, added, existed, deleted, rejected
		FROM ingest_files WHERE sha256 = $1`, sha,
	).Scan(&f.zoneID, &f.action, &f.path, &f.linesTotal, &f.added, &f.existed, &f.deleted, &f.rejected)
	if errors.Is(err, pgx.ErrNoRows) {
		return f, false
	}
	if err != nil {
		t.Fatalf("reading ingest_files: %v", err)
	}
	return f, true
}

// countOf — the number of rows in a table of the test schema.
func (e *ingestEnv) countOf(t *testing.T, table string) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count(%s): %v", table, err)
	}
	return n
}

// existsOnDisk — the path exists.
func existsOnDisk(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Lstat(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return err == nil
}

// TestIngest_AddCreatesZonesAndDomains — the "file in incoming →
// records in the DB" scenario (spec 11.2, 5.1): an add-file creates
// the zone automatically, the rows become domains (new/pending, is_idn
// on IDN labels), the report is written to ingest_files (sha256,
// counters), the file moves to processed keeping the structure.
func TestIngest_AddCreatesZonesAndDomains(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	content := fileContent(
		"example.crawl.test", // a valid line
		"# комментарий",      // skipped by the parser
		"",                   // an empty one — too
		"сайт.crawl.test",    // IDN: the label in punycode, the is_idn bit (spec 5.2 item 9)
		"shop.crawl.test",
		"example.crawl.test", // a repeated line: valid, dedup — on the DB side
	)
	sha := sha256.Sum256([]byte(content))
	out := env.process(t, "crawl.test/add/list.txt", content)
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("outcome = %q, want applied", out.Kind)
	}

	// The zone is created automatically on the first file (spec 5.1).
	z, ok := env.zone(t, "crawl.test")
	if !ok {
		t.Fatal("the crawl.test zone not created")
	}
	if !z.isActive {
		t.Error("the zone created inactive, want is_active = true")
	}
	if z.unicode != "" {
		t.Errorf("name_unicode = %q, want NULL (an ASCII zone, spec 5.2 item 7)", z.unicode)
	}
	if n := env.countOf(t, "zones"); n != 1 {
		t.Errorf("zones = %d, want 1", n)
	}

	// Three unique domains (a repeated line does not create a
	// duplicate), all new and pending a check (spec 6.2), without a
	// lease.
	if n := env.countOf(t, "domains"); n != 3 {
		t.Errorf("domains = %d, want 3", n)
	}
	for _, label := range []string{"example", "shop"} {
		d, ok := env.domain(t, "crawl.test", label)
		if !ok {
			t.Fatalf("the %s.crawl.test domain not created", label)
		}
		if d.status != int16(model.StatusNew) || d.checkState != int16(model.CheckStatePending) {
			t.Errorf("%s: status/check_state = %d/%d, want new/pending", label, d.status, d.checkState)
		}
		if d.queuedAt != nil {
			t.Errorf("%s: queued_at not NULL on a new domain", label)
		}
		if d.flags&int16(model.FlagIDN) != 0 {
			t.Errorf("%s: the is_idn bit set on an ASCII label", label)
		}
	}

	// The IDN domain: the label in punycode (spec 5.2), the is_idn bit
	// (spec 5.2 item 9). The crawl.test path zone is not in the PSL, so
	// the Label of the normalization result is compound — the label
	// without the path zone is cut off manually (the way the T-016
	// validator does).
	res, err := idn.Normalize("сайт.crawl.test")
	if err != nil {
		t.Fatal(err)
	}
	label := strings.TrimSuffix(res.ASCII, ".crawl.test")
	if !strings.HasPrefix(label, "xn--") || strings.Contains(label, ".") {
		t.Fatalf("the test data: the сайт label = %q, punycode without dots expected", label)
	}
	d, ok := env.domain(t, "crawl.test", label)
	if !ok {
		t.Fatalf("the IDN domain сайт.crawl.test (%s) not created", label)
	}
	if d.flags&int16(model.FlagIDN) == 0 {
		t.Errorf("the %s label has no is_idn bit", label)
	}

	// The report in the journal (spec 6.1): the sha256 of the content
	// and the counters.
	f, ok := env.fileBySHA(t, sha[:])
	if !ok {
		t.Fatal("the file report not written to ingest_files")
	}
	want := fileRecordState{
		zoneID:     z.id,
		action:     int16(model.ActionAdd),
		path:       "crawl.test/add/list.txt",
		linesTotal: 6,
		added:      3,
	}
	if f != want {
		t.Errorf("the ingest_files record = %+v, want %+v", f, want)
	}

	// The file left incoming and landed in processed keeping the
	// structure (spec 5.1); the staging batch is cleared.
	if !existsOnDisk(t, filepath.Join(env.dir, ingest.DirProcessed, "crawl.test/add/list.txt")) {
		t.Error("the file not moved to processed/crawl.test/add/list.txt")
	}
	if existsOnDisk(t, filepath.Join(env.dir, "crawl.test/add/list.txt")) {
		t.Error("the file left in incoming")
	}
	if n := env.countOf(t, "ingest_stage"); n != 0 {
		t.Errorf("ingest_stage rows after the processing = %d, want 0", n)
	}
}

// TestIngest_UnicodeZoneDirectory — a zone directory in unicode and in
// punycode — one zone (spec 5.1, 11.4 item 1): zones.name — punycode,
// name_unicode — the unicode form for display; files from both
// directories converge into one zones row without duplicates.
func TestIngest_UnicodeZoneDirectory(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	out := env.process(t, "москва/add/список.txt", fileContent("сайт.москва", "торг.москва"))
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("outcome = %q, want applied", out.Kind)
	}

	// The zone is created in punycode form; xn--80adxhks is the
	// established punycode form of "москва" (an independent anchor
	// against idn.Normalize).
	z, ok := env.zone(t, "xn--80adxhks")
	if !ok {
		t.Fatal("the москва zone (xn--80adxhks) not created")
	}
	if z.name != "xn--80adxhks" || z.unicode != "москва" {
		t.Errorf("the zone = %q/%q, want xn--80adxhks/москва", z.name, z.unicode)
	}

	// The IDN domain: a punycode label + the is_idn bit; the unicode
	// file name plays no role.
	res, err := idn.Normalize("сайт.москва")
	if err != nil {
		t.Fatal(err)
	}
	d, ok := env.domain(t, "xn--80adxhks", res.Label)
	if !ok {
		t.Fatalf("the сайт.москва domain (%s) not created", res.Label)
	}
	if d.flags&int16(model.FlagIDN) == 0 {
		t.Error("the IDN label has no is_idn bit")
	}

	// A file from the punycode directory of the same zone — the same
	// zones row, no zone duplicates arise (spec 11.4 item 1).
	out = env.process(t, "xn--80adxhks/add/second.txt", fileContent("third.москва"))
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("outcome = %q, want applied", out.Kind)
	}
	if n := env.countOf(t, "zones"); n != 1 {
		t.Errorf("zones = %d, want 1 (the unicode/punycode directories — one zone)", n)
	}
	if n := env.countOf(t, "domains"); n != 3 {
		t.Errorf("domains = %d, want 3", n)
	}
	if _, ok := env.domain(t, "xn--80adxhks", "third"); !ok {
		t.Error("the third.москва domain not created")
	}
}

// TestIngest_DuplicateFileSkipped — a repeat of the same file (the same
// sha256) already in the journal: re-processing is safe (spec 5.1) —
// the apply is not performed, no new rows, the file moves to processed
// without a second ingest_files record.
func TestIngest_DuplicateFileSkipped(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	content := fileContent("example.crawl.test", "shop.crawl.test")
	out := env.process(t, "crawl.test/add/list.txt", content)
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("the first processing: outcome = %q, want applied", out.Kind)
	}

	// The same copy of the file appears in incoming again.
	out = env.process(t, "crawl.test/add/list.txt", content)
	if out.Kind != ingest.OutcomeDuplicate {
		t.Fatalf("the repeated processing: outcome = %q, want duplicate", out.Kind)
	}
	if n := env.countOf(t, "ingest_files"); n != 1 {
		t.Errorf("ingest_files records = %d, want 1 (a duplicate is not reported)", n)
	}
	if n := env.countOf(t, "domains"); n != 2 {
		t.Errorf("domains = %d, want 2 (a duplicate is not applied)", n)
	}

	// Both file instances are in the archive: a name collision — the
	// -2 suffix (T-021).
	proc := filepath.Join(env.dir, ingest.DirProcessed, "crawl.test/add")
	for _, name := range []string{"list.txt", "list-2.txt"} {
		if !existsOnDisk(t, filepath.Join(proc, name)) {
			t.Errorf("%s missing from the processed archive", name)
		}
	}
}

// TestIngest_ModifiedFileSamePath — a modified file at the same path (a
// different sha256) with an overlap and new domains: the existing ones
// are not changed, the new ones are added (spec 5.1: "an existing
// domain is not changed, except for the deleted status case").
func TestIngest_ModifiedFileSamePath(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	if out := env.process(t, "crawl.test/add/list.txt",
		fileContent("example.crawl.test", "shop.crawl.test")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("the first processing: outcome = %q, want applied", out.Kind)
	}

	// example has "passed" the check: ingest did not change the
	// status/state — a repeated add must not overwrite them.
	if _, err := env.pool.Exec(context.Background(), `
		UPDATE domains AS d SET status = $1, check_state = $2, queued_at = now()
		FROM zones AS z WHERE z.id = d.zone_id AND z.name = $3 AND d.label = $4`,
		int16(model.StatusAlive), int16(model.CheckStateDone), "crawl.test", "example"); err != nil {
		t.Fatal(err)
	}

	out := env.process(t, "crawl.test/add/list.txt",
		fileContent("example.crawl.test", "blog.crawl.test", "forum.crawl.test"))
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("the second processing: outcome = %q, want applied", out.Kind)
	}
	if out.Report.Added != 2 || out.Report.Existed != 1 || out.Report.Revived != 0 {
		t.Errorf("counters = added %d, existed %d, revived %d; want 2/1/0",
			out.Report.Added, out.Report.Existed, out.Report.Revived)
	}

	// The existing domain is untouched: alive/done and the lease in place.
	d, ok := env.domain(t, "crawl.test", "example")
	if !ok {
		t.Fatal("the example domain disappeared")
	}
	if d.status != int16(model.StatusAlive) || d.checkState != int16(model.CheckStateDone) {
		t.Errorf("example: status/check_state = %d/%d, want alive/done (a repeated add does not change them)",
			d.status, d.checkState)
	}
	if d.queuedAt == nil {
		t.Error("queued_at reset on the existing domain")
	}

	// The new ones — as from the first file: new/pending.
	for _, label := range []string{"blog", "forum"} {
		d, ok := env.domain(t, "crawl.test", label)
		if !ok {
			t.Fatalf("the %s domain not added", label)
		}
		if d.status != int16(model.StatusNew) || d.checkState != int16(model.CheckStatePending) {
			t.Errorf("%s: status/check_state = %d/%d, want new/pending", label, d.status, d.checkState)
		}
	}
	if n := env.countOf(t, "domains"); n != 4 {
		t.Errorf("domains = %d, want 4", n)
	}
	if n := env.countOf(t, "ingest_files"); n != 2 {
		t.Errorf("ingest_files records = %d, want 2 (one per file)", n)
	}
}

// TestIngest_DeleteMarksDeleted — a delete-file transitions the listed
// domains to deleted, the domain stays in the DB (spec 13.1); missing
// ones — not_found in the pipeline report (no column in ingest_files,
// spec 6.1).
func TestIngest_DeleteMarksDeleted(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	if out := env.process(t, "crawl.test/add/list.txt",
		fileContent("example.crawl.test", "shop.crawl.test")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("add: outcome = %q, want applied", out.Kind)
	}

	content := fileContent("example.crawl.test", "ghost.crawl.test")
	sha := sha256.Sum256([]byte(content))
	out := env.process(t, "crawl.test/delete/x.txt", content)
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("delete: outcome = %q, want applied", out.Kind)
	}
	if out.Report.Deleted != 1 || out.Report.NotFound != 1 {
		t.Errorf("delete counters = deleted %d, not_found %d; want 1/1",
			out.Report.Deleted, out.Report.NotFound)
	}

	// The domain stays in the DB with the deleted status (spec 13.1).
	d, ok := env.domain(t, "crawl.test", "example")
	if !ok {
		t.Fatal("the deleted domain disappeared from the DB (spec 13.1: it stays)")
	}
	if d.status != int16(model.StatusDeleted) {
		t.Errorf("example: status = %d, want deleted", d.status)
	}

	// The domains not listed in the file are untouched.
	d, ok = env.domain(t, "crawl.test", "shop")
	if !ok {
		t.Fatal("the shop domain disappeared from the DB — delete does not remove rows (spec 13.1)")
	}
	if d.status != int16(model.StatusNew) {
		t.Errorf("shop: status = %d, want new (not listed in the delete file)", d.status)
	}
	if n := env.countOf(t, "domains"); n != 2 {
		t.Errorf("domains = %d, want 2 (the DB rows are not removed)", n)
	}

	// The delete-file report in the journal: the delete action, the
	// deleted counter.
	f, ok := env.fileBySHA(t, sha[:])
	if !ok {
		t.Fatal("the delete-file report not written")
	}
	if f.action != int16(model.ActionDelete) || f.path != "crawl.test/delete/x.txt" || f.deleted != 1 {
		t.Errorf("the ingest_files record = %+v, want action delete, deleted 1", f)
	}
	if !existsOnDisk(t, filepath.Join(env.dir, ingest.DirProcessed, "crawl.test/delete/x.txt")) {
		t.Error("the delete file not moved to processed")
	}
}

// TestIngest_ReviveDeletedDomain — returning a deleted domain by an
// add-file (spec 5.1: "except for the deleted status case — it returns
// to new/pending"): status → new, check_state → pending, the lease is
// reset.
func TestIngest_ReviveDeletedDomain(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	if out := env.process(t, "crawl.test/add/one.txt",
		fileContent("example.crawl.test")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("add: outcome = %q, want applied", out.Kind)
	}
	// The domain had already gone into work before the deletion: the
	// lease stayed with the deleted domain — the return must reset it
	// (pending without a lease).
	if _, err := env.pool.Exec(context.Background(), `
		UPDATE domains AS d SET check_state = $1, queued_at = now()
		FROM zones AS z WHERE z.id = d.zone_id AND z.name = $2 AND d.label = $3`,
		int16(model.CheckStateQueued), "crawl.test", "example"); err != nil {
		t.Fatal(err)
	}
	if out := env.process(t, "crawl.test/delete/x.txt",
		fileContent("# снятие с проверки", "example.crawl.test")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("delete: outcome = %q, want applied", out.Kind)
	}

	// Different content — a different sha256 (otherwise it would be a
	// file duplicate), the same domain.
	out := env.process(t, "crawl.test/add/one.txt",
		fileContent("example.crawl.test", "# повторная загрузка"))
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("the revival: outcome = %q, want applied", out.Kind)
	}
	if out.Report.Added != 1 || out.Report.Revived != 1 || out.Report.Existed != 0 {
		t.Errorf("counters = added %d, revived %d, existed %d; want 1/1/0",
			out.Report.Added, out.Report.Revived, out.Report.Existed)
	}

	d, ok := env.domain(t, "crawl.test", "example")
	if !ok {
		t.Fatal("the example domain disappeared")
	}
	if d.status != int16(model.StatusNew) || d.checkState != int16(model.CheckStatePending) {
		t.Errorf("example: status/check_state = %d/%d, want new/pending after the revival",
			d.status, d.checkState)
	}
	if d.queuedAt != nil {
		t.Error("queued_at not reset on the revived domain")
	}
}

// TestIngest_RejectedLinesWithinThreshold — single bad lines do not
// fail the file (spec 5.1): a foreign zone and broken punycode are
// rejected and visible in the report counters (the rejected reasons),
// the valid lines are applied, the file is processed within the
// ingest.max_error_ratio threshold.
func TestIngest_RejectedLinesWithinThreshold(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	lines := make([]string, 0, 22)
	for i := 1; i <= 20; i++ {
		lines = append(lines, fmt.Sprintf("ok%02d.crawl.test", i))
	}
	lines = append(lines,
		"bad.example.com",  // a foreign zone (wrong_zone, spec 5.2 item 6)
		"xn--a.crawl.test", // broken punycode (invalid, spec 5.2 item 3)
	)
	content := fileContent(lines...)
	sha := sha256.Sum256([]byte(content))

	out := env.process(t, "crawl.test/add/dump.txt", content)
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("outcome = %q, want applied (2 rejections out of 22 ≈ 9%% below the 10%% threshold)", out.Kind)
	}
	if out.Report.Rejected != 2 || out.Report.Accepted != 20 {
		t.Errorf("lines = accepted %d, rejected %d; want 20/2", out.Report.Accepted, out.Report.Rejected)
	}
	if out.Report.Reasons[ingest.ReasonWrongZone] != 1 || out.Report.Reasons[ingest.ReasonInvalid] != 1 {
		t.Errorf("rejection reasons = %v, want wrong_zone 1, invalid 1", out.Report.Reasons)
	}

	// Only the valid lines became domains.
	if n := env.countOf(t, "domains"); n != 20 {
		t.Errorf("domains = %d, want 20", n)
	}
	if _, ok := env.domain(t, "crawl.test", "bad"); ok {
		t.Error("a foreign-zone line made it into domains")
	}

	f, ok := env.fileBySHA(t, sha[:])
	if !ok {
		t.Fatal("the report not written")
	}
	if f.rejected != 2 || f.added != 20 || f.linesTotal != 22 {
		t.Errorf("the ingest_files record = %+v, want rejected 2, added 20, lines 22", f)
	}
	if !existsOnDisk(t, filepath.Join(env.dir, ingest.DirProcessed, "crawl.test/add/dump.txt")) {
		t.Error("the file with single rejections did not land in processed")
	}
}

// TestIngest_ErrorRatioFailsFile — a rejection share above
// ingest.max_error_ratio (spec 5.1, T-016): the file is wrong as a
// whole — not a single line is applied (including valid ones), no
// ingest_files record, the file in failed/ with a log report next to
// it.
func TestIngest_ErrorRatioFailsFile(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	// 4 rejections out of 5 content lines = 80% > 10%.
	content := fileContent(
		"ok.crawl.test", // valid — but the file is wrong as a whole
		"a.example.com", // a foreign zone
		"b.example.com",
		"c.example.com",
		"xn--a.crawl.test", // broken punycode
	)
	out := env.process(t, "crawl.test/add/dump.txt", content)
	if out.Kind != ingest.OutcomeFailed {
		t.Fatalf("outcome = %q, want failed (the rejection share above the threshold)", out.Kind)
	}

	// Neither domains nor a report: the batch is dropped, the journal
	// row is not written (T-021: the same hash does not occupy the
	// journal, a fixed file can be loaded again).
	if n := env.countOf(t, "domains"); n != 0 {
		t.Errorf("domains = %d, want 0 (the file is wrong as a whole)", n)
	}
	if n := env.countOf(t, "ingest_files"); n != 0 {
		t.Errorf("ingest_files records = %d, want 0", n)
	}
	if n := env.countOf(t, "ingest_stage"); n != 0 {
		t.Errorf("ingest_stage rows = %d, want 0 (the batch dropped)", n)
	}

	// The file in failed/ with a log report next to it (spec 5.1).
	failed := filepath.Join(env.dir, ingest.DirFailed, "crawl.test/add/dump.txt")
	if !existsOnDisk(t, failed) {
		t.Fatal("the file not moved to failed/crawl.test/add/dump.txt")
	}
	report, err := os.ReadFile(failed + ".report")
	if err != nil {
		t.Fatalf("the log report next to the file: %v", err)
	}
	if !strings.Contains(string(report), "crawl.test/add/dump.txt") {
		t.Errorf("the log report does not name the file:\n%s", report)
	}
	for _, reason := range []string{"wrong_zone: 3", "invalid: 1"} {
		if !strings.Contains(string(report), reason) {
			t.Errorf("the log report missing the %q counter:\n%s", reason, report)
		}
	}
	if existsOnDisk(t, filepath.Join(env.dir, "crawl.test/add/dump.txt")) {
		t.Error("the file left in incoming")
	}
}

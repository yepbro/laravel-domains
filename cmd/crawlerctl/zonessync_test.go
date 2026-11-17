// Unit tests for crawlerctl zones sync (T-125, stage 14; the
// --deactivate-missing gate — T-311) on a fake pool (db.DB) and an
// httptest server — without a DB and the network. Checked: the IANA
// slice parsing (the version from the first comment, upper case,
// unicode→punycode, dedup, junk with a line number), the exact SQL texts
// and parameters (a contract, the zones_test.go style) and the fake's
// live semantics: the batch insert adds only new zones enabled, existing
// ones are not touched at all, without --deactivate-missing DB zones are
// not disabled, with the flag but without --apply — a dry-run (the
// candidates listed, the disabling UPDATE not executed), with --apply —
// the ones missing from the slice are disabled (idempotently, with a
// report); the large-share sanity guard: a refusal without --force, the
// bypass with it; the e2e run() — exit codes and error contexts modeled
// on the neighboring commands. Behavior against live
// PostgreSQL/data.iana.org — the dev environment (make shell).
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/db"
)

// --- SQL contracts ---------------------------------------------------------

// The want texts duplicate the constants: a SQL change without updating
// the expectations (or vice versa) fails the test loudly.
const wantZonesSyncInsert = "INSERT INTO zones (name, name_unicode, is_active) " +
	"SELECT u.name, nullif(u.unicode, ''), true " +
	"FROM unnest($1::text[], $2::text[]) AS u(name, unicode) " +
	"WHERE NOT EXISTS (SELECT 1 FROM zones AS z WHERE z.name = u.name) " +
	"ON CONFLICT (name) DO NOTHING"

const wantZonesDeactivateMissing = "UPDATE zones SET is_active = false " +
	"WHERE is_active AND name <> ALL($1::text[]) " +
	"RETURNING id, name, coalesce(name_unicode, '') AS name_unicode"

const wantZonesDeactivateCandidates = "SELECT z.id, z.name, coalesce(z.name_unicode, '') AS name_unicode, a.active_total " +
	"FROM zones AS z CROSS JOIN (SELECT count(*) AS active_total FROM zones WHERE is_active) AS a " +
	"WHERE z.is_active AND z.name <> ALL($1::text[]) ORDER BY z.name"

func TestZonesSyncSQLContract(t *testing.T) {
	for sql, want := range map[string]string{
		zonesSyncInsertSQL:           wantZonesSyncInsert,
		zonesDeactivateMissingSQL:    wantZonesDeactivateMissing,
		zonesDeactivateCandidatesSQL: wantZonesDeactivateCandidates,
	} {
		if sql != want {
			t.Errorf("the SQL contract:\ngot %s\nwant %s", sql, want)
		}
	}
}

// --- parser ------------------------------------------------------------------

func TestParseZoneList(t *testing.T) {
	// A mini copy of the slice format: a version comment, upper case,
	// unicode and punycode spellings of one zone (dedup after
	// normalization), a trailing comment and empty lines.
	src := "# Version 2026101900, Last Updated Sun Oct 19 07:07:01 2026 UTC\n" +
		"AAA\nCOM\nOrg\nРФ\nXN--P1AI\n# a trailing comment is not the version\n\n"
	list, err := parseZoneList([]byte(src))
	if err != nil {
		t.Fatalf("parseZoneList: %v", err)
	}
	if list.Version != "2026101900" {
		t.Errorf("the version = %q, want 2026101900", list.Version)
	}
	want := []string{"aaa", "com", "org", "xn--p1ai"} // XN--P1AI and its unicode spelling converge into one
	if !reflect.DeepEqual(list.Zones, want) {
		t.Errorf("the zones = %v, want %v", list.Zones, want)
	}
	if list.Duplicates != 1 {
		t.Errorf("duplicates %d, want 1", list.Duplicates)
	}
}

func TestParseZoneListVersion(t *testing.T) {
	cases := map[string]string{
		"# Version 2026101900, Last Updated Sun Oct 19 07:07:01 2026 UTC": "2026101900",
		"# Version 42":    "42",
		"# a custom list": "a custom list", // a non-version comment — in full
		"#":               "",
	}
	for comment, want := range cases {
		src := comment + "\nCOM\nORG\n"
		list, err := parseZoneList([]byte(src))
		if err != nil {
			t.Fatalf("parseZoneList(%q): %v", src, err)
		}
		if list.Version != want {
			t.Errorf("the version from %q = %q, want %q", comment, list.Version, want)
		}
	}
	// A file without comments at all.
	list, err := parseZoneList([]byte("COM\nORG\n"))
	if err != nil {
		t.Fatalf("parseZoneList without a comment: %v", err)
	}
	if list.Version != "" {
		t.Errorf("the version = %q, want empty", list.Version)
	}
	// A comment after the zone lines — not a header, does not become the version.
	list, err = parseZoneList([]byte("COM\n# a late comment\nORG\n"))
	if err != nil {
		t.Fatalf("parseZoneList with a late comment: %v", err)
	}
	if list.Version != "" {
		t.Errorf("the version from the trailing comment = %q, want empty", list.Version)
	}
}

func TestParseZoneListGarbageLine(t *testing.T) {
	// Junk — an error with the line number (the numbering covers all file
	// lines), not a silent skip: line 4 after the comment, a valid zone
	// and an empty line.
	src := "# Version 1\nCOM\n\nbad!!zone\nORG\n"
	_, err := parseZoneList([]byte(src))
	if err == nil || !strings.Contains(err.Error(), "line 4") {
		t.Fatalf("err = %v, want the line number 4", err)
	}
	if !strings.Contains(err.Error(), "bad!!zone") {
		t.Errorf("the error does not name the junk line: %v", err)
	}
}

func TestParseZoneListNoZones(t *testing.T) {
	for name, src := range map[string]string{
		"an empty file":    "",
		"a comment only":   "# Version 1\n",
		"empty lines only": "\n\n",
	} {
		if _, err := parseZoneList([]byte(src)); err == nil || !strings.Contains(err.Error(), "no zones in the file") {
			t.Errorf("%s: err = %v, want \"no zones in the file\" (the --deactivate-missing guard)", name, err)
		}
	}
}

func TestParseZoneListCRLF(t *testing.T) {
	// Windows line endings (the file could have been edited on a host) —
	// TrimSpace removes \r before normalization.
	list, err := parseZoneList([]byte("# Version 2026101900\r\nCOM\r\nORG\r\n"))
	if err != nil {
		t.Fatalf("parseZoneList CRLF: %v", err)
	}
	if !reflect.DeepEqual(list.Zones, []string{"com", "org"}) {
		t.Errorf("the zones = %v", list.Zones)
	}
}

// --- download ------------------------------------------------------------------

func TestFetchZoneList(t *testing.T) {
	body := "# Version 2026101900\nCOM\nORG\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			_, _ = w.Write([]byte(body))
		case "/big":
			_, _ = w.Write(bytes.Repeat([]byte("a\n"), 600*1024)) // > 1 MiB
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	data, err := fetchZoneList(context.Background(), srv.URL+"/ok")
	if err != nil {
		t.Fatalf("fetchZoneList: %v", err)
	}
	if string(data) != body {
		t.Errorf("the body = %q, want the slice content", data)
	}

	if _, err := fetchZoneList(context.Background(), srv.URL+"/404"); err == nil ||
		!strings.Contains(err.Error(), "HTTP 404") {
		t.Errorf("the 404 error = %v, want HTTP 404", err)
	}
	if _, err := fetchZoneList(context.Background(), srv.URL+"/big"); err == nil ||
		!strings.Contains(err.Error(), "exceeds the limit") {
		t.Errorf("the cap error = %v, want \"over the limit\"", err)
	}
	// An unreachable address — a download error (timeout / connection refusal).
	if _, err := fetchZoneList(context.Background(), "http://127.0.0.1:1/tlds.txt"); err == nil ||
		!strings.Contains(err.Error(), "download") {
		t.Errorf("the connection error = %v, want the download context", err)
	}
}

// TestReadZoneListFile — the offline --file path with the same size cap
// as the URL download: --file and --url are the same input.
func TestReadZoneListFile(t *testing.T) {
	dir := t.TempDir()
	small := filepath.Join(dir, "tlds.txt")
	if err := os.WriteFile(small, []byte("# Version 2026101900\nCOM\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	data, err := readZoneListFile(small)
	if err != nil {
		t.Fatalf("readZoneListFile: %v", err)
	}
	if !strings.HasPrefix(string(data), "# Version") {
		t.Errorf("the content = %q", data)
	}

	big := filepath.Join(dir, "big.txt")
	if err := os.WriteFile(big, bytes.Repeat([]byte("a\n"), 600*1024), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readZoneListFile(big); err == nil || !strings.Contains(err.Error(), "exceeds the limit") {
		t.Errorf("the cap error = %v, want \"over the limit\"", err)
	}
	if _, err := readZoneListFile(filepath.Join(dir, "nope.txt")); err == nil {
		t.Error("a nonexistent file is an error")
	}
}

// --- fake pool (db.DB) ----------------------------------------------------------

// syncPool — a db.DB without a DB: the live semantics of the zones sync
// SQL — the unnest batch insert with existing-name filtering (NOT EXISTS +
// ON CONFLICT: new zones get the next id and is_active = true, existing
// ones are not changed; RowsAffected — only inserted) and the disabling
// of the ones missing from the list (the is_active guard — a repeat
// disables 0 rows; RETURNING only of the actually disabled).
type syncPool struct {
	zones  []zoneRow
	nextID int16

	mu       sync.Mutex
	queryErr error
	execErr  error
	queries  []subCall
	execs    []subCall
}

var _ db.DB = (*syncPool)(nil)

func newSyncPool() *syncPool {
	return &syncPool{
		zones: []zoneRow{
			{id: 7, name: "crawl.test", nameUnicode: "", active: true, total: 120, checked: 80},
			{id: 9, name: "example.test", nameUnicode: "", active: false, total: 10, checked: 0},
			{id: 11, name: "xn--80adxhks", nameUnicode: "москва", active: true, total: 3, checked: 3},
		},
		nextID: 12,
	}
}

func (p *syncPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("syncPool: CopyFrom must not be called")
}

func (p *syncPool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.execs = append(p.execs, subCall{sql, append([]any(nil), args...)})
	if p.execErr != nil {
		return pgconn.CommandTag{}, p.execErr
	}
	switch sql {
	case zonesSyncInsertSQL:
		names := args[0].([]string)
		unicodes := args[1].([]string)
		inserted := 0
		for i, name := range names {
			if p.zoneIndex(name) >= 0 {
				continue // already there: NOT EXISTS filtered it before the insert
			}
			p.zones = append(p.zones, zoneRow{id: p.nextID, name: name, nameUnicode: unicodes[i], active: true})
			p.nextID++
			inserted++
		}
		return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", inserted)), nil
	default:
		return pgconn.CommandTag{}, errors.New("syncPool: unexpected SQL Exec: " + sql)
	}
}

func (p *syncPool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, subCall{sql, append([]any(nil), args...)})
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	switch sql {
	case zonesDeactivateCandidatesSQL:
		// The preview (T-311): the same predicate as the disabling
		// UPDATE, read-only — the pool state does not change; every row
		// carries the total of the active zones (the guard share).
		inFile := make(map[string]bool, len(args[0].([]string)))
		for _, n := range args[0].([]string) {
			inFile[n] = true
		}
		total := 0
		for i := range p.zones {
			if p.zones[i].active {
				total++
			}
		}
		var off [][]any
		for i := range p.zones {
			if p.zones[i].active && !inFile[p.zones[i].name] { // the is_active guard
				off = append(off, []any{p.zones[i].id, p.zones[i].name, p.zones[i].nameUnicode, int64(total)})
			}
		}
		// ORDER BY name — the SQL promise the listing relies on.
		sort.Slice(off, func(i, j int) bool { return off[i][1].(string) < off[j][1].(string) })
		// The rows — exactly the SELECT columns (id, name, name_unicode,
		// active_total): the fake mirrors the SQL, not the Scan
		// destinations — a desync fails the test here, not on the live DB.
		return statusRowsOf(off...), nil
	case zonesDeactivateMissingSQL:
		inFile := make(map[string]bool, len(args[0].([]string)))
		for _, n := range args[0].([]string) {
			inFile[n] = true
		}
		var off []zoneRow
		for i := range p.zones {
			if p.zones[i].active && !inFile[p.zones[i].name] { // the is_active guard
				p.zones[i].active = false
				off = append(off, p.zones[i])
			}
		}
		sort.Slice(off, func(i, j int) bool { return off[i].name < off[j].name })
		// The rows — exactly the RETURNING columns (id, name, name_unicode):
		// the fake mirrors the SQL, not the Scan destinations — a desync
		// fails the test here, not on the live DB.
		rows := make([][]any, len(off))
		for i, z := range off {
			rows[i] = []any{z.id, z.name, z.nameUnicode}
		}
		return statusRowsOf(rows...), nil
	default:
		return nil, errors.New("syncPool: unexpected SQL Query: " + sql)
	}
}

// zoneIndex — the index of a zone by name (called under the pool mutex).
func (p *syncPool) zoneIndex(name string) int {
	for i := range p.zones {
		if p.zones[i].name == name {
			return i
		}
	}
	return -1
}

// --- write: insert ------------------------------------------------------------

// syncSampleList — a slice over the newSyncPool seed: com/org/xn--p1ai
// are new, example.test and xn--80adxhks already exist, crawl.test is
// missing (a candidate for --deactivate-missing). Parsed by the real
// parser — which also checks the unicode→punycode→unicode-column chain.
func syncSampleList(t *testing.T) zoneListFile {
	t.Helper()
	src := "# Version 2026101900, Last Updated Sun Oct 19 07:07:01 2026 UTC\n" +
		"COM\nEXAMPLE.TEST\nXN--80ADXHKS\norg\nрф\n"
	list, err := parseZoneList([]byte(src))
	if err != nil {
		t.Fatalf("parseZoneList: %v", err)
	}
	return list
}

func TestZonesSyncRun(t *testing.T) {
	pool := newSyncPool()
	list := syncSampleList(t)

	var stdout bytes.Buffer
	if err := zonesSyncRun(context.Background(), pool, list, zonesSyncOpts{}, &stdout); err != nil {
		t.Fatalf("zonesSyncRun: %v", err)
	}
	got := stdout.String()
	for _, want := range []string{
		"zones read 5 (duplicates in the file 0), added 3, already existed 2",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the output does not contain %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "retired") {
		t.Errorf("without --deactivate-missing there must be no disabling report:\n%s", got)
	}

	// A single batch insert: the names in file order, unicode — only for
	// XN-- zones (an empty string for ASCII → NULL via nullif in SQL).
	if len(pool.execs) != 1 {
		t.Fatalf("Exec calls %d, want 1 (one insert)", len(pool.execs))
	}
	call := pool.execs[0]
	if call.sql != zonesSyncInsertSQL {
		t.Errorf("SQL = %s, want zonesSyncInsertSQL", call.sql)
	}
	if want := []string{"com", "example.test", "xn--80adxhks", "org", "xn--p1ai"}; !reflect.DeepEqual(call.args[0], want) {
		t.Errorf("the names = %v, want %v", call.args[0], want)
	}
	if want := []string{"", "", "москва", "", "рф"}; !reflect.DeepEqual(call.args[1], want) {
		t.Errorf("unicode = %v, want %v", call.args[1], want)
	}

	// New zones — enabled, with ids after the seed; existing ones are not
	// touched: example.test stays disabled, the crawl.test counters unchanged.
	for _, name := range []string{"com", "org", "xn--p1ai"} {
		i := pool.zoneIndex(name)
		if i < 0 {
			t.Fatalf("the zone %s was not inserted", name)
		}
		if !pool.zones[i].active {
			t.Errorf("the new zone %s must be enabled (spec 5.1)", name)
		}
	}
	if pool.zones[pool.zoneIndex("example.test")].active {
		t.Error("the existing disabled example.test must not be enabled")
	}
	crawl := pool.zones[pool.zoneIndex("crawl.test")]
	if !crawl.active || crawl.total != 120 || crawl.checked != 80 {
		t.Errorf("crawl.test changed: %+v — sync does not touch existing rows", crawl)
	}

	// Without the flag the DB zones missing from the slice are not
	// disabled — neither the preview nor the disabling UPDATE ran.
	for _, q := range pool.queries {
		if q.sql == zonesDeactivateMissingSQL || q.sql == zonesDeactivateCandidatesSQL {
			t.Errorf("without --deactivate-missing there must be no disabling: %v", q)
		}
	}

	// Idempotency: a repeated run with the same slice — 0 inserts, the
	// pool state unchanged.
	stdout.Reset()
	if err := zonesSyncRun(context.Background(), pool, list, zonesSyncOpts{}, &stdout); err != nil {
		t.Fatalf("a repeated zonesSyncRun: %v", err)
	}
	if !strings.Contains(stdout.String(), "added 0, already existed 5") {
		t.Errorf("the repeated run is not idempotent:\n%s", stdout.String())
	}
	if len(pool.zones) != 6 {
		t.Errorf("zones in the pool %d, want 6 (no duplicates)", len(pool.zones))
	}
}

// syncWideSample — a pool and a slice over the newSyncPool seed plus
// extra active zones present in the slice: crawl.test stays the only
// candidate, but its share of the active zones (1 of 5 = 20% in the bare
// seed after the insert) falls below the sanity guard — the "normal"
// deactivation path without the guard interference.
func syncWideSample(t *testing.T, extra int) (*syncPool, zoneListFile) {
	t.Helper()
	pool := newSyncPool()
	var b strings.Builder
	b.WriteString("# Version 2026101900, Last Updated Sun Oct 19 07:07:01 2026 UTC\n" +
		"COM\nEXAMPLE.TEST\nXN--80ADXHKS\norg\nрф\n")
	for i := 0; i < extra; i++ {
		name := fmt.Sprintf("w%02d.test", i)
		b.WriteString(strings.ToUpper(name) + "\n")
		pool.zones = append(pool.zones, zoneRow{id: pool.nextID, name: name, active: true})
		pool.nextID++
	}
	list, err := parseZoneList([]byte(b.String()))
	if err != nil {
		t.Fatalf("parseZoneList: %v", err)
	}
	return pool, list
}

// callsOf — the recorded Query calls with the given SQL, in order.
func callsOf(pool *syncPool, sql string) []subCall {
	var res []subCall
	for _, q := range pool.queries {
		if q.sql == sql {
			res = append(res, q)
		}
	}
	return res
}

// TestZonesSyncRunDeactivateMissingDryRun — --deactivate-missing without
// --apply (T-311): the candidates are listed ("would disable"), the
// disabling UPDATE is not executed, the pool state does not change.
func TestZonesSyncRunDeactivateMissingDryRun(t *testing.T) {
	pool, list := syncWideSample(t, 20) // 1 candidate of 22 active — below the guard

	var stdout bytes.Buffer
	if err := zonesSyncRun(context.Background(), pool, list,
		zonesSyncOpts{deactivateMissing: true}, &stdout); err != nil {
		t.Fatalf("zonesSyncRun --deactivate-missing: %v", err)
	}
	got := stdout.String()
	// crawl.test is in the DB but not in the slice — the candidate is
	// listed; example.test is not in the slice but is already disabled —
	// the is_active guard, it does not get into the listing.
	if !strings.Contains(got, "would disable missing from the slice (retired TLD): 1: crawl.test") {
		t.Errorf("the output does not contain the candidate list:\n%s", got)
	}
	if !strings.Contains(got, "dry-run: no zones disabled; the disabling will be done by crawlerctl zones sync --deactivate-missing --apply") {
		t.Errorf("the output does not contain the dry-run hint:\n%s", got)
	}
	if strings.Contains(got, "disabled missing from the slice") {
		t.Errorf("the dry-run reports a performed disabling:\n%s", got)
	}
	if strings.Contains(got, "sanity guard") {
		t.Errorf("below the guard threshold there must be no guard warning:\n%s", got)
	}
	// The preview SELECT ran once with the full zone list of the slice;
	// the disabling UPDATE did not run at all — crawl.test stays enabled.
	preview := callsOf(pool, zonesDeactivateCandidatesSQL)
	if len(preview) != 1 {
		t.Fatalf("preview SELECTs %d, want 1", len(preview))
	}
	if want := list.Zones; !reflect.DeepEqual(preview[0].args[0], want) {
		t.Errorf("the preview argument = %v, want the full slice list %v", preview[0].args[0], want)
	}
	if n := len(callsOf(pool, zonesDeactivateMissingSQL)); n != 0 {
		t.Errorf("disabling UPDATEs %d, want 0 (dry-run writes nothing)", n)
	}
	if i := pool.zoneIndex("crawl.test"); i < 0 || !pool.zones[i].active {
		t.Error("the dry-run must not disable crawl.test")
	}
}

// TestZonesSyncRunDeactivateMissingApply — --deactivate-missing --apply:
// the disabling UPDATE runs after the preview and disables the candidate;
// a repeated run is idempotent.
func TestZonesSyncRunDeactivateMissingApply(t *testing.T) {
	pool, list := syncWideSample(t, 20) // 1 candidate of 22 active — below the guard

	var stdout bytes.Buffer
	if err := zonesSyncRun(context.Background(), pool, list,
		zonesSyncOpts{deactivateMissing: true, apply: true}, &stdout); err != nil {
		t.Fatalf("zonesSyncRun --deactivate-missing --apply: %v", err)
	}
	got := stdout.String()
	if !strings.Contains(got, "disabled missing from the slice (retired TLD): 1: crawl.test") {
		t.Errorf("the output does not contain the disabled list:\n%s", got)
	}
	if i := pool.zoneIndex("crawl.test"); i < 0 || pool.zones[i].active {
		t.Error("crawl.test must be disabled")
	}
	// The preview SELECT runs before the disabling UPDATE with the full
	// zone list of the slice (the candidates feed the listing and the
	// guard; the UPDATE's RETURNING — the report).
	preview := callsOf(pool, zonesDeactivateCandidatesSQL)
	deact := callsOf(pool, zonesDeactivateMissingSQL)
	if len(preview) != 1 || len(deact) != 1 {
		t.Fatalf("preview SELECTs %d and disabling UPDATEs %d, want 1 and 1", len(preview), len(deact))
	}
	if want := list.Zones; !reflect.DeepEqual(deact[0].args[0], want) {
		t.Errorf("the disabling argument = %v, want the full slice list %v", deact[0].args[0], want)
	}

	// A repeated run — idempotent: nothing left to disable, the UPDATE of
	// the first run is not repeated.
	stdout.Reset()
	if err := zonesSyncRun(context.Background(), pool, list,
		zonesSyncOpts{deactivateMissing: true, apply: true}, &stdout); err != nil {
		t.Fatalf("a repeated zonesSyncRun: %v", err)
	}
	if !strings.Contains(stdout.String(), "no active zones missing from the slice") {
		t.Errorf("the repeated run does not report \"no active ones\":\n%s", stdout.String())
	}
	if n := len(callsOf(pool, zonesDeactivateMissingSQL)); n != 1 {
		t.Errorf("disabling UPDATEs after the repeat %d, want still 1 (idempotency)", n)
	}
}

// TestZonesSyncRunDeactivateGuard — the large-share sanity guard (T-311):
// the bare newSyncPool seed with the sample slice leaves 1 candidate of 5
// active zones after the insert (20% — above the 10% threshold). Without
// --apply — a warning in the dry-run; with --apply — a refusal without a
// single disabling UPDATE; --force — the bypass with the disabling
// performed.
func TestZonesSyncRunDeactivateGuard(t *testing.T) {
	list := syncSampleList(t)

	// Dry-run: the warning names the share and the bypass, nothing is
	// disabled (the candidates are still listed).
	pool := newSyncPool()
	var stdout bytes.Buffer
	if err := zonesSyncRun(context.Background(), pool, list,
		zonesSyncOpts{deactivateMissing: true}, &stdout); err != nil {
		t.Fatalf("zonesSyncRun dry-run over the guard: %v", err)
	}
	got := stdout.String()
	if !strings.Contains(got, "would disable missing from the slice (retired TLD): 1: crawl.test") {
		t.Errorf("the dry-run over the guard must still list the candidate:\n%s", got)
	}
	if !strings.Contains(got, "warning: 1 of 5 active zones (20%) — above the 10% sanity guard") {
		t.Errorf("the dry-run does not warn about the guard:\n%s", got)
	}
	if !strings.Contains(got, "dry-run: no zones disabled") {
		t.Errorf("the dry-run over the guard must still end with the dry-run line:\n%s", got)
	}
	if n := len(callsOf(pool, zonesDeactivateMissingSQL)); n != 0 {
		t.Errorf("disabling UPDATEs %d, want 0 (dry-run)", n)
	}

	// --apply without --force: a refusal, the disabling UPDATE is not
	// executed, the zone stays enabled.
	pool = newSyncPool()
	err := zonesSyncRun(context.Background(), pool, list,
		zonesSyncOpts{deactivateMissing: true, apply: true}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "above the 10% sanity guard") {
		t.Fatalf("the guard error = %v, want the sanity guard refusal", err)
	}
	if !strings.Contains(err.Error(), "repeat with --force") {
		t.Errorf("the guard error does not name the bypass:\n%v", err)
	}
	if n := len(callsOf(pool, zonesDeactivateMissingSQL)); n != 0 {
		t.Errorf("disabling UPDATEs %d, want 0 (the guard refuses before the write)", n)
	}
	if i := pool.zoneIndex("crawl.test"); i < 0 || !pool.zones[i].active {
		t.Error("the guard refusal must leave crawl.test enabled")
	}

	// --apply --force: the bypass — the disabling is performed.
	pool = newSyncPool()
	stdout.Reset()
	if err := zonesSyncRun(context.Background(), pool, list,
		zonesSyncOpts{deactivateMissing: true, apply: true, force: true}, &stdout); err != nil {
		t.Fatalf("zonesSyncRun --apply --force: %v", err)
	}
	got = stdout.String()
	if !strings.Contains(got, "continuing with --force: the large-share sanity guard is bypassed") {
		t.Errorf("the output does not note the guard bypass:\n%s", got)
	}
	if !strings.Contains(got, "disabled missing from the slice (retired TLD): 1: crawl.test") {
		t.Errorf("the output does not contain the disabled list:\n%s", got)
	}
	if i := pool.zoneIndex("crawl.test"); i < 0 || pool.zones[i].active {
		t.Error("--force must disable crawl.test")
	}
}

// TestZonesDeactivateGuardTripped — the guard boundary (the hy4 review of
// T-311, P3-4): the trigger is the STRICT > of the exact ratio — exactly
// 10% does not trip, a share above it does, however slightly; without
// candidates the verdict is false (the early-exit path of zonesSyncRun).
func TestZonesDeactivateGuardTripped(t *testing.T) {
	cases := []struct {
		candidates, activeTotal int
		want                    bool
	}{
		{0, 1, false},     // no candidates — the early-exit path
		{1, 10, false},    // exactly 10% — the strict > boundary
		{10, 100, false},  // exactly 10% — the strict > boundary
		{1, 9, true},      // 11.1%
		{11, 100, true},   // 11%
		{147, 1460, true}, // 10.07% — above, though a truncated print would say 10
	}
	for _, tc := range cases {
		if got := zonesDeactivateGuardTripped(tc.candidates, tc.activeTotal); got != tc.want {
			t.Errorf("zonesDeactivateGuardTripped(%d, %d) = %v, want %v",
				tc.candidates, tc.activeTotal, got, tc.want)
		}
	}
}

// TestZonesDeactivateGuardPercent — the displayed share is rounded UP
// (the hy4 review of T-311, P2-1): 10.07% prints as 11, never as the
// self-contradictory "10% — above the 10% sanity guard"; an exact 10%
// stays 10.
func TestZonesDeactivateGuardPercent(t *testing.T) {
	cases := []struct {
		candidates, activeTotal, want int
	}{
		{1, 10, 10},     // exactly 10%
		{10, 100, 10},   // exactly 10%
		{147, 1460, 11}, // 10.07% — the ceiling, not the truncation
		{1, 9, 12},      // 11.1% → 12
		{1, 3, 34},      // 33.3% → 34
	}
	for _, tc := range cases {
		if got := zonesDeactivateGuardPercent(tc.candidates, tc.activeTotal); got != tc.want {
			t.Errorf("zonesDeactivateGuardPercent(%d, %d) = %d, want %d",
				tc.candidates, tc.activeTotal, got, tc.want)
		}
	}
}

func TestZonesSyncRunErrors(t *testing.T) {
	list := syncSampleList(t)

	pool := newSyncPool()
	pool.execErr = errors.New("a write failure")
	err := zonesSyncRun(context.Background(), pool, list, zonesSyncOpts{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "zone insert") {
		t.Fatalf("the insert error = %v, want the \"zone insert\" context", err)
	}

	// The deactivation path reads the candidates first (T-311): a Query
	// failure surfaces with the preview context, before the UPDATE.
	pool = newSyncPool()
	pool.queryErr = errors.New("connection failure")
	err = zonesSyncRun(context.Background(), pool, list,
		zonesSyncOpts{deactivateMissing: true, apply: true}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "selecting the zones missing from the slice") {
		t.Fatalf("the preview error = %v, want the preview context", err)
	}
}

// --- commands via run -----------------------------------------------------------

// TestRunZonesSyncExecution — the zones sync execution path without
// infrastructure: the slice is read/parsed before the configuration and
// the pool, so a source failure and a junk line — code 1 without any
// configuration mention, a valid slice — a deterministic configuration
// failure (code 1); usage errors (flags, arguments) — code 2.
func TestRunZonesSyncExecution(t *testing.T) {
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	dir := t.TempDir()
	good := filepath.Join(dir, "tlds.txt")
	if err := os.WriteFile(good, []byte("# Version 2026101900, Last Updated Sun Oct 19 07:07:01 2026 UTC\nCOM\nORG\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(dir, "bad.txt")
	if err := os.WriteFile(bad, []byte("# Version 1\nCOM\n!!garbage!!\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	commentsOnly := filepath.Join(dir, "empty.txt")
	if err := os.WriteFile(commentsOnly, []byte("# a comment only\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cases := []runCase{
		{name: "sync --file — no configuration", args: []string{"zones", "sync", "--file", good},
			code: exitError, inErr: "zones sync: config:"},
		{name: "sync a junk line — before the configuration", args: []string{"zones", "sync", "--file", bad},
			code: exitError, inErr: "zones sync: line 3"},
		{name: "sync a nonexistent file", args: []string{"zones", "sync", "--file", filepath.Join(dir, "nope.txt")},
			code: exitError, inErr: "zones sync: reading"},
		{name: "sync a file without zones", args: []string{"zones", "sync", "--file", commentsOnly},
			code: exitError, inErr: "no zones in the file"},
		{name: "sync --file and --url are incompatible", args: []string{"zones", "sync", "--file", good, "--url", "http://example.invalid/tlds.txt"},
			code: exitUsage, inErr: "incompatible"},
		{name: "sync --apply without --deactivate-missing", args: []string{"zones", "sync", "--apply"},
			code: exitUsage, inErr: "--apply complements --deactivate-missing"},
		{name: "sync --force without --apply", args: []string{"zones", "sync", "--deactivate-missing", "--force"},
			code: exitUsage, inErr: "--force complements"},
		{name: "sync an extra argument", args: []string{"zones", "sync", "foo"},
			code: exitUsage, inErr: "extra arguments"},
		{name: "sync an unknown flag", args: []string{"zones", "sync", "--nope"},
			code: exitUsage, inErr: "zones sync:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tc.args, &stdout, &stderr)
			if code != tc.code {
				t.Errorf("run(%q) code = %d, want %d; stderr:\n%s", tc.args, code, tc.code, stderr.String())
			}
			if tc.inErr != "" && !strings.Contains(stderr.String(), tc.inErr) {
				t.Errorf("run(%q) stderr does not contain %q:\n%s", tc.args, tc.inErr, stderr.String())
			}
		})
	}
}

// TestRunZonesSyncURL — the --url source: a successful download reaches
// the configuration (fails in it), a source failure (HTTP 500) — code 1
// with the download context before touching the configuration.
func TestRunZonesSyncURL(t *testing.T) {
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/tlds.txt" {
			_, _ = w.Write([]byte("# Version 2026101900\nCOM\nORG\n"))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	var stdout, stderr bytes.Buffer
	code := run([]string{"zones", "sync", "--url", srv.URL + "/tlds.txt"}, &stdout, &stderr)
	if code != exitError || !strings.Contains(stderr.String(), "zones sync: config:") {
		t.Errorf("code = %d, stderr:\n%s; want a configuration refusal after a successful download", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "file version: 2026101900") {
		t.Errorf("stdout does not report the slice version:\n%s", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = run([]string{"zones", "sync", "--url", srv.URL + "/broken"}, &stdout, &stderr)
	if code != exitError || !strings.Contains(stderr.String(), "HTTP 500") {
		t.Errorf("code = %d, stderr:\n%s; want an HTTP 500 download error", code, stderr.String())
	}
	if strings.Contains(stderr.String(), "config:") {
		t.Errorf("a source failure must not reach the configuration:\n%s", stderr.String())
	}
}

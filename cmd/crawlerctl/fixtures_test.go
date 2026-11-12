// Unit tests for crawlerctl fixtures export (T-242): routing/flags/help —
// without infrastructure (a valid call reaches the configuration and
// fails deterministically, the TestRunZonesCollapseExecution style);
// quota proportions (the largest remainder method), the smoke subsample
// (determinism, names), NDJSON row encoding (the format v1 contract:
// NULL → null, bytea → base64, jsonb as-is), the duplicate collector and
// the selection SQL contracts (predicate substrings — the renderq_test.go
// style). A live export run against the dev DB is the task's self-check
// (the results go into the report), not a test.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// TestRunFixtures — command usage: help, an unknown action, flag
// validation before the DB connection (code 2), missing configuration
// after validation (code 1).
func TestRunFixtures(t *testing.T) {
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	cases := []runCase{
		{name: "fixtures without an action", args: []string{"fixtures"}, code: exitUsage, inErr: "specify an action"},
		{name: "fixtures unknown action", args: []string{"fixtures", "restore"}, code: exitUsage, inErr: `valid: export, import`},
		{name: "fixtures --help", args: []string{"fixtures", "--help"}, code: exitOK, inOut: "the render engine comparison bench fixtures"},
		{name: "fixtures help", args: []string{"fixtures", "help"}, code: exitOK, inOut: "fixtures export --out"},
		{name: "export --help", args: []string{"fixtures", "export", "--help"}, code: exitOK, inOut: "--seed N"},
		{name: "import --help", args: []string{"fixtures", "import", "--help"}, code: exitOK, inOut: "--publish MODE"},
		{name: "import --help mentions --force", args: []string{"fixtures", "import", "--help"}, code: exitOK, inOut: "--force"},
		{name: "import help", args: []string{"fixtures", "import", "help"}, code: exitOK, inOut: "fixtures import --dir"},
		{name: "export without --out", args: []string{"fixtures", "export"}, code: exitUsage, inErr: "specify the dump directory"},
		{name: "export --out empty", args: []string{"fixtures", "export", "--out", "  "}, code: exitUsage, inErr: "specify the dump directory"},
		{name: "export an extra argument", args: []string{"fixtures", "export", "--out", "d", "stray"}, code: exitUsage, inErr: "extra arguments"},
		{name: "export a negative quota", args: []string{"fixtures", "export", "--out", "d", "--ok", "-1"},
			code: exitUsage, inErr: "--ok"},
		{name: "export zero strata", args: []string{"fixtures", "export", "--out", "d", "--ok", "0", "--failed", "0", "--heavy", "0"},
			code: exitUsage, inErr: "the stratum quota sum is zero"},
		{name: "export weeks 0", args: []string{"fixtures", "export", "--out", "d", "--weeks", "0"},
			code: exitUsage, inErr: "--weeks"},
		{name: "export valid — no configuration", args: []string{"fixtures", "export", "--out", "d"},
			code: exitError, inErr: "fixtures export:"},
		{name: "import without --dir", args: []string{"fixtures", "import"}, code: exitUsage, inErr: "specify the dump directory"},
		{name: "import --dir empty", args: []string{"fixtures", "import", "--dir", " "}, code: exitUsage, inErr: "specify the dump directory"},
		{name: "import --publish junk", args: []string{"fixtures", "import", "--dir", "d", "--publish", "some"},
			code: exitUsage, inErr: "--publish"},
		{name: "import --publish without --force", args: []string{"fixtures", "import", "--dir", "d", "--publish", "all"},
			code: exitUsage, inErr: "--publish complements --force"},
		{name: "import an extra argument", args: []string{"fixtures", "import", "--dir", "d", "stray"},
			code: exitUsage, inErr: "extra arguments"},
		{name: "import valid — no configuration", args: []string{"fixtures", "import", "--dir", "d"},
			code: exitError, inErr: "fixtures import:"},
		{name: "import valid with publishing — no configuration", args: []string{"fixtures", "import", "--dir", "d", "--force", "--publish", "smoke"},
			code: exitError, inErr: "fixtures import:"},
		{name: "the top-level help mentions fixtures", args: []string{"--help"},
			code: exitOK, inOut: "fixtures export --out"},
		{name: "the top-level help mentions fixtures import", args: []string{"--help"},
			code: exitOK, inOut: "fixtures import --dir"},
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
			if tc.inOut != "" && !strings.Contains(stdout.String(), tc.inOut) {
				t.Errorf("run(%q) stdout does not contain %q:\n%s", tc.args, tc.inOut, stdout.String())
			}
		})
	}
}

// TestFixturesAllocateQuotas — the largest remainder method: exact
// shares, a remainder top-up, determinism on equal shares, degenerate
// cases.
func TestFixturesAllocateQuotas(t *testing.T) {
	t.Run("exact shares without a remainder", func(t *testing.T) {
		got := fixturesAllocateQuotas(map[int16]int64{1: 100, 2: 300}, 8)
		want := map[int16]int{1: 2, 2: 6}
		if len(got) != len(want) {
			t.Fatalf("quotas = %v, want %v", got, want)
		}
		for z, q := range want {
			if got[z] != q {
				t.Errorf("zone %d: quota %d, want %d (all: %v)", z, got[z], q, got)
			}
		}
	})

	t.Run("the top-up by the largest remainder", func(t *testing.T) {
		// 100/(100+100+100+1)*10: bases 2,2,2,0; a remainder of 2 — zones
		// with a larger fractional part (the last one: 0.098) and, on a
		// tie, the smaller zone_id.
		got := fixturesAllocateQuotas(map[int16]int64{1: 100, 2: 100, 3: 100, 4: 1}, 10)
		if total := got[1] + got[2] + got[3] + got[4]; total != 10 {
			t.Fatalf("the quota sum = %d, want 10 (%v)", total, got)
		}
		if got[4] > 1 {
			t.Errorf("the zone with 1 row got the quota %d (> available)", got[4])
		}
		// Three equal zones split 10 as 4/3/3 (+1 to the smallest id).
		if got[1] != 4 || got[2] != 3 || got[3] != 3 {
			t.Errorf("equal zones: quotas %d/%d/%d, want 4/3/3", got[1], got[2], got[3])
		}
	})

	t.Run("a zone's quota is not above what is available", func(t *testing.T) {
		got := fixturesAllocateQuotas(map[int16]int64{7: 3}, 100)
		if got[7] != 3 {
			t.Errorf("quota = %d, want 3 (3 available)", got[7])
		}
	})

	t.Run("degenerate", func(t *testing.T) {
		if got := fixturesAllocateQuotas(nil, 10); len(got) != 0 {
			t.Errorf("empty counts: %v", got)
		}
		if got := fixturesAllocateQuotas(map[int16]int64{1: 5}, 0); len(got) != 0 {
			t.Errorf("a zero total: %v", got)
		}
		if got := fixturesAllocateQuotas(map[int16]int64{1: 0, 2: 4}, 4); len(got) != 1 || got[2] != 4 {
			t.Errorf("a zero zone does not participate: %v", got)
		}
	})

	t.Run("the sum equals total", func(t *testing.T) {
		counts := map[int16]int64{1: 7, 2: 13, 3: 31, 4: 5, 5: 44}
		for _, total := range []int{1, 2, 7, 33, 100, 1000} {
			got := fixturesAllocateQuotas(counts, total)
			sum := 0
			for _, q := range got {
				sum += q
			}
			available := 0
			for _, n := range counts {
				available += int(n)
			}
			want := total
			if want > available {
				want = available
			}
			if sum != want {
				t.Errorf("total %d: the quota sum %d, want %d (%v)", total, sum, want, got)
			}
			for z, q := range got {
				if q > int(counts[z]) {
					t.Errorf("total %d: zone %d: quota %d > available %d", total, z, q, counts[z])
				}
			}
		}
	})
}

// TestFixturesCollect — the selection collector: the stratum label,
// duplicate key cutting (a row never enters the dump twice — the import
// idempotency).
func TestFixturesCollect(t *testing.T) {
	c := newFixturesCollect()
	c.add("ok", []fixturesResult{
		{ZoneID: 1, DomainID: 1, SubdomainID: 0, Variant: 0},
		{ZoneID: 1, DomainID: 1, SubdomainID: 1, Variant: 0},
	})
	c.add("heavy", []fixturesResult{
		{ZoneID: 1, DomainID: 1, SubdomainID: 0, Variant: 0}, // an ok duplicate
		{ZoneID: 1, DomainID: 2, SubdomainID: 0, Variant: 0},
	})
	if len(c.rows) != 3 {
		t.Fatalf("rows %d, want 3", len(c.rows))
	}
	if c.dups != 1 {
		t.Errorf("duplicates %d, want 1", c.dups)
	}
	for i, want := range []string{"ok", "ok", "heavy"} {
		if c.rows[i].Stratum != want {
			t.Errorf("row %d: stratum %q, want %q", i, c.rows[i].Stratum, want)
		}
	}
}

// TestFixturesEncodeAll — the format v1 NDJSON contract: one object per
// line, NULL columns → null, bytea → base64, jsonb → as-is, without HTML
// escaping, time → RFC 3339.
func TestFixturesEncodeAll(t *testing.T) {
	ts := time.Date(2026, 11, 5, 12, 30, 0, 0, time.UTC)
	httpStatus := int16(200)
	url := "https://example.com/?a=1&b=<2>"
	rows := []fixturesResult{{
		ZoneID: 7, DomainID: 42, SubdomainID: 1, Variant: 1,
		Scheme: 2, HTTPStatus: &httpStatus, FinalURL: &url,
		Headers:     json.RawMessage(`{"content-type":["text/html"]}`),
		Cookies:     nil,
		Rendered:    true,
		ContentHash: []byte{0x01, 0x02, 0x03}, // base64: AQID
		CheckedAt:   ts,
		Stratum:     "ok",
	}}

	var buf bytes.Buffer
	if err := fixturesEncodeAll(&buf, rows); err != nil {
		t.Fatalf("encoding: %v", err)
	}
	line := strings.TrimRight(buf.String(), "\n")
	if strings.Count(line, "\n") != 0 {
		t.Errorf("one NDJSON line was expected, got:\n%s", line)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("the line is not JSON: %v\n%s", err, line)
	}

	// bytea → base64
	if m["content_hash"] != "AQID" {
		t.Errorf("content_hash = %v, want AQID", m["content_hash"])
	}
	// jsonb → nested JSON; NULL jsonb → null
	if h, ok := m["headers"].(map[string]any); !ok || h["content-type"] == nil {
		t.Errorf("headers is not nested JSON: %v", m["headers"])
	}
	if m["cookies"] != nil {
		t.Errorf("cookies = %v, want null", m["cookies"])
	}
	// NULL columns → null (not a missing field)
	if m["render_failed_at"] != nil {
		t.Errorf("render_failed_at = %v, want null", m["render_failed_at"])
	}
	// without HTML escaping: & and <> from final_url — as-is
	if !strings.Contains(line, "&b=<2>") {
		t.Errorf("final_url is HTML-escaped in NDJSON:\n%s", line)
	}
	// time → RFC 3339
	if m["checked_at"] != "2026-11-05T12:30:00Z" {
		t.Errorf("checked_at = %v, want RFC 3339", m["checked_at"])
	}
	if m["stratum"] != "ok" {
		t.Errorf("stratum = %v, want ok", m["stratum"])
	}

	// A NULL row: all optional fields are absent.
	var nullLine bytes.Buffer
	if err := fixturesEncodeAll(&nullLine, []fixturesResult{{ZoneID: 1, DomainID: 2, SubdomainID: 0, Variant: 0, Scheme: 1, CheckedAt: ts, Stratum: "failed"}}); err != nil {
		t.Fatalf("encoding a NULL row: %v", err)
	}
	var nm map[string]any
	if err := json.Unmarshal(nullLine.Bytes(), &nm); err != nil {
		t.Fatalf("the NULL row is not JSON: %v", err)
	}
	for _, col := range []string{"http_status", "final_url", "alias_subdomain_id", "headers", "cookies", "content_hash", "error", "response_ms", "rules_version", "render_failed_at"} {
		v, ok := nm[col]
		if !ok {
			t.Errorf("the column %s is missing from the dump (the contract: null, not absence)", col)
		} else if v != nil {
			t.Errorf("the column %s = %v, want null", col, v)
		}
	}
}

// TestFixturesDomainNDJSON — a domains row of the dump: NULL columns →
// null, all table columns present (the T-243 import contract).
func TestFixturesDomainNDJSON(t *testing.T) {
	var buf bytes.Buffer
	if err := fixturesEncodeAll(&buf, []fixturesDomain{{ZoneID: 3, ID: 9, Label: "example", Status: 2, CheckState: 2, Flags: 1, DNSMask: 5, TempAttempts: 0}}); err != nil {
		t.Fatalf("encoding: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("the line is not JSON: %v", err)
	}
	for _, col := range []string{"zone_id", "id", "label", "status", "check_state", "flags", "dns_mask", "queued_at", "last_checked_at", "temp_attempts"} {
		if _, ok := m[col]; !ok {
			t.Errorf("the column %s is missing from domains.ndjson", col)
		}
	}
	if m["queued_at"] != nil || m["last_checked_at"] != nil {
		t.Errorf("the domain NULL columns = %v/%v, want null", m["queued_at"], m["last_checked_at"])
	}
}

// TestFixturesSmokePick — the smoke subsample: determinism (the same
// seed — the same order), the limit, the label.zone name, robustness to
// the row order.
func TestFixturesSmokePick(t *testing.T) {
	rows := []fixturesResult{
		{ZoneID: 7, DomainID: 1, SubdomainID: 0, Variant: 0},
		{ZoneID: 7, DomainID: 1, SubdomainID: 1, Variant: 0}, // the same domain
		{ZoneID: 7, DomainID: 2, SubdomainID: 0, Variant: 0},
		{ZoneID: 3, DomainID: 5, SubdomainID: 0, Variant: 0},
	}
	domains := []fixturesDomain{
		{ZoneID: 7, ID: 1, Label: "alpha"},
		{ZoneID: 7, ID: 2, Label: "beta"},
		{ZoneID: 3, ID: 5, Label: "gamma"},
	}
	zones := []fixturesZone{{ID: 7, Name: "su"}, {ID: 3, Name: "ru"}}

	a := fixturesSmokePick(rows, domains, zones, "242", 2)
	b := fixturesSmokePick(rows, domains, zones, "242", 2)
	if len(a) != 2 || len(b) != 2 {
		t.Fatalf("the smoke size %d/%d, want 2", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Errorf("non-determinism: %v vs %v", a[i], b[i])
		}
		if !strings.HasSuffix(a[i].Name, ".su") && !strings.HasSuffix(a[i].Name, ".ru") {
			t.Errorf("the smoke domain name %q without a zone", a[i].Name)
		}
	}

	// All domains in the selection are unique.
	seen := map[fixturesDomainKey]bool{}
	for _, s := range a {
		k := fixturesDomainKey{s.ZoneID, s.DomainID}
		if seen[k] {
			t.Errorf("the domain %d/%d twice in smoke", s.ZoneID, s.DomainID)
		}
		seen[k] = true
	}

	// A limit above the unique domains — all of them; zero — empty.
	if got := fixturesSmokePick(rows, domains, zones, "242", 100); len(got) != 3 {
		t.Errorf("the limit 100: %d domains, want 3 (unique)", len(got))
	}
	if got := fixturesSmokePick(rows, domains, zones, "242", 0); got != nil {
		t.Errorf("the limit 0: %v, want nil", got)
	}
}

// TestFixturesSQLContract — the selection SQL contracts: the base
// "real q:render task" predicate in all strata, the failed freshness,
// the heavy content size, the ok anti-join against heavy, the
// deterministic md5 order, the key identifiers in the domain selection.
func TestFixturesSQLContract(t *testing.T) {
	base := "final_url IS NOT NULL"
	for name, sql := range map[string]string{
		"countOk":      fixturesCountOkSQL,
		"countFailed":  fixturesCountFailedSQL,
		"countHeavy":   fixturesCountHeavySQL,
		"selectOk":     fixturesSelectOkSQL,
		"selectHeavy":  fixturesSelectHeavySQL,
		"selectFailed": fixturesSelectFailedSQL,
	} {
		if !strings.Contains(sql, base) {
			t.Errorf("%s: the base predicate %q is missing: %s", name, base, sql)
		}
		if !strings.HasPrefix(sql, "SELECT") {
			t.Errorf("%s: not a SELECT: %s", name, sql)
		}
	}
	if !strings.Contains(fixturesCountOkSQL, "rendered") ||
		strings.Contains(fixturesCountOkSQL, "render_failed_at") {
		t.Errorf("countOk: the ok stratum = rendered without the marker: %s", fixturesCountOkSQL)
	}
	if !strings.Contains(fixturesCountFailedSQL, "render_failed_at IS NOT NULL") ||
		!strings.Contains(fixturesCountFailedSQL, "render_failed_at >= now() - make_interval(weeks => $1::int)") {
		t.Errorf("countFailed: the freshness marker/window (by render_failed_at, not checked_at) are lost: %s", fixturesCountFailedSQL)
	}
	if !strings.Contains(fixturesSelectFailedSQL, "r.render_failed_at >= now() - make_interval(weeks => $2::int)") {
		t.Errorf("selectFailed: the freshness window is not by the render_failed_at marker: %s", fixturesSelectFailedSQL)
	}
	if !strings.Contains(fixturesSelectHeavySQL, "JOIN contents AS c ON c.hash = r.content_hash") ||
		!strings.Contains(fixturesSelectHeavySQL, "ORDER BY c.raw_len DESC") {
		t.Errorf("selectHeavy: the content size is not the priority: %s", fixturesSelectHeavySQL)
	}
	if !strings.Contains(fixturesSelectOkSQL, "AS ex(zone_id, domain_id, subdomain_id, variant)") ||
		!strings.Contains(fixturesSelectOkSQL, "ex.zone_id IS NULL") {
		t.Errorf("selectOk: the anti-join against heavy is lost: %s", fixturesSelectOkSQL)
	}
	// The deterministic order — in all per-zone selections.
	for name, sql := range map[string]string{
		"ok": fixturesSelectOkSQL, "heavy": fixturesSelectHeavySQL, "failed": fixturesSelectFailedSQL,
	} {
		if !strings.Contains(sql, "ORDER BY") || !strings.Contains(sql, "md5($") {
			t.Errorf("%s: the deterministic md5 order is missing: %s", name, sql)
		}
		if !strings.Contains(sql, "LIMIT $") {
			t.Errorf("%s: the quota LIMIT is missing: %s", name, sql)
		}
	}
	if !strings.Contains(fixturesDomainsSQL, "SELECT DISTINCT k.zone_id, k.domain_id") ||
		!strings.Contains(fixturesDomainsSQL, "unnest($1::int2[], $2::int8[])") {
		t.Errorf("domainsSQL: the domain keys are not unnest arrays: %s", fixturesDomainsSQL)
	}
	if !strings.Contains(fixturesZonesSQL, "id = ANY($1::int2[])") {
		t.Errorf("zonesSQL: the zones are not an id array: %s", fixturesZonesSQL)
	}
	// Selection columns: the format contract (no addrs/cname/tls/redirects).
	for _, banned := range []string{"r.addrs", "r.cname", "r.tls", "r.redirects"} {
		if strings.Contains(fixturesResultCols, banned) {
			t.Errorf("fixturesResultCols: the de-identifiable column %s in the selection", banned)
		}
	}
	for _, need := range []string{"r.final_url", "r.headers", "r.cookies", "r.alias_kind", "r.render_failed_at", "r.content_hash"} {
		if !strings.Contains(fixturesResultCols, need) {
			t.Errorf("fixturesResultCols: the render contract column %s is missing", need)
		}
	}
}

// TestFixturesWriteFile — an atomic write: the file appears under the
// final name, no .tmp remains; a write error leaves no half-file.
func TestFixturesWriteFile(t *testing.T) {
	dir := t.TempDir()
	write := func(s string) error {
		return fixturesWriteFile(dir, "a.ndjson", func(w *bufio.Writer) error {
			_, err := w.WriteString(s)
			return err
		})
	}
	if err := write("line 1\n"); err != nil {
		t.Fatalf("writing: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(dir, "a.ndjson")); err != nil || string(b) != "line 1\n" {
		t.Fatalf("the file after writing: %q, %v", b, err)
	}
	if fi, err := os.Stat(filepath.Join(dir, "a.ndjson")); err == nil && fi.Mode().Perm() != 0o600 {
		t.Errorf("the file mode %v, want 0600 (the dump is private)", fi.Mode().Perm())
	}
	if _, err := os.Stat(filepath.Join(dir, "a.ndjson.tmp")); !os.IsNotExist(err) {
		t.Errorf(".tmp remained after a successful write: %v", err)
	}

	// A write function error: no half-file under the final name appears,
	// the old content is not damaged (no rename is performed).
	fail := fixturesWriteFile(dir, "a.ndjson", func(w *bufio.Writer) error {
		_, _ = w.WriteString("junk")
		return errors.New("interrupted")
	})
	if fail == nil {
		t.Fatal("the write error was not returned")
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "a.ndjson")); string(b) != "line 1\n" {
		t.Errorf("the content corrupted by the interrupted write: %q", b)
	}
	if _, err := os.Stat(filepath.Join(dir, "a.ndjson.tmp")); !os.IsNotExist(err) {
		t.Errorf(".tmp remained after the interrupted write: %v", err)
	}
}

// ---------------------------------------------------------------- orchestration

// fixturesRows — pgx.Rows with prepared fixture selection rows (the
// dest types are the fixtures* field types, including NULL column
// pointers; the shared statusRows does not support them — a local set
// here, the T-079 style).
type fixturesRows struct {
	rows [][]any
	i    int
}

func (r *fixturesRows) Close()                                       {}
func (r *fixturesRows) Err() error                                   { return nil }
func (r *fixturesRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fixturesRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fixturesRows) Values() ([]any, error)                       { return r.rows[r.i-1], nil }
func (r *fixturesRows) RawValues() [][]byte                          { return nil }
func (r *fixturesRows) Conn() *pgx.Conn                              { return nil }
func (r *fixturesRows) TypeMap() *pgtype.Map                         { return nil }

func (r *fixturesRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *fixturesRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("fixturesRows: Scan without Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("fixturesRows: %d destinations, %d columns", len(dest), len(row))
	}
	set := func(d, v any) error {
		switch dst := d.(type) {
		case *int16:
			*dst = v.(int16)
		case *int32:
			*dst = v.(int32)
		case *int64:
			*dst = v.(int64)
		case *string:
			*dst = v.(string)
		case *bool:
			*dst = v.(bool)
		case *[]byte:
			if v == nil {
				*dst = nil
			} else {
				*dst = v.([]byte)
			}
		case *json.RawMessage:
			if v == nil {
				*dst = nil
			} else {
				*dst = json.RawMessage(v.([]byte))
			}
		case *time.Time:
			*dst = v.(time.Time)
		case **int16:
			if v == nil {
				*dst = nil
			} else {
				x := v.(int16)
				*dst = &x
			}
		case **int32:
			if v == nil {
				*dst = nil
			} else {
				x := v.(int32)
				*dst = &x
			}
		case **string:
			if v == nil {
				*dst = nil
			} else {
				x := v.(string)
				*dst = &x
			}
		case **time.Time:
			if v == nil {
				*dst = nil
			} else {
				x := v.(time.Time)
				*dst = &x
			}
		default:
			return fmt.Errorf("fixturesRows: the destination %T is not supported", d)
		}
		return nil
	}
	for i, d := range dest {
		if err := set(d, row[i]); err != nil {
			return err
		}
	}
	return nil
}

// fixturesPool — a fake db.DB: routing by the fixture selection SQL
// markers. The scenario: zone 7 (su) with two ok rows, the other strata
// empty (degradation), one domain and one zone.
type fixturesPool struct {
	failOn string // an SQL substring: return an error (a DB failure simulation)
}

func (p *fixturesPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("fixturesPool: CopyFrom is not called")
}

func (p *fixturesPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("fixturesPool: Exec is not called (the export is SELECT-only)")
}

func (p *fixturesPool) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	if p.failOn != "" && strings.Contains(sql, p.failOn) {
		return nil, fmt.Errorf("a fake DB failure (%s)", p.failOn)
	}
	ts := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	switch {
	case strings.Contains(sql, "LIMIT $7::int8"): // select ok
		url, hdr := "https://ok.example/", []byte(`{"a":["b"]}`)
		return &fixturesRows{rows: [][]any{
			{int16(7), int64(1), int16(0), int16(0), int16(2), nil, url, int16(0), nil,
				hdr, nil, true, []byte{1, 2}, nil, nil, ts, nil, nil},
			{int16(7), int64(1), int16(1), int16(0), int16(2), nil, url, int16(0), nil,
				hdr, nil, true, nil, nil, nil, ts, nil, nil},
		}}, nil
	case strings.Contains(sql, "count(*)") && strings.Contains(sql, "rendered"): // count ok
		return &fixturesRows{rows: [][]any{{int16(7), int64(2)}}}, nil
	case strings.Contains(sql, "count(*)"): // count heavy/failed — empty
		return &fixturesRows{}, nil
	case strings.Contains(sql, "LIMIT $3::int8"), strings.Contains(sql, "LIMIT $4::int8"): // heavy/failed
		return &fixturesRows{}, nil
	case strings.Contains(sql, "FROM domains AS d"):
		return &fixturesRows{rows: [][]any{{int16(7), int64(1), "ok", int16(4), int16(2),
			int16(0), int64(3), nil, nil, int16(0)}}}, nil
	case strings.Contains(sql, "FROM zones WHERE"):
		return &fixturesRows{rows: [][]any{{int16(7), "su", nil, true, int64(2), int64(2), nil}}}, nil
	default:
		return nil, fmt.Errorf("fixturesPool: unknown SQL: %s", sql)
	}
}

// TestFixturesExportRun — the export orchestration on a fake pool: all
// five files, the manifest consistent with the rows, honest
// degradations; a rerun over the old manifest overwrites it; a failure
// before writing the files keeps the old manifest, a failure mid-write
// removes it (the "manifest present ⇒ dump complete" contract, review
// T-242 P1).
func TestFixturesExportRun(t *testing.T) {
	params := fixturesParams{OK: 5, Heavy: 5, Failed: 5, Smoke: 1, Weeks: 4, Seed: 242}

	t.Run("success: the files and the manifest", func(t *testing.T) {
		dir := t.TempDir()
		params.OutDir = dir
		if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte("old"), 0o600); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if _, err := fixturesExportRun(context.Background(), &fixturesPool{}, params, &out); err != nil {
			t.Fatalf("the export: %v", err)
		}
		// All dump files in place, the old manifest replaced.
		manifest := filepath.Join(dir, "manifest.json")
		b, err := os.ReadFile(manifest)
		if err != nil {
			t.Fatalf("the manifest: %v", err)
		}
		if string(b) == "old" {
			t.Error("the previous manifest was not overwritten")
		}
		var m fixturesManifest
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("the manifest is not JSON: %v", err)
		}
		if m.FormatVersion != 1 || m.SmokeDomains != 1 || m.Seed != 242 {
			t.Errorf("the manifest: %+v", m)
		}
		if m.Strata["ok"].Taken != 2 || !m.Strata["ok"].Degraded || m.Strata["ok"].Available != 2 {
			t.Errorf("the ok stratum: %+v (want: 2 available, 2 taken, degraded)", m.Strata["ok"])
		}
		for _, s := range []string{"failed", "heavy"} {
			if m.Strata[s].Taken != 0 || !m.Strata[s].Degraded {
				t.Errorf("the %s stratum: %+v (want empty with degradation)", s, m.Strata[s])
			}
		}
		// File/manifest counter consistency.
		for name, want := range map[string]int{
			"zones.ndjson": 1, "domains.ndjson": 1, "subdomain_results.ndjson": 2, "smoke.ndjson": 1,
		} {
			lines, err := readLines(filepath.Join(dir, name))
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if len(lines) != want || m.Files[name] != want {
				t.Errorf("%s: %d rows (the manifest %d), want %d", name, len(lines), m.Files[name], want)
			}
		}
		if !strings.Contains(out.String(), "manifest.json: ok") {
			t.Errorf("the output without the manifest line:\n%s", out.String())
		}
		if strings.Contains(out.String(), "manifest.json: 0") {
			t.Errorf("the output with \"manifest.json: 0 rows\":\n%s", out.String())
		}
		// The smoke domain — from the selection, with the label.zone name.
		smokeLine, err := os.ReadFile(filepath.Join(dir, "smoke.ndjson"))
		if err != nil || !strings.Contains(string(smokeLine), `"name":"ok.su"`) {
			t.Errorf("smoke.ndjson: %s (%v)", smokeLine, err)
		}
	})

	t.Run("a DB failure before writing — the previous manifest alive", func(t *testing.T) {
		dir := t.TempDir()
		params.OutDir = dir
		if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte("old-full"), 0o600); err != nil {
			t.Fatal(err)
		}
		pool := &fixturesPool{failOn: "FROM zones WHERE"} // the last query, before writing files
		var out bytes.Buffer
		if _, err := fixturesExportRun(context.Background(), pool, params, &out); err == nil {
			t.Fatal("the DB failure was not returned as an error")
		}
		if b, _ := os.ReadFile(filepath.Join(dir, "manifest.json")); string(b) != "old-full" {
			t.Errorf("the previous manifest corrupted by the failure before writing: %q", b)
		}
	})

	t.Run("a failure mid-write — the manifest removed", func(t *testing.T) {
		dir := t.TempDir()
		params.OutDir = dir
		if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte("old-full"), 0o600); err != nil {
			t.Fatal(err)
		}
		// zones.ndjson — a directory: writing .tmp succeeds, renaming over
		// a directory fails — a failure exactly between removing the
		// manifest and writing the new one.
		if err := os.Mkdir(filepath.Join(dir, "zones.ndjson"), 0o700); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if _, err := fixturesExportRun(context.Background(), &fixturesPool{}, params, &out); err == nil {
			t.Fatal("the write failure was not returned as an error")
		}
		if _, err := os.Stat(filepath.Join(dir, "manifest.json")); !os.IsNotExist(err) {
			t.Errorf("an incomplete dump with a manifest (the \"manifest present ⇒ complete\" contract): %v", err)
		}
	})
}

// readLines — the file's non-empty lines (for the dump file counters).
func readLines(path string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, l := range strings.Split(string(b), "\n") {
		if l != "" {
			out = append(out, l)
		}
	}
	return out, nil
}

// Unit tests for crawlerctl fixtures import (T-243): routing/flags live
// in TestRunFixtures (fixtures_test.go); here — format v1 dump reading
// and validation (the manifest, duplicate keys, referential integrity),
// the import SQL contracts (OVERRIDING SYSTEM VALUE, ON CONFLICT by PK,
// setval above the maximum, natural-key pre-checks) on a fake pool
// (re-import idempotency — ON CONFLICT behavior; a live run against the
// dev DB is the task's self-check, not a test) and q:render publishing
// on a fake queue (all/smoke modes, dedup, counters, label.zone names).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/queue"
)

// ---------------------------------------------------------------- dump

// writeDumpDir — a format v1 dump directory built from rows
// (fixturesEncodeAll — the same encoder as export: the test files are
// read back by the same reader).
func writeDumpDir(t *testing.T, dir string, manifest *fixturesManifest, zones []fixturesZone,
	domains []fixturesDomain, results []fixturesResult, smoke []fixturesSmokeDomain) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	write := func(name string, rows any) {
		var buf bytes.Buffer
		switch v := rows.(type) {
		case []fixturesZone:
			if err := fixturesEncodeAll(&buf, v); err != nil {
				t.Fatal(err)
			}
		case []fixturesDomain:
			if err := fixturesEncodeAll(&buf, v); err != nil {
				t.Fatal(err)
			}
		case []fixturesResult:
			if err := fixturesEncodeAll(&buf, v); err != nil {
				t.Fatal(err)
			}
		case []fixturesSmokeDomain:
			if err := fixturesEncodeAll(&buf, v); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(dir, name), buf.Bytes(), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("zones.ndjson", zones)
	write("domains.ndjson", domains)
	write("subdomain_results.ndjson", results)
	write("smoke.ndjson", smoke)
	if manifest == nil {
		manifest = &fixturesManifest{FormatVersion: fixturesFormatVersion}
	}
	if manifest.Files == nil {
		manifest.Files = map[string]int{
			"zones.ndjson":             len(zones),
			"domains.ndjson":           len(domains),
			"subdomain_results.ndjson": len(results),
			"smoke.ndjson":             len(smoke),
		}
	}
	if manifest.FormatVersion == 0 {
		manifest.FormatVersion = fixturesFormatVersion
	}
	b, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// importDumpFixture — a consistent mini-dump: the su zone (id 7), a
// domain, two result rows (ok and heavy) and one smoke domain.
func importDumpFixture() (zones []fixturesZone, domains []fixturesDomain, results []fixturesResult, smoke []fixturesSmokeDomain) {
	ts := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	url := "https://ok.example/"
	zones = []fixturesZone{{ID: 7, Name: "su", IsActive: true, DomainsTotal: 10, DomainsChecked: 4}}
	domains = []fixturesDomain{{ZoneID: 7, ID: 42, Label: "ok", Status: 2, CheckState: 2, DNSMask: 3}}
	results = []fixturesResult{
		{ZoneID: 7, DomainID: 42, SubdomainID: 0, Variant: 0, Scheme: 2, FinalURL: &url,
			Headers: json.RawMessage(`{"a":["b"]}`), Rendered: true, CheckedAt: ts, Stratum: "ok"},
		{ZoneID: 7, DomainID: 42, SubdomainID: 1, Variant: 0, Scheme: 2, FinalURL: &url,
			Rendered: true, ContentHash: []byte{1, 2}, CheckedAt: ts, Stratum: "heavy"},
	}
	smoke = []fixturesSmokeDomain{{ZoneID: 7, DomainID: 42, Name: "ok.su"}}
	return
}

// TestFixturesReadDump — format v1 dump reading: completeness (the
// manifest is the completion marker), the format version, the files,
// duplicate keys (the last entry wins), referential integrity.
func TestFixturesReadDump(t *testing.T) {
	zones, domains, results, smoke := importDumpFixture()

	t.Run("the full dump", func(t *testing.T) {
		dir := t.TempDir()
		writeDumpDir(t, dir, &fixturesManifest{FormatVersion: 1, Seed: 242}, zones, domains, results, smoke)
		dump, err := fixturesReadDump(dir)
		if err != nil {
			t.Fatalf("reading: %v", err)
		}
		if len(dump.Zones) != 1 || len(dump.Domains) != 1 || len(dump.Results) != 2 || len(dump.Smoke) != 1 {
			t.Fatalf("the dump counters: zones %d, domains %d, results %d, smoke %d",
				len(dump.Zones), len(dump.Domains), len(dump.Results), len(dump.Smoke))
		}
		if dump.Manifest.Seed != 242 {
			t.Errorf("the manifest was not read: %+v", dump.Manifest)
		}
		if dump.Results[0].Stratum != "ok" || dump.Results[0].Headers == nil {
			t.Errorf("a result row lost fields: %+v", dump.Results[0])
		}
	})

	t.Run("no manifest — the dump is incomplete", func(t *testing.T) {
		dir := t.TempDir()
		writeDumpDir(t, dir, nil, zones, domains, results, smoke)
		if err := os.Remove(filepath.Join(dir, "manifest.json")); err != nil {
			t.Fatal(err)
		}
		_, err := fixturesReadDump(dir)
		if err == nil || !strings.Contains(err.Error(), "incomplete") {
			t.Errorf("the error without a manifest = %v, want \"the dump is incomplete\"", err)
		}
	})

	t.Run("a format version from the future", func(t *testing.T) {
		dir := t.TempDir()
		writeDumpDir(t, dir, &fixturesManifest{FormatVersion: fixturesFormatVersion + 1}, zones, domains, results, smoke)
		_, err := fixturesReadDump(dir)
		if err == nil || !strings.Contains(err.Error(), "format v2 is not supported") {
			t.Errorf("the version error = %v, want a version refusal", err)
		}
	})

	t.Run("the manifest counter does not match — the dump is truncated", func(t *testing.T) {
		dir := t.TempDir()
		writeDumpDir(t, dir, &fixturesManifest{FormatVersion: fixturesFormatVersion,
			Files: map[string]int{"zones.ndjson": 1, "domains.ndjson": 1,
				"subdomain_results.ndjson": len(results) + 5, "smoke.ndjson": len(smoke)}}, zones, domains, results, smoke)
		_, err := fixturesReadDump(dir)
		if err == nil || !strings.Contains(err.Error(), "the manifest counted") {
			t.Errorf("the manifest cross-check error = %v, want a row counter refusal", err)
		}
	})

	t.Run("the manifest without a file counter", func(t *testing.T) {
		dir := t.TempDir()
		writeDumpDir(t, dir, &fixturesManifest{FormatVersion: fixturesFormatVersion,
			Files: map[string]int{"zones.ndjson": 1}}, zones, domains, results, smoke)
		_, err := fixturesReadDump(dir)
		if err == nil || !strings.Contains(err.Error(), "no row counter for domains.ndjson") {
			t.Errorf("the missing counter error = %v", err)
		}
	})

	t.Run("duplicate natural keys inside the dump — an error before the DB", func(t *testing.T) {
		dir := t.TempDir()
		dupZones := []fixturesZone{zones[0], {ID: zones[0].ID + 1, Name: zones[0].Name}}
		writeDumpDir(t, dir, nil, dupZones, domains, results, smoke)
		if _, err := fixturesReadDump(dir); err == nil || !strings.Contains(err.Error(), "the name su twice") {
			t.Errorf("a duplicate zone name = %v, want a refusal (UNIQUE zones.name)", err)
		}

		dir = t.TempDir()
		dupDomains := []fixturesDomain{domains[0], {ZoneID: domains[0].ZoneID, ID: domains[0].ID + 1, Label: domains[0].Label}}
		writeDumpDir(t, dir, nil, zones, dupDomains, results, smoke)
		if _, err := fixturesReadDump(dir); err == nil || !strings.Contains(err.Error(), "the label ok in zone 7 twice") {
			t.Errorf("a duplicate domain label = %v, want a refusal (UNIQUE (zone_id, label))", err)
		}
	})

	t.Run("the dump file is missing", func(t *testing.T) {
		dir := t.TempDir()
		writeDumpDir(t, dir, nil, zones, domains, results, smoke)
		if err := os.Remove(filepath.Join(dir, "subdomain_results.ndjson")); err != nil {
			t.Fatal(err)
		}
		_, err := fixturesReadDump(dir)
		if err == nil || !strings.Contains(err.Error(), "subdomain_results.ndjson") {
			t.Errorf("the missing file error = %v, want the file name", err)
		}
	})

	t.Run("a broken NDJSON line — the row number", func(t *testing.T) {
		dir := t.TempDir()
		writeDumpDir(t, dir, nil, zones, domains, results, smoke)
		if err := os.WriteFile(filepath.Join(dir, "zones.ndjson"), []byte("{\"id\": 1,}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := fixturesReadDump(dir)
		if err == nil || !strings.Contains(err.Error(), "row 1") {
			t.Errorf("the broken line error = %v, want the row number", err)
		}
	})

	t.Run("duplicate keys — the last entry wins", func(t *testing.T) {
		dir := t.TempDir()
		dupResults := append(append([]fixturesResult{}, results...), results[0])
		dupResults[2].Stratum = "failed" // a duplicate of the first row's key — harder to tell apart
		dupDomains := append(append([]fixturesDomain{}, domains...), domains[0])
		dupDomains[1].Label = "other"
		writeDumpDir(t, dir, nil, zones, dupDomains, dupResults, smoke)
		dump, err := fixturesReadDump(dir)
		if err != nil {
			t.Fatalf("reading: %v", err)
		}
		if len(dump.Results) != 2 || dump.DupResults != 1 {
			t.Errorf("results %d (duplicates %d), want 2 (1 duplicate)", len(dump.Results), dump.DupResults)
		}
		if dump.Results[0].Stratum != "failed" {
			t.Errorf("the duplicate did not overwrite the first entry: stratum %q", dump.Results[0].Stratum)
		}
		if len(dump.Domains) != 1 || dump.DupDomains != 1 {
			t.Errorf("domains %d (duplicates %d), want 1/1 — one (zone,id) key, the label changed", len(dump.Domains), dump.DupDomains)
		}
		if dump.Domains[0].Label != "other" {
			t.Errorf("the duplicate domain did not overwrite the label: %q", dump.Domains[0].Label)
		}
	})

	t.Run("a row without the dump's domain — an error before the DB", func(t *testing.T) {
		dir := t.TempDir()
		orphan := append(append([]fixturesResult{}, results...),
			fixturesResult{ZoneID: 7, DomainID: 999, SubdomainID: 0, Variant: 0, Scheme: 2, CheckedAt: time.Now()})
		writeDumpDir(t, dir, nil, zones, domains, orphan, smoke)
		_, err := fixturesReadDump(dir)
		if err == nil || !strings.Contains(err.Error(), "references a domain missing from") {
			t.Errorf("the orphan error = %v, want a referential integrity refusal", err)
		}
	})

	t.Run("a domain without the dump's zone — an error before the DB", func(t *testing.T) {
		dir := t.TempDir()
		orphanDomains := append(append([]fixturesDomain{}, domains...),
			fixturesDomain{ZoneID: 9, ID: 1, Label: "stray"})
		writeDumpDir(t, dir, nil, zones, orphanDomains, results, smoke)
		_, err := fixturesReadDump(dir)
		if err == nil || !strings.Contains(err.Error(), "references a zone missing from") {
			t.Errorf("the domain without a zone error = %v", err)
		}
	})
}

// ---------------------------------------------------------------- pool

// importPool — a fake db.DB for the import: INSERTs go to Exec
// (RowsAffected from the length of the first array column — a batch of
// rows), the pre-checks, setval and the plan's read-only counters
// (S-P3-5) go to Query with prepared answers; all SQL is remembered
// (the contracts and the repeated run).
type importPool struct {
	mu sync.Mutex
	// execSQL — the texts of the executed INSERT/setval statements (in order).
	execSQL []string
	// querySQL — the Query texts (pre-checks, setval, the plan).
	querySQL []string
	// zoneRows — the zone pre-check answer: (id, name).
	zoneRows [][]any
	// domainRows — the domain pre-check answer: (zone_id, label, id).
	domainRows [][]any
	// setvalVal — the setval answer.
	setvalVal int64
	// Plan counters: dump rows existing by PK and the swept content (the
	// dry-run reads them with SELECTs).
	zonesExist, domainsExist, resultsExist int
	sweepVal                               int64
	failOn                                 string // an SQL substring → an error
}

func (p *importPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("importPool: CopyFrom is not called")
}

func (p *importPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.execSQL = append(p.execSQL, sql)
	if p.failOn != "" && strings.Contains(sql, p.failOn) {
		return pgconn.CommandTag{}, fmt.Errorf("a fake DB failure (%s)", p.failOn)
	}
	// RowsAffected = rows in the batch: the first column of every import
	// INSERT is the zone_id array.
	if len(args) > 0 {
		if arr, ok := args[0].([]int16); ok {
			return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", len(arr))), nil
		}
	}
	return pgconn.NewCommandTag("INSERT 0 0"), nil
}

func (p *importPool) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.querySQL = append(p.querySQL, sql)
	if p.failOn != "" && strings.Contains(sql, p.failOn) {
		return nil, fmt.Errorf("a fake DB failure (%s)", p.failOn)
	}
	switch {
	case strings.Contains(sql, "count(*) FROM zones AS z"): // plan: existing PKs
		return &fixturesRows{rows: [][]any{{int64(p.zonesExist)}}}, nil
	case strings.Contains(sql, "count(*) FROM domains AS d"):
		return &fixturesRows{rows: [][]any{{int64(p.domainsExist)}}}, nil
	case strings.Contains(sql, "count(*) FROM subdomain_results AS r"):
		return &fixturesRows{rows: [][]any{{int64(p.resultsExist)}}}, nil
	case strings.Contains(sql, "count(*) FROM contents"): // plan: cleanup
		return &fixturesRows{rows: [][]any{{p.sweepVal}}}, nil
	case strings.Contains(sql, "SELECT id, name FROM zones"):
		return &fixturesRows{rows: p.zoneRows}, nil
	case strings.Contains(sql, "AS k(zone_id, label)"):
		return &fixturesRows{rows: p.domainRows}, nil
	case strings.Contains(sql, "SELECT setval("):
		return &fixturesRows{rows: [][]any{{p.setvalVal}}}, nil
	default:
		return nil, fmt.Errorf("importPool: unknown Query: %s", sql)
	}
}

func (p *importPool) calls() (exec, query []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string{}, p.execSQL...), append([]string{}, p.querySQL...)
}

func (p *importPool) countSQL(substr string) int {
	exec, query := p.calls()
	n := 0
	for _, s := range append(exec, query...) {
		if strings.Contains(s, substr) {
			n++
		}
	}
	return n
}

// importDump — a dump from importDumpFixture wrapped into fixturesDump.
func importDump(t *testing.T) *fixturesDump {
	t.Helper()
	zones, domains, results, smoke := importDumpFixture()
	return &fixturesDump{
		Manifest: fixturesManifest{FormatVersion: fixturesFormatVersion},
		Zones:    zones, Domains: domains, Results: results, Smoke: smoke,
	}
}

// TestFixturesImportRun — the import on a fake pool: the order
// pre-check → zones → domains → results → sequences; the counters; a
// repeated run executes the same idempotent statements (ON CONFLICT by
// PK).
func TestFixturesImportRun(t *testing.T) {
	t.Run("success: the order and the counters", func(t *testing.T) {
		pool := &importPool{setvalVal: 7}
		dump := importDump(t)
		var out bytes.Buffer
		stats, err := fixturesImportRun(context.Background(), pool, dump, &out)
		if err != nil {
			t.Fatalf("the import: %v", err)
		}
		if stats.Zones != 1 || stats.Domains != 1 || stats.Results != 2 {
			t.Errorf("the import counters: %+v", stats)
		}
		if stats.ZoneSeq != 7 || stats.DomainSeq != 7 {
			t.Errorf("the sequences were not set: %+v", stats)
		}
		exec, query := pool.calls()
		// Order: the pre-checks (Query) before the INSERT; setval (Query)
		// and the orphaned content cleanup (an Exec without parameters) —
		// after.
		if n := len(exec); n != 4 { // zones, domains, results, contents cleanup
			t.Errorf("Exec calls %d (%v), want 4", n, exec)
		}
		if n := len(query); n != 4 { // 2 pre-checks + 2 setval
			t.Errorf("Query calls %d (%v), want 4", n, query)
		}
		if !strings.Contains(out.String(), "import: zones 1, domains 1, result rows 2") {
			t.Errorf("the import report:\n%s", out.String())
		}
	})

	t.Run("a repeated import — the same idempotent statements", func(t *testing.T) {
		pool := &importPool{setvalVal: 7}
		dump := importDump(t)
		for run := 1; run <= 2; run++ {
			var out bytes.Buffer
			if _, err := fixturesImportRun(context.Background(), pool, dump, &out); err != nil {
				t.Fatalf("run %d: %v", run, err)
			}
		}
		// Both runs — INSERT with ON CONFLICT by PK (idempotency is not
		// "skip the repeat" but "overwrite with the same state"): one
		// statement per table and one setval per run.
		for _, substr := range []string{
			"OVERRIDING SYSTEM VALUE",
			"ON CONFLICT (id) DO UPDATE",
			"ON CONFLICT (zone_id, id) DO UPDATE",
			"ON CONFLICT (zone_id, domain_id, subdomain_id, variant) DO UPDATE",
			"render_failed_at = EXCLUDED.render_failed_at",
			"pg_get_serial_sequence('zones', 'id')",
			"setval('domain_id_seq',",
			"GREATEST((SELECT coalesce(max(id), 0) FROM domains)",
		} {
			if n := pool.countSQL(substr); n != 2 {
				t.Errorf("the statement %q executed %d times over 2 runs, want 2", substr, n)
			}
		}
		// The import makes no plain INSERT without a conflict at all.
		if pool.countSQL("ON CONFLICT") != pool.countSQL("INSERT INTO") {
			t.Errorf("there is an INSERT without ON CONFLICT — a repeated import would duplicate rows")
		}
	})

	t.Run("batches: more than fixturesImportBatch rows — several statements", func(t *testing.T) {
		pool := &importPool{setvalVal: 1}
		zones, domains, _, _ := importDumpFixture()
		var results []fixturesResult
		ts := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
		for i := 0; i < fixturesImportBatch+1; i++ {
			results = append(results, fixturesResult{ZoneID: 7, DomainID: 42,
				SubdomainID: int16(i), Variant: 0, Scheme: 2, CheckedAt: ts, Stratum: "ok"})
		}
		dump := &fixturesDump{Zones: zones, Domains: domains, Results: results}
		var out bytes.Buffer
		stats, err := fixturesImportRun(context.Background(), pool, dump, &out)
		if err != nil {
			t.Fatalf("the import: %v", err)
		}
		if stats.Results != fixturesImportBatch+1 {
			t.Errorf("imported %d rows, want %d", stats.Results, fixturesImportBatch+1)
		}
		if n := pool.countSQL("INSERT INTO subdomain_results"); n != 2 {
			t.Errorf("result batches %d, want 2", n)
		}
	})

	t.Run("a zone name conflict — a refusal before writing", func(t *testing.T) {
		pool := &importPool{zoneRows: [][]any{{int16(3), "su"}}} // in the dump su = id 7
		var out bytes.Buffer
		_, err := fixturesImportRun(context.Background(), pool, importDump(t), &out)
		if err == nil || !strings.Contains(err.Error(), "su (dump id 7, DB id 3)") {
			t.Errorf("the zone conflict error = %v, want the list of zones with ids", err)
		}
		if n := pool.countSQL("INSERT INTO"); n != 0 {
			t.Errorf("after the pre-check refusal %d INSERTs were executed — the writing must not have started", n)
		}
	})

	t.Run("a domain label conflict — a refusal before writing", func(t *testing.T) {
		pool := &importPool{domainRows: [][]any{{int16(7), "ok", int64(5)}}} // in the dump ok = id 42
		var out bytes.Buffer
		_, err := fixturesImportRun(context.Background(), pool, importDump(t), &out)
		if err == nil || !strings.Contains(err.Error(), "dump id 42, DB id 5") {
			t.Errorf("the domain conflict error = %v, want the list of domains with ids", err)
		}
		if n := pool.countSQL("INSERT INTO subdomain_results"); n != 0 {
			t.Errorf("after the domain pre-check refusal the import continued (%d result INSERTs)", n)
		}
	})

	t.Run("a matching id — not a conflict (a repeated import)", func(t *testing.T) {
		pool := &importPool{zoneRows: [][]any{{int16(7), "su"}}, domainRows: [][]any{{int16(7), "ok", int64(42)}}, setvalVal: 7}
		var out bytes.Buffer
		if _, err := fixturesImportRun(context.Background(), pool, importDump(t), &out); err != nil {
			t.Fatalf("the import with matching ids: %v", err)
		}
	})

	t.Run("a DB failure mid-import — an error with the table", func(t *testing.T) {
		pool := &importPool{failOn: "INSERT INTO domains", setvalVal: 7}
		var out bytes.Buffer
		if _, err := fixturesImportRun(context.Background(), pool, importDump(t), &out); err == nil ||
			!strings.Contains(err.Error(), "domain import") {
			t.Errorf("the domain import failure = %v, want a step error", err)
		}
	})

	t.Run("an empty dump — the DB is not touched at all", func(t *testing.T) {
		pool := &importPool{}
		var out bytes.Buffer
		stats, err := fixturesImportRun(context.Background(), pool, &fixturesDump{}, &out)
		if err != nil {
			t.Fatalf("the empty dump: %v", err)
		}
		if stats.Zones+stats.Domains+stats.Results != 0 || stats.ZoneSeq != 0 || stats.DomainSeq != 0 {
			t.Errorf("the empty dump returned counters: %+v", stats)
		}
		if exec, query := pool.calls(); len(exec) != 0 || len(query) != 0 {
			t.Errorf("the empty dump executes statements: exec %v, query %v", exec, query)
		}
	})
}

// TestFixturesImportForceGate — the --force gate (S-P3-5): without the
// flag — only the plan (the target from the DSN without the secret, the
// insert/update counters per table, duplicate keys, the swept content),
// the DB untouched (not a single Exec); with the flag — the plan and the
// import; the target carries no DSN password.
func TestFixturesImportForceGate(t *testing.T) {
	dsn := "postgres://crawler:secret@127.0.0.1:5432/stand?sslmode=disable"

	t.Run("without --force — the plan only, the DB untouched", func(t *testing.T) {
		pool := &importPool{setvalVal: 7, zonesExist: 1, domainsExist: 0, resultsExist: 1, sweepVal: 3}
		var out bytes.Buffer
		if err := fixturesImportApply(context.Background(), pool, importDump(t),
			fixturesImportOpts{DSN: dsn}, &out); err != nil {
			t.Fatalf("dry-run: %v", err)
		}
		s := out.String()
		for _, need := range []string{
			"import target: crawler@127.0.0.1:5432/stand (mode: dry-run)",
			"plan: zones 1 (insert 0, update 1)",
			"domains 1 (insert 1, update 0)",
			"result rows 2 (insert 1, update 1)",
			"sequences: zones.id → at least 7, domain_id_seq → at least 42",
			"dump duplicate keys (folded): zones 0, domains 0, results 0",
			"swept from contents: 3 rows without live references",
			"dry-run: the DB is not changed",
		} {
			if !strings.Contains(s, need) {
				t.Errorf("the plan without %q:\n%s", need, s)
			}
		}
		if strings.Contains(s, "import: zones") {
			t.Errorf("the dry-run performed the import:\n%s", s)
		}
		if exec, _ := pool.calls(); len(exec) != 0 {
			t.Errorf("the dry-run wrote into the DB: %d Exec statements", len(exec))
		}
		// The plan runs on SELECTs: the pre-checks and the existing-PK counters.
		_, query := pool.calls()
		joined := strings.Join(query, "\n")
		for _, substr := range []string{
			"SELECT id, name FROM zones", // the natural-key pre-check
			"count(*) FROM zones AS z",
			"count(*) FROM domains AS d",
			"count(*) FROM subdomain_results AS r",
			"count(*) FROM contents",
		} {
			if !strings.Contains(joined, substr) {
				t.Errorf("the plan without the query %q (the queries: %v)", substr, query)
			}
		}
	})

	t.Run("with --force — the plan and the import", func(t *testing.T) {
		pool := &importPool{setvalVal: 7, zonesExist: 1}
		var out bytes.Buffer
		if err := fixturesImportApply(context.Background(), pool, importDump(t),
			fixturesImportOpts{Force: true, DSN: dsn}, &out); err != nil {
			t.Fatalf("the import: %v", err)
		}
		s := out.String()
		if !strings.Contains(s, "import target: crawler@127.0.0.1:5432/stand (mode: force)") ||
			!strings.Contains(s, "import: zones 1, domains 1, result rows 2") {
			t.Errorf("the --force output without the target/import:\n%s", s)
		}
		if n := pool.countSQL("INSERT INTO zones"); n != 1 {
			t.Errorf("zone INSERTs %d, want 1 (the import goes)", n)
		}
	})

	t.Run("the publishing plan with --publish", func(t *testing.T) {
		pool := &importPool{}
		var out bytes.Buffer
		if err := fixturesImportApply(context.Background(), pool, importDump(t),
			fixturesImportOpts{DSN: dsn, Publish: fixturesPublishSmoke, RedisAddr: "127.0.0.1:6379"}, &out); err != nil {
			t.Fatalf("the publishing plan: %v", err)
		}
		if !strings.Contains(out.String(), "q:render publishing plan (smoke): tasks 2 → Redis 127.0.0.1:6379") {
			t.Errorf("the plan without the publishing line:\n%s", out.String())
		}
	})

	t.Run("the target without the DSN secret — any DSN form", func(t *testing.T) {
		pool := &importPool{}
		for _, dsn := range []string{
			"postgres://crawler:hunter2@db.prod.example:5432/crawler?sslmode=disable",
			"postgres://db.prod.example:5432/crawler?password=hunter2",
			"host=db.prod.example user=crawler password=hunter2",
		} {
			var out bytes.Buffer
			if err := fixturesImportApply(context.Background(), pool, &fixturesDump{},
				fixturesImportOpts{DSN: dsn}, &out); err != nil {
				t.Fatalf("the empty dump (%s): %v", dsn, err)
			}
			if s := out.String(); strings.Contains(s, "hunter2") {
				t.Errorf("the target output contains the DSN password (%s):\n%s", dsn, s)
			}
		}
	})

	t.Run("a zone name conflict is visible in dry-run — before writing", func(t *testing.T) {
		pool := &importPool{zoneRows: [][]any{{int16(3), "su"}}} // in the dump su = id 7
		var out bytes.Buffer
		if err := fixturesImportApply(context.Background(), pool, importDump(t),
			fixturesImportOpts{DSN: dsn}, &out); err == nil ||
			!strings.Contains(err.Error(), "dump id 7, DB id 3") {
			t.Errorf("the dry-run error = %v, want the pre-check refusal before writing", err)
		}
		if exec, _ := pool.calls(); len(exec) != 0 {
			t.Errorf("after the pre-check refusal %d Execs were executed", len(exec))
		}
	})
}

// TestFixturesImportTarget — the target from the DSN: user@host:port/
// dbname, never the password (userinfo or query); keyword/value DSNs and
// junk — the db.RedactDSN mask without structure.
func TestFixturesImportTarget(t *testing.T) {
	for _, c := range []struct{ dsn, want string }{
		{"postgres://crawler:secret@127.0.0.1:5432/stand?sslmode=disable", "crawler@127.0.0.1:5432/stand"},
		{"postgres://crawler@localhost/stand", "crawler@localhost/stand"},
		{"postgres://localhost/stand", "localhost/stand"},
		{"postgres://u:p@h/db?password=also-secret", "u@h/db"},
	} {
		if got := fixturesImportTarget(c.dsn); got != c.want {
			t.Errorf("target(%s) = %q, want %q", c.dsn, got, c.want)
		}
	}
	for _, dsn := range []string{"host=h user=u password=hunter2", "not a dsn", ""} {
		if got := fixturesImportTarget(dsn); got == dsn || strings.Contains(got, "hunter2") {
			t.Errorf("target(%q) = %q — the DSN is exposed as-is or with the password", dsn, got)
		}
	}
}

// TestFixturesImportSQLContract — the import SQL contracts: unnest
// arrays (a column = a parameter), jsonb as text, timestamptz, all
// updatable columns in DO UPDATE, the T-242 contract's sequence notes.
func TestFixturesImportSQLContract(t *testing.T) {
	if !strings.Contains(fixturesImportZonesSQL, "OVERRIDING SYSTEM VALUE") {
		t.Errorf("zones: no OVERRIDING SYSTEM VALUE (the identity, the T-242 contract): %s", fixturesImportZonesSQL)
	}
	for _, need := range []string{"$1::int2[]", "$2::text[]", "$7::timestamptz[]", "ON CONFLICT (id) DO UPDATE"} {
		if !strings.Contains(fixturesImportZonesSQL, need) {
			t.Errorf("zones: %s is missing: %s", need, fixturesImportZonesSQL)
		}
	}
	if strings.Count(fixturesImportDomainsSQL, "::") < 10 {
		t.Errorf("domains: not all columns are unnest arrays: %s", fixturesImportDomainsSQL)
	}
	if !strings.Contains(fixturesImportDomainsSQL, "(d.last_checked_at AT TIME ZONE 'UTC')::date") {
		t.Errorf("domains: the date column without the UTC shift — the session TZ would slide the date: %s", fixturesImportDomainsSQL)
	}
	for _, need := range []string{
		"$10::jsonb[]", "$11::jsonb[]", "$15::timestamptz[]", "$17::timestamptz[]",
		"rendered = EXCLUDED.rendered", "render_failed_at = EXCLUDED.render_failed_at",
	} {
		if !strings.Contains(fixturesImportResultsSQL, need) {
			t.Errorf("results: %s is missing: %s", need, fixturesImportResultsSQL)
		}
	}
	// The content reference is not imported and is reset: contents was
	// not exported (T-242), the production reference would dangle on the
	// bench, and the render write would fail with ErrRefIntegrity
	// (SwapRefTx).
	if strings.Contains(fixturesImportResultsSQL, "content_hash = EXCLUDED.content_hash") ||
		!strings.Contains(fixturesImportResultsSQL, "content_hash = NULL") {
		t.Errorf("results: content_hash must be reset to NULL: %s", fixturesImportResultsSQL)
	}
	// The import does not reassemble columns outside the dump (T-242
	// de-identification).
	for _, banned := range []string{"addrs", "cname", "tls", "redirects", "stratum"} {
		if strings.Contains(fixturesImportResultsSQL, banned) {
			t.Errorf("results: the column %s is outside the dump contract: %s", banned, fixturesImportResultsSQL)
		}
	}
	if !strings.Contains(fixturesDomainsSeqSQL, "GREATEST((SELECT coalesce(max(id), 0) FROM domains)") {
		t.Errorf("domain_id_seq: setval is not above the table's maximum: %s", fixturesDomainsSeqSQL)
	}
	// Orphaned content cleanup: only rows without a live reference
	// (review T-243 P2-1 — refcount is not decremented by resetting
	// references).
	if !strings.Contains(fixturesSweepContentsSQL, "DELETE FROM contents") ||
		!strings.Contains(fixturesSweepContentsSQL, "NOT EXISTS") ||
		strings.Contains(fixturesSweepContentsSQL, "refcount") {
		t.Errorf("content cleanup: not sweeping the orphans by references: %s", fixturesSweepContentsSQL)
	}
}

// ---------------------------------------------------------------- publishing

// publishQueue — a fake queue: Publish checks the message type and the
// stream (deep value validation is the queue package's territory, over
// at RedisQueue; here — what the fixture publishing controls) and
// remembers; failAt — the publishing number to fail on (0 — do not
// fail).
type publishQueue struct {
	failAt    int
	failErr   error
	published []queue.Result
	streams   []string
}

func (q *publishQueue) Publish(_ context.Context, stream string, msg queue.Message) (string, error) {
	if stream != queue.StreamRender {
		return "", fmt.Errorf("publishQueue: the stream %q — the test publishes only q:render", stream)
	}
	r, ok := msg.(queue.Result)
	if !ok {
		return "", fmt.Errorf("publishQueue: the message %T — the test expects queue.Result", msg)
	}
	if q.failAt > 0 && len(q.published)+1 == q.failAt {
		return "", q.failErr
	}
	q.published = append(q.published, r)
	q.streams = append(q.streams, stream)
	return fmt.Sprintf("1-%d", len(q.published)), nil
}

func (q *publishQueue) Consume(context.Context, string, string, string, int) ([]queue.Delivery, error) {
	return nil, errors.New("publishQueue: Consume is not called")
}

func (q *publishQueue) Ack(context.Context, string, string, ...string) error {
	return errors.New("publishQueue: Ack is not called")
}

// TestFixturesPublishRun — publishing the fixture tasks: the all mode
// (all rows, label.zone names, Fresh is not set — the worker sets it),
// the smoke mode (subsample domains), duplicate keys, a publishing
// failure.
func TestFixturesPublishRun(t *testing.T) {
	t.Run("all: all the rows with names", func(t *testing.T) {
		q := &publishQueue{}
		var out bytes.Buffer
		stats, err := fixturesPublishRun(context.Background(), q, importDump(t), fixturesPublishAll, &out)
		if err != nil {
			t.Fatalf("publishing: %v", err)
		}
		if stats.Published != 2 || len(q.published) != 2 {
			t.Fatalf("published %d (the counter %d), want 2", len(q.published), stats.Published)
		}
		if stats.Strata["ok"] != 1 || stats.Strata["heavy"] != 1 {
			t.Errorf("the strata breakdown: %+v", stats.Strata)
		}
		first := q.published[0]
		if first.Name != "ok.su" {
			t.Errorf("the task name %q, want ok.su (label.zone)", first.Name)
		}
		if first.ZoneID != 7 || first.DomainID != 42 || first.SubdomainID != 0 || first.Variant != 0 {
			t.Errorf("the task keys: %+v", first)
		}
		if first.Fresh {
			t.Errorf("Fresh is set by the fixture publishing — the worker sets it when publishing into q:analyze")
		}
		if !strings.Contains(out.String(), "published 2") {
			t.Errorf("the publishing report:\n%s", out.String())
		}
	})

	t.Run("smoke: only the subsample domains", func(t *testing.T) {
		dump := importDump(t)
		// The second domain is outside smoke: its subdomain's row is not
		// published.
		dump.Domains = append(dump.Domains, fixturesDomain{ZoneID: 7, ID: 43, Label: "other"})
		dump.Results = append(dump.Results, fixturesResult{ZoneID: 7, DomainID: 43,
			SubdomainID: 0, Variant: 0, Scheme: 2, CheckedAt: time.Now(), Stratum: "ok"})
		q := &publishQueue{}
		var out bytes.Buffer
		stats, err := fixturesPublishRun(context.Background(), q, dump, fixturesPublishSmoke, &out)
		if err != nil {
			t.Fatalf("the smoke publishing: %v", err)
		}
		if stats.Published != 2 || stats.SmokeDomains != 1 {
			t.Fatalf("smoke: published %d, %d domains, want 2 tasks of 1 domain", stats.Published, stats.SmokeDomains)
		}
		for _, r := range q.published {
			if r.DomainID != 42 {
				t.Errorf("a task outside the smoke subsample was published: %+v", r)
			}
		}
		if !strings.Contains(out.String(), "smoke, 1 domain") {
			t.Errorf("the smoke report:\n%s", out.String())
		}
	})

	t.Run("duplicate task keys are skipped with a count", func(t *testing.T) {
		dump := importDump(t)
		dump.Results = append(dump.Results, dump.Results[0]) // a duplicate key
		q := &publishQueue{}
		var out bytes.Buffer
		stats, err := fixturesPublishRun(context.Background(), q, dump, fixturesPublishAll, &out)
		if err != nil {
			t.Fatalf("publishing: %v", err)
		}
		if stats.Published != 2 || stats.SkippedDups != 1 {
			t.Errorf("published %d, duplicates skipped %d, want 2/1", stats.Published, stats.SkippedDups)
		}
		if !strings.Contains(out.String(), "duplicate task keys skipped: 1") {
			t.Errorf("the duplicates report:\n%s", out.String())
		}
	})

	t.Run("a publishing failure — an error with the progress", func(t *testing.T) {
		q := &publishQueue{failAt: 2, failErr: errors.New("redis went to lunch")}
		var out bytes.Buffer
		stats, err := fixturesPublishRun(context.Background(), q, importDump(t), fixturesPublishAll, &out)
		if err == nil || !strings.Contains(err.Error(), "published 1") || !strings.Contains(err.Error(), "redis went") {
			t.Errorf("the publishing failure = %v, want the progress and the reason", err)
		}
		if stats.Published != 1 {
			t.Errorf("the publishing progress %d, want 1", stats.Published)
		}
	})

	t.Run("an empty smoke subsample — a refusal, not an empty run", func(t *testing.T) {
		dump := importDump(t)
		dump.Smoke = nil // a degraded dump / the wrong directory
		q := &publishQueue{}
		var out bytes.Buffer
		if _, err := fixturesPublishRun(context.Background(), q, dump, fixturesPublishSmoke, &out); err == nil ||
			!strings.Contains(err.Error(), "the smoke subsample is empty") {
			t.Errorf("the empty smoke = %v, want a refusal (the operator must not mistake it for a working run)", err)
		}
		if len(q.published) != 0 {
			t.Errorf("the empty smoke published something: %d", len(q.published))
		}
	})

	t.Run("unknown mode", func(t *testing.T) {
		var out bytes.Buffer
		if _, err := fixturesPublishRun(context.Background(), &publishQueue{}, importDump(t), "some", &out); err == nil ||
			!strings.Contains(err.Error(), "unknown mode") {
			t.Errorf("the junk mode = %v, want a refusal", err)
		}
	})
}

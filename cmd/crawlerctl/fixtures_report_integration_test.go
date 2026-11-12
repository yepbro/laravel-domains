// Integration test of the snapshot and the comparison report (T-244)
// against the live PostgreSQL of the dev environment: the task's
// criterion — "on two runs of the same configuration the detection diff
// ≈0; on a corrupted run the report catches the divergence" — is checked
// on real SQL (unit tests exercise pure structures). Isolation — a
// one-off schema with migrations in the dev-stack DB (the shared helper
// of the live fixture tests t243Schema, T-243). Run: make
// test-integration (CRAWLER_INTEGRATION=1, the DB — the tools
// container's APP_PG_DSN); without the variable the test is skipped.
package main

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestFixturesSnapshotReportLive — two runs of one selection on a live
// schema: snapshots via fixturesSnapshotReadRows (the snapshot SQL), the
// report via fixturesReportBuild — detection ≈0, DOM pairs/identity,
// success; a corrupted run (flipped verdicts) is caught by a FAILED
// verdict.
func TestFixturesSnapshotReportLive(t *testing.T) {
	if os.Getenv("CRAWLER_INTEGRATION") != "1" {
		t.Skip("the snapshot/report integration test: run make test-integration")
	}
	ctx := context.Background()
	pool, cleanup := t243Schema(t)
	defer cleanup()

	// Dump: 6 tasks of one domain (the ok stratum, the base final_url
	// predicate).
	ts := time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC)
	url := "https://fixture.example/"
	dump := &fixturesDump{
		Manifest: fixturesManifest{FormatVersion: fixturesFormatVersion, Seed: 242},
		Zones:    []fixturesZone{{ID: 9, Name: "fixture.test", IsActive: true, DomainsTotal: 1}},
		Domains:  []fixturesDomain{{ZoneID: 9, ID: 77, Label: "spa", Status: 2, CheckState: 2, DNSMask: 1}},
	}
	for i := int64(1); i <= 6; i++ {
		dump.Results = append(dump.Results, fixturesResult{
			ZoneID: 9, DomainID: 77, SubdomainID: int16(i), Variant: 0,
			Scheme: 2, FinalURL: &url, CheckedAt: ts, Stratum: "ok",
		})
	}
	if _, err := fixturesImportRun(ctx, pool, dump, os.Stdout); err != nil {
		t.Fatalf("the import: %v", err)
	}

	// simulateRun — a run: success/finalize/verdicts. The worker contract
	// (renderUpdateSQL): rendered=true together with content_hash and
	// rules_version=NULL — the analyzer then re-stamps the row; findings
	// are written only on hits (findingUpsertSQL), at zero — DELETE
	// (findingDeleteSQL, P1-1). laravel[sub]==nil — the analyzer has not
	// passed over the row yet (rules_version stays NULL).
	simulateRun := func(finalized map[int16]bool, laravel map[int16]*bool, domHash map[int16]string, domLen map[int16]int32) {
		t.Helper()
		for sub := int16(1); sub <= 6; sub++ {
			if finalized[sub] {
				if _, err := pool.Exec(ctx,
					"UPDATE subdomain_results SET render_failed_at = now() WHERE zone_id = 9 AND domain_id = 77 AND subdomain_id = $1 AND variant = 0", sub); err != nil {
					t.Fatalf("finalization %d: %v", sub, err)
				}
				continue
			}
			hash := []byte(domHash[sub])
			if _, err := pool.Exec(ctx,
				"INSERT INTO contents (hash, pack_id, off, len, raw_len, refcount) VALUES ($1, 0, 0, $2, $2, 1) ON CONFLICT (hash) DO UPDATE SET refcount = contents.refcount + 1",
				hash, domLen[sub]); err != nil {
				t.Fatalf("the content seed %d: %v", sub, err)
			}
			if _, err := pool.Exec(ctx,
				"UPDATE subdomain_results SET rendered = true, content_hash = $1, rules_version = NULL WHERE zone_id = 9 AND domain_id = 77 AND subdomain_id = $2 AND variant = 0",
				hash, sub); err != nil {
				t.Fatalf("the render %d: %v", sub, err)
			}
			isLaravel, ok := laravel[sub]
			if !ok {
				continue // the q:analyze tail: a row without the analyzer's stamp
			}
			if _, err := pool.Exec(ctx,
				"UPDATE subdomain_results SET rules_version = 7 WHERE zone_id = 9 AND domain_id = 77 AND subdomain_id = $1 AND variant = 0", sub); err != nil {
				t.Fatalf("the analyzer stamp %d: %v", sub, err)
			}
			if *isLaravel {
				if _, err := pool.Exec(ctx,
					"INSERT INTO laravel_findings (zone_id, domain_id, subdomain_id, variant, score, is_laravel, signals, rules_version) "+
						"VALUES (9, 77, $1, 0, 0.9, true, '[]'::jsonb, 7) ON CONFLICT (zone_id, domain_id, subdomain_id, variant) "+
						"DO UPDATE SET is_laravel = true, rules_version = 7",
					sub); err != nil {
					t.Fatalf("the analyzer verdict %d: %v", sub, err)
				}
			} else if _, err := pool.Exec(ctx,
				"DELETE FROM laravel_findings WHERE zone_id = 9 AND domain_id = 77 AND subdomain_id = $1 AND variant = 0", sub); err != nil {
				t.Fatalf("the findings deletion %d: %v", sub, err)
			}
		}
	}

	keys := make([]fixturesKey, 0, len(dump.Results))
	for _, r := range dump.Results {
		keys = append(keys, fixturesKey{r.ZoneID, r.DomainID, r.SubdomainID, r.Variant})
	}
	take := func(label, engine string) *fixturesSnapshot {
		t.Helper()
		rows, err := fixturesSnapshotReadRows(ctx, pool, keys)
		if err != nil {
			t.Fatalf("the snapshot %s: %v", label, err)
		}
		return &fixturesSnapshot{
			FormatVersion: fixturesSnapshotVersion,
			Label:         label, Engine: engine,
			CreatedAt: time.Now().UTC(),
			Dump:      fixturesSnapshotDump{Seed: 242, Keys: len(keys)},
			Rows:      rows,
		}
	}

	// Run 1 (chromium): 5 successes (sub 1..5), 1 finalization; verdicts
	// on all successes, Laravel on 1 and 4.
	reset := func() {
		if _, err := fixturesImportRun(ctx, pool, dump, os.Stdout); err != nil {
			t.Fatalf("the repeated import: %v", err)
		}
	}
	reset()
	simulateRun(
		map[int16]bool{6: true},
		map[int16]*bool{1: bp(true), 2: bp(false), 3: bp(false), 4: bp(true), 5: bp(false)},
		map[int16]string{1: "01010101010101010101010101010101", 2: "02020202020202020202020202020202",
			3: "03030303030303030303030303030303", 4: "04040404040404040404040404040404", 5: "05050505050505050505050505050505"},
		map[int16]int32{1: 1000, 2: 2000, 3: 3000, 4: 4000, 5: 5000},
	)
	snapChromium := take("chromium-1", "chromium")

	// Run 2 (moli): the same tasks; 5 successes, sub 3 has a different
	// DOM (the engine assembled a different page), verdicts identical.
	reset()
	simulateRun(
		map[int16]bool{6: true},
		map[int16]*bool{1: bp(true), 2: bp(false), 3: bp(false), 4: bp(true), 5: bp(false)},
		map[int16]string{1: "01010101010101010101010101010101", 2: "02020202020202020202020202020202",
			3: "33333333333333333333333333333333", 4: "04040404040404040404040404040404", 5: "05050505050505050505050505050505"},
		map[int16]int32{1: 1000, 2: 2000, 3: 3100, 4: 4000, 5: 5000},
	)
	snapMoli := take("moli-1", "moli")

	report, err := fixturesReportBuild(snapChromium, snapMoli, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("the report: %v", err)
	}
	if report.SuccessRenderedB != 5 || report.SuccessRenderedC != 5 {
		t.Fatalf("the runs success = %d/%d, want 5/5", report.SuccessRenderedB, report.SuccessRenderedC)
	}
	if report.DetectionComparable != 5 || report.DetectionDiff != 0 {
		t.Fatalf("detection = %d comparable / %d divergences, want 5/0", report.DetectionComparable, report.DetectionDiff)
	}
	if v := verdictOf(t, report, "detection"); v == nil || v.Status != verdictPass {
		t.Fatalf("the detection verdict = %+v, want PASSED", v)
	}
	if report.DOMPairs != 5 || report.DOMIdentical != 4 {
		t.Fatalf("DOM pairs = %d, identical %d, want 5/4", report.DOMPairs, report.DOMIdentical)
	}
	if report.DOMMedLenRatio == nil || *report.DOMMedLenRatio != 1 {
		t.Fatalf("the DOM length median = %v, want 1", report.DOMMedLenRatio)
	}

	// Corrupted run: subdomain 1's signal VANISHED entirely (the analyzer
	// deleted the findings — not a single hit, P1-1), subdomain 2's —
	// appeared; 2 of 5 comparable (40%) — the report must catch both.
	reset()
	simulateRun(
		map[int16]bool{6: true},
		map[int16]*bool{1: bp(false), 2: bp(true), 3: bp(false), 4: bp(true), 5: bp(false)},
		map[int16]string{1: "01010101010101010101010101010101", 2: "02020202020202020202020202020202",
			3: "03030303030303030303030303030303", 4: "04040404040404040404040404040404", 5: "05050505050505050505050505050505"},
		map[int16]int32{1: 1000, 2: 2000, 3: 3000, 4: 4000, 5: 5000},
	)
	snapCorrupted := take("moli-2", "moli")
	report, err = fixturesReportBuild(snapChromium, snapCorrupted, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("the report (corrupted): %v", err)
	}
	if report.DetectionDiff != 2 || report.DetectionRatePct != 40 {
		t.Fatalf("divergences = %d (%.2f%%), want 2 (40%%)", report.DetectionDiff, report.DetectionRatePct)
	}
	if v := verdictOf(t, report, "detection"); v == nil || v.Status != verdictFail {
		t.Fatalf("the detection verdict = %+v, want FAILED", v)
	}

	// The q:analyze tail: subdomain 4's DOM is written but the analyzer
	// has not passed (rules_version NULL, the findings in the DB are from
	// the previous run): the row is not comparable (P1-2) — the previous
	// run's findings do not produce a false zero.
	reset()
	simulateRun(
		map[int16]bool{6: true},
		map[int16]*bool{1: bp(true), 2: bp(false), 3: bp(false), 5: bp(false)}, // 4 — no stamp
		map[int16]string{1: "01010101010101010101010101010101", 2: "02020202020202020202020202020202",
			3: "03030303030303030303030303030303", 4: "04040404040404040404040404040404", 5: "05050505050505050505050505050505"},
		map[int16]int32{1: 1000, 2: 2000, 3: 3000, 4: 4000, 5: 5000},
	)
	snapTail := take("moli-3", "moli")
	report, err = fixturesReportBuild(snapChromium, snapTail, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("the report (the analyzer tail): %v", err)
	}
	if report.DetectionComparable != 4 {
		t.Fatalf("comparable %d, want 4 (a row without the analyzer stamp is excluded)", report.DetectionComparable)
	}
	if report.DetectionDiff != 0 {
		t.Fatalf("divergences %d, want 0", report.DetectionDiff)
	}
}

// bp — a *bool literal for the hit maps (Go has no pointer literal).
func bp(v bool) *bool { return &v }

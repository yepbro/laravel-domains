// Unit tests for crawlerctl fixtures report (T-244): the detection diff
// per the T-213 protocol (the sanity checks "two runs of one
// configuration — diff ≈0" and "an artificially corrupted run is
// caught"), success with the engine-independent losses subtracted, pace
// and page time (the histogram), structural DOM metrics, verdicts
// against the §5.1 item 4 thresholds and honest degradations of missing
// sources. Snapshots are synthetic structures (format v1); no
// infrastructure is needed.
package main

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"
)

// snapRows — a snapshot over n identical rows: rendered+content and a
// default Laravel verdict (the T-213 protocol: comparable keys — the
// content written by the run and stamped by that run's analyzer).
func snapRows(n int, mut func(i int, r *fixturesSnapshotRow)) []fixturesSnapshotRow {
	rules := int32(7)
	out := make([]fixturesSnapshotRow, 0, n)
	for i := 0; i < n; i++ {
		r := fixturesSnapshotRow{
			ZoneID: 1, DomainID: int64(i + 1), SubdomainID: 0, Variant: 0,
			Rendered: true, HasContent: true,
			ContentHash: "aabb", DOMLen: 1000, DOMParsed: true,
			DOMNodes: 100, DOMScripts: 10,
			RulesVersion:        &rules, // the analyzer's stamp for this DOM
			LaravelFound:        true,
			IsLaravel:           i%10 == 0,
			Score:               0.7,
			FindingRulesVersion: &rules,
		}
		if mut != nil {
			mut(i, &r)
		}
		out = append(out, r)
	}
	return out
}

// snapOf — a snapshot with the runs' basic identity (label/engine
// differ — these are different runs of one selection).
func snapOf(label, engine string, rows []fixturesSnapshotRow) *fixturesSnapshot {
	return &fixturesSnapshot{
		FormatVersion: fixturesSnapshotVersion,
		Label:         label,
		Engine:        engine,
		CreatedAt:     time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC),
		Dump:          fixturesSnapshotDump{Seed: 242, Keys: len(rows)},
		Rows:          rows,
	}
}

// verdictOf — a verdict by name from the report (none — nil).
func verdictOf(t *testing.T, r *fixturesReport, namePart string) *fixturesReportVerdict {
	t.Helper()
	for i := range r.Verdicts {
		if strings.Contains(r.Verdicts[i].Name, namePart) {
			return &r.Verdicts[i]
		}
	}
	return nil
}

// TestFixturesReportDetectionSanity — the task's sanity criterion: two
// runs of one configuration → a detection diff of 0, a PASSED verdict.
func TestFixturesReportDetectionSanity(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(300, nil))
	c := snapOf("chromium-2", "chromium", snapRows(300, nil))
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("the report: %v", err)
	}
	if r.DetectionComparable != 300 || r.DetectionDiff != 0 {
		t.Fatalf("detection: %d comparable, %d divergences — want 300/0", r.DetectionComparable, r.DetectionDiff)
	}
	if r.DetectionRatePct != 0 {
		t.Errorf("the divergence share = %v, want 0", r.DetectionRatePct)
	}
	v := verdictOf(t, r, "detection")
	if v == nil || v.Status != verdictPass {
		t.Fatalf("the detection verdict = %+v, want PASSED", v)
	}
}

// TestFixturesReportDetectionCorrupted — an artificially corrupted run:
// the analyzer's verdicts flipped at >1% of keys — the report catches
// the divergence (FAILED, the share is visible in the text).
func TestFixturesReportDetectionCorrupted(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(300, nil))
	c := snapOf("moli-1", "moli", snapRows(300, func(i int, r *fixturesSnapshotRow) {
		if i < 6 { // 2% of comparable keys with the opposite verdict
			r.IsLaravel = !r.IsLaravel
		}
	}))
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("the report: %v", err)
	}
	if r.DetectionDiff != 6 || r.DetectionRatePct != 2 {
		t.Fatalf("divergences = %d (%.2f%%), want 6 (2.00%%)", r.DetectionDiff, r.DetectionRatePct)
	}
	v := verdictOf(t, r, "detection")
	if v == nil || v.Status != verdictFail {
		t.Fatalf("the detection verdict = %+v, want FAILED", v)
	}
	var buf bytes.Buffer
	fixturesReportPrint(&buf, r)
	for _, want := range []string{"2.00%", "6", "FAILED"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("the report text without %q:\n%s", want, buf.String())
		}
	}
}

// TestFixturesReportDetectionDegradations — detection diff degradations:
// the analyzer has not finished (rows without a stamp), the rules
// changed; and "all signs lost" is caught as a divergence (P1-1).
func TestFixturesReportDetectionDegradations(t *testing.T) {
	t.Run("the analyzer has not finished — a row without a stamp", func(t *testing.T) {
		b := snapOf("chromium-1", "chromium", snapRows(10, nil))
		c := snapOf("moli-1", "moli", snapRows(10, func(i int, r *fixturesSnapshotRow) {
			r.RulesVersion = nil // the render passed, the analyzer has not passed over this DOM
		}))
		r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
		if err != nil {
			t.Fatalf("the report: %v", err)
		}
		if r.DetectionComparable != 0 {
			t.Fatalf("comparable %d, want 0 (without the analyzer stamp)", r.DetectionComparable)
		}
		if v := verdictOf(t, r, "detection"); v == nil || v.Status != verdictNone {
			t.Fatalf("the detection verdict = %+v, want NO DATA", v)
		}
	})
	t.Run("finding_rules_version diverged", func(t *testing.T) {
		other := int32(9)
		b := snapOf("chromium-1", "chromium", snapRows(10, nil))
		c := snapOf("moli-1", "moli", snapRows(10, func(i int, r *fixturesSnapshotRow) {
			r.FindingRulesVersion = &other
		}))
		r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
		if err != nil {
			t.Fatalf("the report: %v", err)
		}
		if v := verdictOf(t, r, "detection"); v == nil || v.Status != verdictNone {
			t.Fatalf("the detection verdict = %+v, want NO DATA (the rules changed)", v)
		}
		if len(r.Warnings) == 0 {
			t.Errorf("no warning about the rule versions")
		}
	})
	t.Run("the loss of all signs is a divergence, not a skip", func(t *testing.T) {
		b := snapOf("chromium-1", "chromium", snapRows(100, nil))
		c := snapOf("moli-1", "moli", snapRows(100, func(i int, r *fixturesSnapshotRow) {
			if i%10 == 0 { // all baseline-Laravel rows
				r.LaravelFound = false // the analyzer deleted the findings: not a single hit
				r.IsLaravel = false
				r.FindingRulesVersion = nil
			}
		}))
		r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
		if err != nil {
			t.Fatalf("the report: %v", err)
		}
		if r.DetectionComparable != 100 || r.DetectionDiff != 10 {
			t.Fatalf("comparable %d, divergences %d — want 100/10 (the signals vanishing is a divergence)", r.DetectionComparable, r.DetectionDiff)
		}
		if r.DetectionFoundB != 10 || r.DetectionFoundC != 0 {
			t.Fatalf("Laravel verdicts %d/%d, want 10/0", r.DetectionFoundB, r.DetectionFoundC)
		}
		if v := verdictOf(t, r, "detection"); v == nil || v.Status != verdictFail {
			t.Fatalf("the detection verdict = %+v, want FAILED (10%%)", v)
		}
	})
}

// TestFixturesReportSuccess — success: the share of rows with the run's
// DOM; the verdict with the engine-independent losses subtracted (DNS/
// cert from the metrics) and a strict comparison without metrics.
func TestFixturesReportSuccess(t *testing.T) {
	base := snapOf("chromium-1", "chromium", snapRows(100, nil))
	// Candidate: 90 successful, 10 without a DOM.
	cand := snapOf("moli-1", "moli", snapRows(100, func(i int, r *fixturesSnapshotRow) {
		if i >= 90 {
			r.Rendered, r.HasContent, r.ContentHash, r.DOMLen = false, false, "", 0
		}
	}))

	t.Run("no metrics — the strict comparison", func(t *testing.T) {
		r, err := fixturesReportBuild(base, cand, fixturesDefaultMemoryBudget)
		if err != nil {
			t.Fatalf("the report: %v", err)
		}
		if r.SuccessPctB != 100 || r.SuccessPctC != 90 {
			t.Fatalf("success = %v/%v, want 100/90", r.SuccessPctB, r.SuccessPctC)
		}
		v := verdictOf(t, r, "success")
		if v == nil || v.Status != verdictFail {
			t.Fatalf("the success verdict = %+v, want FAILED (the strict comparison)", v)
		}
	})

	t.Run("with metrics — subtracting the DNS/cert losses", func(t *testing.T) {
		b := *base // a copy: the sub-test's metrics do not leak into others
		b.Streams = &fixturesSnapshotStreams{AnalyzeLen: 100, AnalyzeFirstMs: 1000, AnalyzeLastMs: 21000}
		b.Metrics = &fixturesSnapshotMetrics{ProcessStartSeconds: 1, Tasks: 100, SecondsCount: 100, SecondsSum: 300}
		c := *cand
		c.Streams = &fixturesSnapshotStreams{AnalyzeLen: 100, AnalyzeFirstMs: 1000, AnalyzeLastMs: 21000}
		c.Metrics = &fixturesSnapshotMetrics{
			ProcessStartSeconds: 2, Tasks: 100, SecondsCount: 100, SecondsSum: 350,
			DNSFinalized: 5, CertFinalized: 6, // 11% engine-independent losses
		}
		r, err := fixturesReportBuild(&b, &c, fixturesDefaultMemoryBudget)
		if err != nil {
			t.Fatalf("the report: %v", err)
		}
		if r.IndepLossPctC == nil || *r.IndepLossPctC != 11 {
			t.Fatalf("the losses = %v, want 11%%", r.IndepLossPctC)
		}
		v := verdictOf(t, r, "success")
		if v == nil || v.Status != verdictPass {
			t.Fatalf("the success verdict = %+v, want PASSED (90 ≥ 100 − 11)", v)
		}
		if !strings.Contains(v.Detail, "11.00%") {
			t.Errorf("the verdict details without the loss share: %s", v.Detail)
		}
	})
}

// TestFixturesReportPace — the pace over the q:analyze boundaries and
// the "not worse than baseline" verdict; empty streams — NO DATA.
func TestFixturesReportPace(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(50, nil))
	b.Streams = &fixturesSnapshotStreams{AnalyzeLen: 51, AnalyzeFirstMs: 1000, AnalyzeLastMs: 11000} // 5 rows/s
	c := snapOf("moli-1", "moli", snapRows(50, nil))

	t.Run("the candidate is faster", func(t *testing.T) {
		c2 := *c                                                                                         // a copy of the struct, not the pointer: mutations do not leak into neighboring sub-tests
		c2.Streams = &fixturesSnapshotStreams{AnalyzeLen: 51, AnalyzeFirstMs: 2000, AnalyzeLastMs: 8200} // 8.06 rows/s
		r, err := fixturesReportBuild(b, &c2, fixturesDefaultMemoryBudget)
		if err != nil {
			t.Fatalf("the report: %v", err)
		}
		if r.PaceB == nil || *r.PaceB != 5 {
			t.Fatalf("the baseline pace = %v, want 5", r.PaceB)
		}
		v := verdictOf(t, r, "pace")
		if v == nil || v.Status != verdictPass {
			t.Fatalf("the pace verdict = %+v, want PASSED", v)
		}
	})
	t.Run("the candidate is slower", func(t *testing.T) {
		c2 := *c
		c2.Streams = &fixturesSnapshotStreams{AnalyzeLen: 51, AnalyzeFirstMs: 1000, AnalyzeLastMs: 26000} // 2 rows/s
		r, err := fixturesReportBuild(b, &c2, fixturesDefaultMemoryBudget)
		if err != nil {
			t.Fatalf("the report: %v", err)
		}
		if v := verdictOf(t, r, "pace"); v == nil || v.Status != verdictFail {
			t.Fatalf("the pace verdict = %+v, want FAILED", v)
		}
	})
	t.Run("the streams were not captured", func(t *testing.T) {
		r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
		if err != nil {
			t.Fatalf("the report: %v", err)
		}
		if v := verdictOf(t, r, "pace"); v == nil || v.Status != verdictNone {
			t.Fatalf("the pace verdict = %+v, want NO DATA", v)
		}
	})
}

// TestFixturesHistQuantile — a quantile over the histogram's buckets:
// interpolation inside a bucket, a flat stretch, missing data.
func TestFixturesHistQuantile(t *testing.T) {
	buckets := []fixturesHistBucket{
		{0.5, 10}, {1, 20}, {2.5, 50}, {5, 90}, {10, 100},
	}
	if got := fixturesHistQuantile(buckets, 100, 0.5); got == nil || *got != 2.5 {
		t.Errorf("p50 = %v, want 2.5", got)
	}
	// p95: rank 95 — the [5,10) bucket with 90..100 → 5 + (95-90)/10*5 = 7.5.
	if got := fixturesHistQuantile(buckets, 100, 0.95); got == nil || *got != 7.5 {
		t.Errorf("p95 = %v, want 7.5", got)
	}
	// p10 — exactly at the first bucket's bound.
	if got := fixturesHistQuantile(buckets, 100, 0.10); got == nil || *got != 0.5 {
		t.Errorf("p10 = %v, want 0.5", got)
	}
	if got := fixturesHistQuantile(nil, 100, 0.95); got != nil {
		t.Errorf("an empty histogram = %v, want nil", got)
	}
	if got := fixturesHistQuantile(buckets, 0, 0.95); got != nil {
		t.Errorf("a zero count = %v, want nil", got)
	}
}

// TestFixturesReportDOM — structural DOM metrics: the medians of the
// ratios, the share of identical DOM, anomalous pairs.
func TestFixturesReportDOM(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(100, nil))
	c := snapOf("moli-1", "moli", snapRows(100, func(i int, r *fixturesSnapshotRow) {
		switch {
		case i < 90: // the stable core — the same DOM
		case i < 95: // slightly different (the ratio within tolerance)
			r.ContentHash, r.DOMLen, r.DOMNodes, r.DOMScripts = "ccdd", 1200, 110, 10
		default: // anomalous — three times shorter
			r.ContentHash, r.DOMLen, r.DOMNodes, r.DOMScripts = "eeff", 300, 90, 10
		}
	}))
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("the report: %v", err)
	}
	if r.DOMPairs != 100 || r.DOMIdentical != 90 {
		t.Fatalf("pairs/identical = %d/%d, want 100/90", r.DOMPairs, r.DOMIdentical)
	}
	if r.DOMAnomalousPairs != 5 || r.DOMAnomalousPct != 5 {
		t.Fatalf("anomalous = %d (%.2f%%), want 5 (5%%)", r.DOMAnomalousPairs, r.DOMAnomalousPct)
	}
	if r.DOMMedLenRatio == nil || *r.DOMMedLenRatio != 1 {
		t.Fatalf("the length median = %v, want 1", r.DOMMedLenRatio)
	}
	var buf bytes.Buffer
	fixturesReportPrint(&buf, r)
	for _, want := range []string{"×1.00", "90.00%", "identical DOM"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("the report text without %q:\n%s", want, buf.String())
		}
	}
}

// TestFixturesReportMemoryAndCrashes — the memory thresholds (⅓ of the
// budget) and crashes: the operator's input, the browser-class proxy,
// degradation.
func TestFixturesReportMemoryAndCrashes(t *testing.T) {
	newSnap := func(label string, rss *int64, crashes *int64, metrics *fixturesSnapshotMetrics) *fixturesSnapshot {
		s := snapOf(label, "moli", snapRows(10, nil))
		s.EngineRSSMax, s.EngineCrashes, s.Metrics = rss, crashes, metrics
		return s
	}
	b := snapOf("chromium-1", "chromium", snapRows(10, nil))

	t.Run("memory within the threshold", func(t *testing.T) {
		rss := int64(300 << 20)
		r, err := fixturesReportBuild(b, newSnap("moli-1", &rss, nil, nil), 2<<30)
		if err != nil {
			t.Fatalf("the report: %v", err)
		}
		if v := verdictOf(t, r, "memory"); v == nil || v.Status != verdictPass {
			t.Fatalf("the memory verdict = %+v, want PASSED", v)
		}
	})
	t.Run("memory over the threshold", func(t *testing.T) {
		rss := int64(800 << 20)
		r, err := fixturesReportBuild(b, newSnap("moli-1", &rss, nil, nil), 2<<30)
		if err != nil {
			t.Fatalf("the report: %v", err)
		}
		if v := verdictOf(t, r, "memory"); v == nil || v.Status != verdictFail {
			t.Fatalf("the memory verdict = %+v, want FAILED (800 MiB ≥ ⅓×2 GiB)", v)
		}
	})
	t.Run("memory not set", func(t *testing.T) {
		r, err := fixturesReportBuild(b, newSnap("moli-1", nil, nil, nil), 2<<30)
		if err != nil {
			t.Fatalf("the report: %v", err)
		}
		if v := verdictOf(t, r, "memory"); v == nil || v.Status != verdictNone {
			t.Fatalf("the memory verdict = %+v, want NO DATA", v)
		}
	})
	t.Run("crashes by the operator", func(t *testing.T) {
		two := int64(2)
		r, err := fixturesReportBuild(b, newSnap("moli-1", nil, &two, nil), 2<<30)
		if err != nil {
			t.Fatalf("the report: %v", err)
		}
		if v := verdictOf(t, r, "crashes"); v == nil || v.Status != verdictFail {
			t.Fatalf("the crashes verdict = %+v, want FAILED", v)
		}
	})
	t.Run("crashes by the browser class", func(t *testing.T) {
		m := &fixturesSnapshotMetrics{ProcessStartSeconds: 5, Tasks: 10, SecondsCount: 10, Errors: map[string]float64{"browser": 3}}
		r, err := fixturesReportBuild(b, newSnap("moli-1", nil, nil, m), 2<<30)
		if err != nil {
			t.Fatalf("the report: %v", err)
		}
		if v := verdictOf(t, r, "crashes"); v == nil || v.Status != verdictFail {
			t.Fatalf("the crashes verdict = %+v, want FAILED (browser=3)", v)
		}
	})
	t.Run("crashes without sources", func(t *testing.T) {
		r, err := fixturesReportBuild(b, newSnap("moli-1", nil, nil, nil), 2<<30)
		if err != nil {
			t.Fatalf("the report: %v", err)
		}
		if v := verdictOf(t, r, "crashes"); v == nil || v.Status != verdictNone {
			t.Fatalf("the crashes verdict = %+v, want NO DATA", v)
		}
	})
}

// TestFixturesReportCumulative — the worker was not restarted between
// the snapshots: the counters are cumulative — a warning and a
// degradation of the verdicts tied to the candidate's metrics (the
// browser class).
func TestFixturesReportCumulative(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(10, nil))
	b.Metrics = &fixturesSnapshotMetrics{ProcessStartSeconds: 42, Tasks: 10}
	c := snapOf("moli-1", "moli", snapRows(10, nil))
	c.Metrics = &fixturesSnapshotMetrics{ProcessStartSeconds: 42, Tasks: 15, Errors: map[string]float64{"browser": 7}}
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("the report: %v", err)
	}
	if !r.Cumulative {
		t.Fatalf("the counters were not recognized as cumulative")
	}
	if len(r.Warnings) == 0 {
		t.Errorf("no warning about the cumulative counters")
	}
	if r.IndepLossPctC != nil {
		t.Errorf("the losses over the cumulative counters = %v, want nil", r.IndepLossPctC)
	}
	if v := verdictOf(t, r, "crashes"); v == nil || v.Status != verdictNone {
		t.Fatalf("the crashes verdict = %+v, want NO DATA (cumulative metrics)", v)
	}
}

// TestFixturesReportKeysMismatch — snapshots over different selections:
// the report refuses to compare (an error) rather than comparing apples
// with oranges.
func TestFixturesReportKeysMismatch(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(10, nil))
	c := snapOf("moli-1", "moli", snapRows(10, func(i int, r *fixturesSnapshotRow) {
		r.DomainID += 100 // a different dump
	}))
	if _, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget); err == nil {
		t.Fatalf("the report over different selections did not refuse")
	}
}

// TestFixturesReportPendingWork — an incomplete run: the q:render/
// q:render-fallback leftovers are visible as warnings.
func TestFixturesReportPendingWork(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(10, nil))
	c := snapOf("moli-1", "moli", snapRows(10, nil))
	c.Streams = &fixturesSnapshotStreams{RenderLen: 4, FallbackLen: 2}
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("the report: %v", err)
	}
	joined := strings.Join(r.Warnings, "\n")
	for _, want := range []string{"q:render-fallback: 2", "q:render"} {
		if !strings.Contains(joined, want) {
			t.Errorf("the warnings without %q: %s", want, joined)
		}
	}
}

// TestFixturesReportEmptyMetrics — the candidate's empty metrics (a
// foreign worker, or one restarted after the run): the crashes verdict
// degrades, there is a warning (review T-244 P2-1).
func TestFixturesReportEmptyMetrics(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(10, nil))
	b.Metrics = &fixturesSnapshotMetrics{ProcessStartSeconds: 1, Tasks: 5, SecondsCount: 5, SecondsSum: 10}
	c := snapOf("moli-1", "moli", snapRows(10, nil))
	c.Metrics = &fixturesSnapshotMetrics{ProcessStartSeconds: 2} // Tasks=0, SecondsCount=0
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("the report: %v", err)
	}
	if !r.MetricsEmptyC || r.MetricsValidC {
		t.Fatalf("the empty metrics were not recognized: empty=%v valid=%v", r.MetricsEmptyC, r.MetricsValidC)
	}
	if r.IndepLossPctC != nil {
		t.Errorf("the losses over the empty metrics = %v, want nil", r.IndepLossPctC)
	}
	if v := verdictOf(t, r, "crashes"); v == nil || v.Status != verdictNone {
		t.Fatalf("the crashes verdict = %+v, want NO DATA (metrics without events)", v)
	}
	if !strings.Contains(strings.Join(r.Warnings, "\n"), "no events") {
		t.Errorf("no warning about the empty metrics")
	}
}

// TestFixturesReportPaceWithoutStreams — the page time (avg/p95) from
// the histogram is printed even without a pace: the metrics were taken,
// Redis was not (review T-244 P2-2).
func TestFixturesReportPaceWithoutStreams(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(10, nil))
	b.Metrics = &fixturesSnapshotMetrics{
		ProcessStartSeconds: 1, Tasks: 10, SecondsSum: 40, SecondsCount: 10,
		Buckets: []fixturesHistBucket{{LE: 2.5, Count: 5}, {LE: 5, Count: 10}},
	}
	c := snapOf("moli-1", "moli", snapRows(10, nil))
	c.Metrics = &fixturesSnapshotMetrics{
		ProcessStartSeconds: 2, Tasks: 10, SecondsSum: 30, SecondsCount: 10,
		Buckets: []fixturesHistBucket{{LE: 2.5, Count: 8}, {LE: 5, Count: 10}},
	}
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("the report: %v", err)
	}
	if r.PaceB != nil || r.PaceC != nil {
		t.Fatalf("the pace without streams = %v/%v, want nil/nil", r.PaceB, r.PaceC)
	}
	var buf bytes.Buffer
	fixturesReportPrint(&buf, r)
	text := buf.String()
	if !strings.Contains(text, "no pace data") {
		t.Errorf("the text without the honest pace degradation:\n%s", text)
	}
	for _, want := range []string{"avg 4.00 s", "p95 4.75 s", "avg 3.00 s"} {
		if !strings.Contains(text, want) {
			t.Errorf("the text without %q (the page time was lost without the pace):\n%s", want, text)
		}
	}
}

// TestFixturesReportDOMScriptZero — pairs where one of the runs executed
// no <script> at all: they are not in the medians but are visible as a
// separate metric (review T-244 P2-3).
func TestFixturesReportDOMScriptZero(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(100, nil))
	c := snapOf("moli-1", "moli", snapRows(100, func(i int, r *fixturesSnapshotRow) {
		if i < 3 {
			r.DOMScripts = 0 // the candidate executed no scripts
		}
	}))
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("the report: %v", err)
	}
	if r.DOMScriptZeroPairs != 3 {
		t.Fatalf("pairs with zero scripts %d, want 3", r.DOMScriptZeroPairs)
	}
	if r.DOMMedScriptsRatio == nil || *r.DOMMedScriptsRatio != 1 {
		t.Fatalf("the scripts median = %v, want 1 (zeros do not enter the median)", r.DOMMedScriptsRatio)
	}
	var buf bytes.Buffer
	fixturesReportPrint(&buf, r)
	if !strings.Contains(buf.String(), "no <script>: 3") {
		t.Errorf("the text without the lost scripts metric:\n%s", buf.String())
	}
}

// TestFixturesReportTruncatedKeys — a snapshot with a truncated set of
// rows (dump rows vanished from the DB): a warning, not a silent
// denominator (review T-244 P2-4).
func TestFixturesReportTruncatedKeys(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(10, nil))
	b.Dump.Keys = 12 // the dump counted 12, 10 remain in the DB
	c := snapOf("moli-1", "moli", snapRows(10, nil))
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("the report: %v", err)
	}
	joined := strings.Join(r.Warnings, "\n")
	if !strings.Contains(joined, "10 rows in the snapshot with 12 dump keys") {
		t.Errorf("no warning about the selection truncation: %s", joined)
	}
	if r.Keys != 10 {
		t.Errorf("the denominator = %d, want 10", r.Keys)
	}
}

// TestFixturesReportPairSanity — the pair's sanity checks: identical
// labels and a non-chromium baseline — warnings (review T-244 P3-4).
func TestFixturesReportPairSanity(t *testing.T) {
	b := snapOf("run", "moli", snapRows(10, nil))
	c := snapOf("run", "moli", snapRows(10, nil))
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("the report: %v", err)
	}
	joined := strings.Join(r.Warnings, "\n")
	if !strings.Contains(joined, "the snapshot labels are the same") {
		t.Errorf("no warning about the same labels: %s", joined)
	}
	if !strings.Contains(joined, "baseline-chromium") {
		t.Errorf("no warning about the baseline engine: %s", joined)
	}
}

// TestFixturesHistQuantileSentinelTail — a rank above the cumulative
// with the +Inf sentinel as the last bucket: the last finite bound, not
// MaxFloat64 (review T-244 P3-2).
func TestFixturesHistQuantileSentinelTail(t *testing.T) {
	buckets := []fixturesHistBucket{
		{0.5, 10}, {5, 60}, {math.MaxFloat64, 70},
	}
	got := fixturesHistQuantile(buckets, 70, 0.99)
	if got == nil || *got != 5 {
		t.Fatalf("p99 = %v, want 5 (not the sentinel)", got)
	}
	if got := fixturesHistQuantile([]fixturesHistBucket{{math.MaxFloat64, 5}}, 5, 0.5); got != nil {
		t.Fatalf("the only sentinel bucket = %v, want nil", got)
	}
}

// TestFixturesReportFullText — the complete report: all §5.1 item 3
// sections and all item 4 verdicts are present in the text.
func TestFixturesReportFullText(t *testing.T) {
	rssB, rssC := int64(768<<20), int64(96<<20)
	zero := int64(0)
	b := snapOf("chromium-1", "chromium", snapRows(100, nil))
	b.EngineRSSMax = &rssB
	b.Streams = &fixturesSnapshotStreams{AnalyzeLen: 100, AnalyzeFirstMs: 1000, AnalyzeLastMs: 20000}
	b.Metrics = &fixturesSnapshotMetrics{
		ProcessStartSeconds: 1, Tasks: 100, SecondsSum: 400, SecondsCount: 100,
		Buckets:         []fixturesHistBucket{{LE: 2.5, Count: 50}, {LE: 5, Count: 95}, {LE: 30, Count: 100}},
		TarpitFinalized: 3,
		TaskFinalized:   map[string]float64{"net::ERR_CONNECTION_REFUSED": 2, "timeout": 1},
	}
	c := snapOf("moli-1", "moli", snapRows(100, func(i int, r *fixturesSnapshotRow) {
		if i >= 98 {
			r.Rendered, r.HasContent, r.ContentHash, r.DOMLen = false, false, "", 0
		}
	}))
	c.EngineRSSMax = &rssC
	c.EngineCrashes = &zero
	c.Streams = &fixturesSnapshotStreams{AnalyzeLen: 100, AnalyzeFirstMs: 500, AnalyzeLastMs: 20500}
	c.Metrics = &fixturesSnapshotMetrics{
		ProcessStartSeconds: 2, Tasks: 100, SecondsSum: 300, SecondsCount: 100,
		Buckets:      []fixturesHistBucket{{LE: 2.5, Count: 70}, {LE: 5, Count: 99}, {LE: 30, Count: 100}},
		DNSFinalized: 1, CertFinalized: 1,
		TaskFinalized:     map[string]float64{"timeout": 2},
		FallbackPublished: map[string]float64{"timeout": 2},
		Errors:            map[string]float64{"task": 4},
	}
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("the report: %v", err)
	}
	var buf bytes.Buffer
	fixturesReportPrint(&buf, r)
	text := buf.String()
	for _, want := range []string{
		"Render success",
		"Pace and page time",
		"Finalization reasons",
		"net::ERR_CONNECTION_REFUSED",
		"Laravel detection diff",
		"Structural DOM metrics",
		"Engine RSS memory",
		"Verdicts against the §5.1 item 4 thresholds",
		"Laravel detection < 1%",
		"success",
		"pace not worse than baseline",
		"memory < ⅓ of the chromium budget",
		"0 engine crashes",
		"fallback publications",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the report text without %q:\n%s", want, text)
		}
	}
}

// TestFixturesReasonList — the reason dictionary as a string: sorting by
// count, determinism on ties.
func TestFixturesReasonList(t *testing.T) {
	got := fixturesReasonList(map[string]float64{"timeout": 5, "net::ERR_CONNECTION_REFUSED": 7, "other": 5})
	if got != "net::ERR_CONNECTION_REFUSED: 7, other: 5, timeout: 5" {
		t.Errorf("reasonList = %q", got)
	}
	if got := fixturesReasonList(nil); got != "—" {
		t.Errorf("the empty dictionary = %q", got)
	}
}

// TestFixturesFormatBytes — human-readable bytes for the report lines.
func TestFixturesFormatBytes(t *testing.T) {
	cases := map[int64]string{
		512:                         "512 B",
		2048:                        "2.0 KiB",
		5 << 20:                     "5.0 MiB",
		768 << 20:                   "768.0 MiB",
		fixturesDefaultMemoryBudget: "2.00 GiB",
	}
	for v, want := range cases {
		if got := fixturesFormatBytes(v); got != want {
			t.Errorf("formatBytes(%d) = %q, want %q", v, got, want)
		}
	}
}

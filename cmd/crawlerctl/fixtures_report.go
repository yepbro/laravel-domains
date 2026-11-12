// ---------------------------------------------------------------- report
//
// crawlerctl fixtures report (T-244, the render engine selection task
// spec tasks/TZ_render_engines.md §5.1 items 3–4, §6.1 item 5):
// comparing two run snapshots (fixtures snapshot) — baseline-chromium
// and the candidate engine — by the §5.1 item 3 metrics with explicit
// verdict lines against the canary admission thresholds of §5.1 item 4:
//
//	the share of successful renders;  finalization reasons (the
//	                                  net::ERR_* dictionary of
//	                                  taskFinalizeReason);
//	pace (rows/s, avg/p95);           the engine's RSS memory (<⅓ of the
//	                                  chromium budget);
//	the Laravel detection diff (<1%, the T-213 protocol — the
//	candidate's analyzer verdicts against baseline-chromium);
//	structural DOM metrics (length, nodes, scripts) — a normalized
//	comparison (medians of ratios); a byte-level diff is not applied:
//	the live DOM is non-deterministic;
//	0 engine core crashes.
//
// The comparison degrades honestly: a source missing from the snapshot
// (metrics not taken, the analyzer has not finished the tail, the
// engine RSS not set by the operator) yields a NO DATA line (the
// verdictNone constant, Russian until stage 5) instead of a verdict —
// the report invents no values and hides no
// gaps. The metric counters are valid for a single run only if the
// worker was restarted between runs (the process start moment is in the
// snapshot); otherwise they are cumulative, and the verdicts tied to
// them degrade with a warning.

package main

import (
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
)

// fixturesDefaultMemoryBudget — the chromium worker's memory budget for
// the §5.1 item 4 threshold "memory <⅓ of the chromium budget": the
// render circuit's production budget ≈2 GB (spec 3.2; docs/deploy.md
// §2). The --memory-budget flag overrides it for a bench with a
// different profile.
const fixturesDefaultMemoryBudget = 2 << 30

// Verdict line statuses.
const (
	verdictPass = "PASSED"
	verdictFail = "FAILED"
	verdictNone = "NO DATA"
)

// fixturesReportVerdict — one verdict line against a §5.1 item 4
// threshold.
type fixturesReportVerdict struct {
	Name   string // the threshold wording
	Status string // verdictPass | verdictFail | verdictNone
	Detail string // the numbers the verdict was based on
}

// fixturesReport — the comparison computation over two snapshots: all
// §5.1 item 3 metrics and the item 4 verdicts. A structure (not text) —
// so the tests check the substance; printing — fixturesReportPrint.
type fixturesReport struct {
	Keys            int
	Seed            int64
	BaselineLabel   string
	CandidateLabel  string
	BaselineEngine  string
	CandidateEngine string
	Warnings        []string

	SuccessRenderedB int // successful renders (rendered + content) / Tasks
	SuccessRenderedC int
	SuccessPctB      float64
	SuccessPctC      float64
	IndepLossPctC    *float64 // the candidate's engine-independent losses: (dns+cert)/tasks, % (nil — no metrics)

	PaceB      *float64 // rows/s over the q:analyze boundaries (nil — no data)
	PaceC      *float64
	PaceTasksB int64
	PaceTasksC int64
	PaceSecB   float64
	PaceSecC   float64
	AvgB       *float64 // the average page time over the histogram, s
	AvgC       *float64
	P95B       *float64 // the p95 page time, s
	P95C       *float64

	RSSB   *int64 // the baseline engine's max RSS, bytes
	RSSC   *int64
	Budget int64 // the chromium budget for the ⅓ threshold

	CrashesC    *int64  // the candidate's engine core crashes (the operator)
	BrowserErrC float64 // the candidate's browser-class errors (the engine unavailable, proxy)

	DetectionComparable int
	DetectionDiff       int
	DetectionRatePct    float64
	DetectionFoundB     int
	DetectionFoundC     int

	// Finalization reasons — the taskFinalizeReason dictionary (net::ERR_*),
	// tarpit/cert/DNS finalizations and fallback publications (§5.1
	// item 3). nil — the run's metrics were not taken.
	FinalizeB *fixturesSnapshotMetrics
	FinalizeC *fixturesSnapshotMetrics

	// The candidate's metrics are valid for a single run (the worker was
	// restarted between the snapshots and there are events); false — not
	// taken, cumulative or empty (a foreign worker, or one restarted
	// after the run: Tasks and SecondsCount are zero, review T-244
	// P2-1).
	MetricsValidC bool
	MetricsEmptyC bool
	Cumulative    bool

	DOMPairs           int
	DOMIdentical       int
	DOMIdenticalPct    float64
	DOMMedLenRatio     *float64 // the median of the candidate/baseline ratios
	DOMMedNodesRatio   *float64
	DOMMedScriptsRatio *float64
	DOMAnomalousPairs  int
	DOMAnomalousPct    float64
	DOMScriptZeroPairs int // pairs where one of the runs executed no <script> at all

	Verdicts []fixturesReportVerdict
	Fail     bool // at least one failed verdict (verdictFail)
}

// ---------------------------------------------------------------- calculations

// fixturesSuccessCount — a run's successful renders: rendered + a content
// reference (after the T-243 import a reference appears only for a DOM
// written by the run).
func fixturesSuccessCount(snap *fixturesSnapshot) int {
	n := 0
	for _, r := range snap.Rows {
		if r.Rendered && r.HasContent {
			n++
		}
	}
	return n
}

// fixturesPace — the run's pace (rows/s) over the q:analyze stream: the
// worker publishes records as it processes, the (N−1) intervals between
// the first and the last give the average pace of the rework stream.
// nil — no data (the stream was not captured or has fewer than two
// records).
func fixturesPace(s *fixturesSnapshotStreams) (pace *float64, tasks int64, sec float64) {
	if s == nil || s.AnalyzeLen < 2 || s.AnalyzeLastMs <= s.AnalyzeFirstMs {
		return nil, 0, 0
	}
	sec = float64(s.AnalyzeLastMs-s.AnalyzeFirstMs) / 1000
	v := float64(s.AnalyzeLen-1) / sec
	return &v, s.AnalyzeLen, sec
}

// fixturesHistQuantile — a quantile over the histogram's cumulative
// buckets (interpolation like Prometheus histogram_quantile). nil — no
// histogram or quantile. The closing bucket (the MaxFloat64 sentinel
// instead of +Inf): there is no interpolation into infinity — the last
// finite bound is returned (a lower estimate of the tail beyond the
// timeout).
func fixturesHistQuantile(buckets []fixturesHistBucket, count float64, q float64) *float64 {
	if count <= 0 || len(buckets) == 0 {
		return nil
	}
	rank := q * count
	var prevLE, prevCount float64 // the first bucket's lower bound is 0
	for i, b := range buckets {
		if b.Count < rank {
			prevLE, prevCount = b.LE, b.Count
			continue
		}
		if b.Count == prevCount {
			v := b.LE // a flat stretch: all observations at the bound
			return &v
		}
		if b.LE >= math.MaxFloat64 {
			if prevCount == 0 {
				return nil // the only bucket is the sentinel: no finite bounds
			}
			v := prevLE // the rank in the closing bucket: a lower tail estimate
			return &v
		}
		v := prevLE + (rank-prevCount)/(b.Count-prevCount)*(b.LE-prevLE)
		if i == 0 {
			v = b.LE * rank / b.Count // from zero to the first bound
		}
		return &v
	}
	last := buckets[len(buckets)-1].LE
	if last >= math.MaxFloat64 {
		// the +Inf sentinel as the last bound (the rank above the cumulative
		// of a broken exposition): the last finite bound is more honest
		// than 1.8e308 s (review T-244 P3-2)
		if len(buckets) > 1 {
			last = buckets[len(buckets)-2].LE
		} else {
			return nil
		}
	}
	return &last
}

// fixturesHistAvg — the average over the histogram's sum/count.
func fixturesHistAvg(sum, count float64) *float64 {
	if count <= 0 {
		return nil
	}
	v := sum / count
	return &v
}

// fixturesMedian — the median of a slice (the slice is not mutated: a
// copy).
func fixturesMedian(values []float64) *float64 {
	if len(values) == 0 {
		return nil
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	var v float64
	if len(sorted)%2 == 1 {
		v = sorted[mid]
	} else {
		v = (sorted[mid-1] + sorted[mid]) / 2
	}
	return &v
}

// fixturesRowMap — the snapshot's rows by key.
func fixturesRowMap(snap *fixturesSnapshot) map[fixturesKey]fixturesSnapshotRow {
	m := make(map[fixturesKey]fixturesSnapshotRow, len(snap.Rows))
	for _, r := range snap.Rows {
		m[fixturesKey{r.ZoneID, r.DomainID, r.SubdomainID, r.Variant}] = r
	}
	return m
}

// fixturesKeysUnionMismatch — a mismatch of the two snapshots' key sets
// (taken over different dumps): the report requires one selection — the
// keys foreign to baseline and candidate are returned.
func fixturesKeysUnionMismatch(b, c map[fixturesKey]fixturesSnapshotRow) (onlyB, onlyC int) {
	for k := range b {
		if _, ok := c[k]; !ok {
			onlyB++
		}
	}
	for k := range c {
		if _, ok := b[k]; !ok {
			onlyC++
		}
	}
	return onlyB, onlyC
}

// fixturesDetection — the Laravel detection diff (the T-213 protocol):
// the candidate's analyzer verdicts against baseline row by row.
// Comparable rows are those written by the run in both cases (content
// present) and stamped by that run's analyzer (the row's rules_version
// NOT NULL: the render clears the stamp, the analyzer sets it — NULL
// means "the analyzer has not finished"; findings in the DB could be
// left over from a previous run, review T-244 P1-2). A row's verdict is
// is_laravel when findings exist and false when they do not: the
// analyzer DELETES the findings row at zero hits (findingDeleteSQL) — a
// missing row is the normal "no signs", not "not finished"; otherwise a
// full loss/appearance of signals would fall out of the diff (review
// T-244 P1-1). The findings' rule versions are returned for a cross-
// check: different rule sets make the <1% threshold incomparable.
func fixturesDetection(b, c map[fixturesKey]fixturesSnapshotRow) (comparable, diff, foundB, foundC int, rulesB, rulesC map[int32]int) {
	rulesB, rulesC = map[int32]int{}, map[int32]int{}
	for k, rb := range b {
		rc, ok := c[k]
		if !ok {
			continue
		}
		if !rb.HasContent || !rc.HasContent || rb.RulesVersion == nil || rc.RulesVersion == nil {
			continue // not written by the run or not analyzed in it
		}
		comparable++
		if rb.FindingRulesVersion != nil {
			rulesB[*rb.FindingRulesVersion]++
		}
		if rc.FindingRulesVersion != nil {
			rulesC[*rc.FindingRulesVersion]++
		}
		if rb.LaravelFound && rb.IsLaravel {
			foundB++
		}
		if rc.LaravelFound && rc.IsLaravel {
			foundC++
		}
		if rb.LaravelFound != rc.LaravelFound || rb.IsLaravel != rc.IsLaravel {
			diff++
		}
	}
	return comparable, diff, foundB, foundC, rulesB, rulesC
}

// fixturesDOMCompare — structural DOM metrics: key pairs with content in
// both runs; an identical DOM (content_hash — the store's dedup), the
// medians of the candidate/baseline ratios (length/nodes/scripts —
// count the structure, not bytes: the live DOM is non-deterministic)
// and the share of anomalous pairs by length (outside ×0.5…×2 — the
// engine assembled a fundamentally different page); plus the pairs where
// one of the runs executed NOT A SINGLE script (they are not in the
// medians — a ratio to zero is meaningless, but losing all <script> is
// a signal of its own, review T-244 P2-3).
func fixturesDOMCompare(b, c map[fixturesKey]fixturesSnapshotRow) (pairs, identical, anomalous, scriptZero int, medLen, medNodes, medScripts *float64) {
	var lenRatios, nodeRatios, scriptRatios []float64
	for k, rb := range b {
		rc, ok := c[k]
		if !ok || !rb.HasContent || !rc.HasContent {
			continue
		}
		pairs++
		if rb.ContentHash != "" && rb.ContentHash == rc.ContentHash {
			identical++
		}
		if rb.DOMLen > 0 && rc.DOMLen > 0 {
			lenRatios = append(lenRatios, float64(rc.DOMLen)/float64(rb.DOMLen))
			if r := float64(rc.DOMLen) / float64(rb.DOMLen); r < 0.5 || r > 2 {
				anomalous++
			}
		}
		if rb.DOMParsed && rc.DOMParsed {
			if rb.DOMNodes > 0 && rc.DOMNodes > 0 {
				nodeRatios = append(nodeRatios, float64(rc.DOMNodes)/float64(rb.DOMNodes))
			}
			switch {
			case rb.DOMScripts > 0 && rc.DOMScripts > 0:
				scriptRatios = append(scriptRatios, float64(rc.DOMScripts)/float64(rb.DOMScripts))
			case rb.DOMScripts == 0 && rc.DOMScripts == 0:
				// no scripts in either — normal for static pages
			default:
				scriptZero++
			}
		}
	}
	return pairs, identical, anomalous, scriptZero,
		fixturesMedian(lenRatios), fixturesMedian(nodeRatios), fixturesMedian(scriptRatios)
}

// fixturesMetricsCumulative — are a snapshot pair's counters cumulative:
// the metrics were taken in both, but the worker process is one (the
// start did not change) — the candidate's values include the baseline's
// run.
func fixturesMetricsCumulative(b, c *fixturesSnapshot) bool {
	return b.Metrics != nil && c.Metrics != nil &&
		b.Metrics.ProcessStartSeconds != 0 &&
		b.Metrics.ProcessStartSeconds == c.Metrics.ProcessStartSeconds
}

// fixturesFinalizedSum — the total finalizations of the task class (by
// the reason dictionary) — for the reason lines.
func fixturesFinalizedSum(m map[string]float64) float64 {
	var sum float64
	for _, v := range m {
		sum += v
	}
	return sum
}

// fixturesReportBuild — the comparison computation: the §5.1 item 3
// metrics + the item 4 verdicts. An error — the snapshots are over
// different selections (comparing further is pointless).
func fixturesReportBuild(b, c *fixturesSnapshot, budget int64) (*fixturesReport, error) {
	rowsB, rowsC := fixturesRowMap(b), fixturesRowMap(c)
	if onlyB, onlyC := fixturesKeysUnionMismatch(rowsB, rowsC); onlyB+onlyC > 0 {
		return nil, fmt.Errorf("the snapshots were taken over different selections: %d keys only in baseline, %d only in candidate (the dumps/strata diverge — the comparison is invalid)", onlyB, onlyC)
	}

	r := &fixturesReport{
		Keys:            len(b.Rows),
		Seed:            b.Dump.Seed,
		BaselineLabel:   b.Label,
		CandidateLabel:  c.Label,
		BaselineEngine:  b.Engine,
		CandidateEngine: c.Engine,
		Budget:          budget,
	}
	if b.Dump.Seed != c.Dump.Seed {
		r.Warnings = append(r.Warnings, fmt.Sprintf("the dump seeds diverge: baseline %d, candidate %d — the selections could have been assembled from different DB states", b.Dump.Seed, c.Dump.Seed))
	}
	// Selection cross-checks: a snapshot with a truncated set of rows
	// (dump rows vanished from the DB) would silently understate the
	// success denominator (P2-4).
	for _, s := range []struct {
		name string
		snap *fixturesSnapshot
	}{
		{"baseline", b}, {"candidate", c},
	} {
		if s.snap.Dump.Keys > 0 && s.snap.Dump.Keys != len(s.snap.Rows) {
			r.Warnings = append(r.Warnings, fmt.Sprintf("%s: %d rows in the snapshot with %d dump keys — the success denominator is truncated", s.name, len(s.snap.Rows), s.snap.Dump.Keys))
		}
	}
	if b.Label == c.Label {
		r.Warnings = append(r.Warnings, "the snapshot labels are the same — make sure these are different runs, not one file")
	}
	if b.Engine != "chromium" {
		r.Warnings = append(r.Warnings, fmt.Sprintf("the baseline engine %q — spec §5.1 requires baseline-chromium (a fresh chromium run over the same selection)", b.Engine))
	}
	cumulative := fixturesMetricsCumulative(b, c)
	if cumulative {
		r.Warnings = append(r.Warnings, "the worker was not restarted between the runs (process_start_time matches): the candidate counters are cumulative — the finalization reasons, the page time and the browser errors include the baseline run, the verdicts over them are degraded; the pace over the q:analyze streams and the success over the DB rows are valid")
	}
	// Run incompleteness is visible from the stream leftovers; BOTH runs
	// are checked: an incomplete baseline would weaken the success
	// threshold for the candidate (P2-5); q:dead silently reduces any
	// run's success.
	for _, s := range []struct {
		name string
		snap *fixturesSnapshot
	}{
		{"baseline", b}, {"candidate", c},
	} {
		if s.snap.Streams == nil {
			continue
		}
		if s.snap.Streams.RenderLen > 0 {
			r.Warnings = append(r.Warnings, fmt.Sprintf("q:render %s is not empty (%d records) — the run is not finished (or Trim did not reach it), the pace and the success reflect an unfinished run", s.name, s.snap.Streams.RenderLen))
		}
		if s.snap.Streams.FallbackLen > 0 {
			r.Warnings = append(r.Warnings, fmt.Sprintf("the fallback %s is not drained (q:render-fallback: %d records) — the success is understated, take the snapshot after the drain", s.name, s.snap.Streams.FallbackLen))
		}
		if s.snap.Streams.DeadLen > 0 {
			r.Warnings = append(r.Warnings, fmt.Sprintf("q:dead %s: %d records — the tasks exhausted their deliveries, the success and the reasons are worth triaging", s.name, s.snap.Streams.DeadLen))
		}
	}

	// Success: rows with the run's DOM / the selection's tasks.
	r.SuccessRenderedB = fixturesSuccessCount(b)
	r.SuccessRenderedC = fixturesSuccessCount(c)
	r.SuccessPctB = pct(r.SuccessRenderedB, r.Keys)
	r.SuccessPctC = pct(r.SuccessRenderedC, r.Keys)
	if c.Metrics != nil && !cumulative && c.Metrics.Tasks > 0 {
		v := (c.Metrics.DNSFinalized + c.Metrics.CertFinalized) / c.Metrics.Tasks * 100
		r.IndepLossPctC = &v
	}

	// Pace and page time.
	r.PaceB, r.PaceTasksB, r.PaceSecB = fixturesPace(b.Streams)
	r.PaceC, r.PaceTasksC, r.PaceSecC = fixturesPace(c.Streams)
	if b.Metrics != nil {
		r.AvgB = fixturesHistAvg(b.Metrics.SecondsSum, b.Metrics.SecondsCount)
		r.P95B = fixturesHistQuantile(b.Metrics.Buckets, b.Metrics.SecondsCount, 0.95)
	}
	if c.Metrics != nil {
		r.AvgC = fixturesHistAvg(c.Metrics.SecondsSum, c.Metrics.SecondsCount)
		r.P95C = fixturesHistQuantile(c.Metrics.Buckets, c.Metrics.SecondsCount, 0.95)
	}

	// Memory and crashes.
	r.RSSB, r.RSSC = b.EngineRSSMax, c.EngineRSSMax
	r.CrashesC = c.EngineCrashes
	r.FinalizeB, r.FinalizeC = b.Metrics, c.Metrics
	r.Cumulative = cumulative
	r.MetricsEmptyC = c.Metrics != nil && !cumulative &&
		c.Metrics.Tasks == 0 && c.Metrics.SecondsCount == 0
	r.MetricsValidC = c.Metrics != nil && !cumulative && !r.MetricsEmptyC
	if r.MetricsEmptyC {
		r.Warnings = append(r.Warnings, "the candidate metrics have no events (0 tasks and time observations): --metrics points at an empty/foreign worker or the worker was restarted after the run — the metric verdicts are degraded")
	}
	if r.MetricsValidC {
		r.BrowserErrC = c.Metrics.Errors["browser"]
		if fb := fixturesFinalizedSum(c.Metrics.FallbackPublished); fb > 0 {
			r.Warnings = append(r.Warnings, fmt.Sprintf("fallback publications during the candidate run: %.0f — with the worker B drained they already count into the success (take the snapshot after the q:render-fallback drain); without B the success verdict is \"the engine without a fallback\", stricter than the spec §5.1 item 4 threshold", fb))
		}
	}

	// Detection diff.
	comparable, diff, foundB, foundC, rulesB, rulesC := fixturesDetection(rowsB, rowsC)
	r.DetectionComparable = comparable
	r.DetectionDiff = diff
	r.DetectionFoundB = foundB
	r.DetectionFoundC = foundC
	r.DetectionRatePct = pct(diff, comparable)
	rulesDiffer := len(rulesB) != len(rulesC)
	if !rulesDiffer {
		for v := range rulesB {
			if _, ok := rulesC[v]; !ok {
				rulesDiffer = true
				break
			}
		}
	}
	if rulesDiffer {
		r.Warnings = append(r.Warnings, "the analyzer rule versions diverge between the runs (rules_version) — the <1% detection threshold compares different rule sets, the verdict is degraded")
	}

	// Structural DOM metrics.
	r.DOMPairs, r.DOMIdentical, r.DOMAnomalousPairs, r.DOMScriptZeroPairs, r.DOMMedLenRatio, r.DOMMedNodesRatio, r.DOMMedScriptsRatio =
		fixturesDOMCompare(rowsB, rowsC)
	r.DOMIdenticalPct = pct(r.DOMIdentical, r.DOMPairs)
	r.DOMAnomalousPct = pct(r.DOMAnomalousPairs, r.DOMPairs)

	r.Verdicts = fixturesReportVerdicts(r, rulesDiffer)
	for _, v := range r.Verdicts {
		if v.Status == verdictFail {
			r.Fail = true
		}
	}
	return r, nil
}

// fixturesReportVerdicts — verdict lines against the §5.1 item 4
// thresholds. The order follows the spec's text: detection, success,
// pace, memory, crashes.
func fixturesReportVerdicts(r *fixturesReport, rulesDiffer bool) []fixturesReportVerdict {
	var out []fixturesReportVerdict

	// Laravel detection < 1% (T-213).
	switch {
	case r.DetectionComparable == 0:
		out = append(out, fixturesReportVerdict{Name: "Laravel detection < 1%",
			Status: verdictNone, Detail: "no comparable rows (the run wrote no DOM or the analyzer has not stamped results.rules_version — wait for the analyzer drain)"})
	case rulesDiffer:
		out = append(out, fixturesReportVerdict{Name: "Laravel detection < 1%",
			Status: verdictNone, Detail: fmt.Sprintf("the analyzer rules changed between the runs; %s divergences (%d of %d)", pctf(r.DetectionRatePct), r.DetectionDiff, r.DetectionComparable)})
	case r.DetectionRatePct < 1:
		out = append(out, fixturesReportVerdict{Name: "Laravel detection < 1%",
			Status: verdictPass, Detail: fmt.Sprintf("%s divergences (%d of %d comparable)", pctf(r.DetectionRatePct), r.DetectionDiff, r.DetectionComparable)})
	default:
		out = append(out, fixturesReportVerdict{Name: "Laravel detection < 1%",
			Status: verdictFail, Detail: fmt.Sprintf("%s divergences (%d of %d comparable)", pctf(r.DetectionRatePct), r.DetectionDiff, r.DetectionComparable)})
	}

	// Success: candidate ≥ baseline − engine-independent losses (DNS/cert).
	switch {
	case r.IndepLossPctC != nil && r.SuccessPctC >= r.SuccessPctB-*r.IndepLossPctC:
		out = append(out, fixturesReportVerdict{Name: "success ≥ baseline − engine-independent losses",
			Status: verdictPass, Detail: fmt.Sprintf("%s ≥ %s − %s", pctf(r.SuccessPctC), pctf(r.SuccessPctB), pctf(*r.IndepLossPctC))})
	case r.IndepLossPctC != nil:
		out = append(out, fixturesReportVerdict{Name: "success ≥ baseline − engine-independent losses",
			Status: verdictFail, Detail: fmt.Sprintf("%s < %s − %s (DNS/cert)", pctf(r.SuccessPctC), pctf(r.SuccessPctB), pctf(*r.IndepLossPctC))})
	case r.SuccessPctC >= r.SuccessPctB:
		// The losses are unknown (no metrics) — a strict comparison: it
		// raises the bar for the candidate but lets no regression
		// through.
		out = append(out, fixturesReportVerdict{Name: "success ≥ baseline (strict: the DNS/cert losses are unknown)",
			Status: verdictPass, Detail: fmt.Sprintf("%s ≥ %s", pctf(r.SuccessPctC), pctf(r.SuccessPctB))})
	default:
		out = append(out, fixturesReportVerdict{Name: "success ≥ baseline (strict: the DNS/cert losses are unknown)",
			Status: verdictFail, Detail: fmt.Sprintf("%s < %s; take the run's --metrics to subtract the engine-independent losses", pctf(r.SuccessPctC), pctf(r.SuccessPctB))})
	}

	// Pace not worse than baseline.
	switch {
	case r.PaceB == nil || r.PaceC == nil:
		out = append(out, fixturesReportVerdict{Name: "pace not worse than baseline",
			Status: verdictNone, Detail: "no pace captured (Redis was unavailable at the snapshot or q:analyze < 2 records)"})
	case *r.PaceC >= *r.PaceB:
		out = append(out, fixturesReportVerdict{Name: "pace not worse than baseline",
			Status: verdictPass, Detail: fmt.Sprintf("%.2f ≥ %.2f rows/s", *r.PaceC, *r.PaceB)})
	default:
		out = append(out, fixturesReportVerdict{Name: "pace not worse than baseline",
			Status: verdictFail, Detail: fmt.Sprintf("%.2f < %.2f rows/s", *r.PaceC, *r.PaceB)})
	}

	// Memory < ⅓ of the chromium budget.
	switch {
	case r.RSSC == nil:
		out = append(out, fixturesReportVerdict{Name: "memory < ⅓ of the chromium budget",
			Status: verdictNone, Detail: "the engine RSS is not set (--engine-rss-max at the candidate's snapshot)"})
	case *r.RSSC*3 < r.Budget:
		out = append(out, fixturesReportVerdict{Name: "memory < ⅓ of the chromium budget",
			Status: verdictPass, Detail: fmt.Sprintf("%s < ⅓ of %s", fixturesFormatBytes(*r.RSSC), fixturesFormatBytes(r.Budget))})
	default:
		out = append(out, fixturesReportVerdict{Name: "memory < ⅓ of the chromium budget",
			Status: verdictFail, Detail: fmt.Sprintf("%s ≥ ⅓ of %s", fixturesFormatBytes(*r.RSSC), fixturesFormatBytes(r.Budget))})
	}

	// 0 engine core crashes.
	switch {
	case r.CrashesC != nil:
		status, word := verdictPass, "0 engine core crashes"
		if *r.CrashesC > 0 {
			status, word = verdictFail, fmt.Sprintf("%d engine core crashes", *r.CrashesC)
		}
		out = append(out, fixturesReportVerdict{Name: "0 engine crashes",
			Status: status, Detail: word + " (the --engine-crashes operator input)"})
	case r.BrowserErrC > 0:
		out = append(out, fixturesReportVerdict{Name: "0 engine crashes",
			Status: verdictFail, Detail: fmt.Sprintf("%.0f browser-class events (the engine unavailable) — a proxy for crashes/core unavailability", r.BrowserErrC)})
	case r.MetricsValidC:
		out = append(out, fixturesReportVerdict{Name: "0 engine crashes",
			Status: verdictPass, Detail: "0 browser-class events by the stage metrics (a proxy; the exact input is --engine-crashes)"})
	default:
		out = append(out, fixturesReportVerdict{Name: "0 engine crashes",
			Status: verdictNone, Detail: "neither the operator input (--engine-crashes) nor valid stage metrics"})
	}
	return out
}

// ---------------------------------------------------------------- printing

// pctf — percentages as a string (2 decimals).
func pctf(v float64) string { return fmt.Sprintf("%.2f%%", v) }

// fixturesFormatBytes — bytes in a human-readable form (KiB/MiB/GiB).
func fixturesFormatBytes(v int64) string {
	switch {
	case v >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(v)/(1<<30))
	case v >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(v)/(1<<20))
	case v >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(v)/(1<<10))
	default:
		return fmt.Sprintf("%d B", v)
	}
}

// ratioOrNone — a ratio as an "×N.NN" string or a dash.
func ratioOrNone(v *float64) string {
	if v == nil {
		return "—"
	}
	return fmt.Sprintf("×%.2f", *v)
}

// fixturesReportPrint — the report text: the §5.1 item 3 metrics and the
// item 4 verdicts (--out gets the same text).
func fixturesReportPrint(w io.Writer, r *fixturesReport) {
	out(w, "Render engine comparison report (the render spec §5.1; T-244)\n")
	out(w, "Selection: %d fixture tasks (dump seed %d)\n", r.Keys, r.Seed)
	out(w, "baseline:  %s (engine %s)\n", r.BaselineLabel, r.BaselineEngine)
	out(w, "candidate: %s (engine %s)\n", r.CandidateLabel, r.CandidateEngine)
	for _, warn := range r.Warnings {
		out(w, "warning: %s\n", warn)
	}
	out(w, "\n")

	out(w, "Render success\n")
	out(w, "  baseline:  %d/%d (%s)", r.SuccessRenderedB, r.Keys, pctf(r.SuccessPctB))
	if r.FinalizeB != nil && r.FinalizeB.RenderedRecords > 0 {
		out(w, " [by the stage metrics: %.0f rendered records]", r.FinalizeB.RenderedRecords)
	}
	out(w, "\n")
	out(w, "  candidate: %d/%d (%s)", r.SuccessRenderedC, r.Keys, pctf(r.SuccessPctC))
	if r.FinalizeC != nil && r.FinalizeC.RenderedRecords > 0 {
		out(w, " [by the stage metrics: %.0f rendered records]", r.FinalizeC.RenderedRecords)
	}
	if r.IndepLossPctC != nil {
		out(w, "; engine-independent losses (DNS/cert): %s", pctf(*r.IndepLossPctC))
	}
	out(w, "\n\n")

	out(w, "Pace and page time\n")
	paceLine := func(name string, pace *float64, tasks int64, sec float64, avg, p95 *float64) {
		if pace != nil {
			out(w, "  %s: %.2f rows/s (%d q:analyze records over %.0f s)", name, *pace, tasks, sec)
		} else {
			out(w, "  %s: no pace data (the q:analyze streams were not captured or < 2 records)", name)
		}
		// Page time is a §5.1 item 3 metric of its own (the stage's
		// histogram): printed even without a pace — the metrics could have
		// been taken without Redis (review T-244 P2-2).
		if avg != nil {
			out(w, "; avg %.2f s", *avg)
		}
		if p95 != nil {
			out(w, "; p95 %.2f s", *p95)
		}
		out(w, "\n")
	}
	paceLine("baseline", r.PaceB, r.PaceTasksB, r.PaceSecB, r.AvgB, r.P95B)
	paceLine("candidate", r.PaceC, r.PaceTasksC, r.PaceSecC, r.AvgC, r.P95C)
	out(w, "\n")

	out(w, "Finalization reasons without a render (the stage metrics, the taskFinalizeReason dictionary)\n")
	finalizeLine := func(name string, m *fixturesSnapshotMetrics) {
		if m == nil {
			out(w, "  %s: no data (a snapshot without --metrics)\n", name)
			return
		}
		out(w, "  %s: tarpit %.0f, cert %.0f, dns %.0f, task class %.0f",
			name, m.TarpitFinalized, m.CertFinalized, m.DNSFinalized, fixturesFinalizedSum(m.TaskFinalized))
		if len(m.TaskFinalized) > 0 {
			out(w, " (%s)", fixturesReasonList(m.TaskFinalized))
		}
		if fb := fixturesFinalizedSum(m.FallbackPublished); fb > 0 {
			out(w, "; fallback publications %.0f (%s)", fb, fixturesReasonList(m.FallbackPublished))
		}
		out(w, "\n")
	}
	finalizeLine("baseline", r.FinalizeB)
	finalizeLine("candidate", r.FinalizeC)
	if r.Cumulative {
		out(w, "  (the worker was not restarted: the candidate counters include the baseline run)\n")
	}
	out(w, "\n")

	out(w, "Laravel detection diff (the T-213 protocol)\n")
	out(w, "  comparable verdicts: %d of %d; Laravel verdicts baseline %d, candidate %d\n",
		r.DetectionComparable, r.Keys, r.DetectionFoundB, r.DetectionFoundC)
	out(w, "  verdict divergences: %d (%s)\n\n", r.DetectionDiff, pctf(r.DetectionRatePct))

	out(w, "Structural DOM metrics (normalized comparison)\n")
	out(w, "  pairs with DOM in both runs: %d; identical DOM (content_hash): %d (%s)\n",
		r.DOMPairs, r.DOMIdentical, pctf(r.DOMIdenticalPct))
	out(w, "  medians of the candidate/baseline ratios: length %s, nodes %s, scripts %s\n",
		ratioOrNone(r.DOMMedLenRatio), ratioOrNone(r.DOMMedNodesRatio), ratioOrNone(r.DOMMedScriptsRatio))
	out(w, "  anomalous pairs by length (outside ×0.5…×2): %d (%s)", r.DOMAnomalousPairs, pctf(r.DOMAnomalousPct))
	if r.DOMScriptZeroPairs > 0 {
		out(w, "; pairs where one of the runs executed no <script>: %d", r.DOMScriptZeroPairs)
	}
	out(w, "\n\n")

	out(w, "Engine RSS memory\n")
	out(w, "  baseline: %s; candidate: %s; the chromium budget %s (⅓ = %s)\n\n",
		rssOrNone(r.RSSB), rssOrNone(r.RSSC), fixturesFormatBytes(r.Budget), fixturesFormatBytes(r.Budget/3))

	out(w, "Verdicts against the §5.1 item 4 thresholds\n")
	for _, v := range r.Verdicts {
		out(w, "  [%s] %s: %s\n", v.Status, v.Name, v.Detail)
	}
}

// rssOrNone — RSS as a string or "no data".
func rssOrNone(v *int64) string {
	if v == nil {
		return "no data"
	}
	return fixturesFormatBytes(*v)
}

// fixturesReasonList — the "reason → count" dictionary as a string (by
// descending count, reason = key): finalization/fallback reasons.
func fixturesReasonList(m map[string]float64) string {
	if len(m) == 0 {
		return "—"
	}
	type pair struct {
		reason string
		n      float64
	}
	pairs := make([]pair, 0, len(m))
	for k, v := range m {
		pairs = append(pairs, pair{k, v})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].n != pairs[j].n {
			return pairs[i].n > pairs[j].n
		}
		return pairs[i].reason < pairs[j].reason
	})
	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = fmt.Sprintf("%s: %.0f", p.reason, p.n)
	}
	return strings.Join(parts, ", ")
}

// ---------------------------------------------------------------- command

// fixturesReportCmd — crawlerctl fixtures report: reading two snapshots,
// the computation, printing (and writing --out with the same text). The
// exit code does not depend on the verdicts (a report is not a gate);
// --fail-exit turns any failed verdict (verdictFail) into code 1 for the
// T-247 cycle's scripts.
func fixturesReportCmd(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("fixtures report")
	baseline := fs.String("baseline", "", "the baseline-chromium snapshot (required)")
	candidate := fs.String("candidate", "", "the candidate engine's snapshot (required)")
	outPath := fs.String("out", "", "the report file (besides stdout; for the T-247 protocol)")
	budget := fs.Int64("memory-budget", fixturesDefaultMemoryBudget, "the chromium memory budget for the ⅓ threshold, bytes")
	failExit := fs.Bool("fail-exit", false, "exit code 1 on any FAILED verdict (for scripts)")
	if handled, err := parseFlags(fs, fixturesHelp, args, stdout); handled {
		return err
	}
	if strings.TrimSpace(*baseline) == "" || strings.TrimSpace(*candidate) == "" {
		return usagef("specify both snapshots: crawlerctl fixtures report --baseline A.snapshot.json --candidate B.snapshot.json")
	}
	if *budget <= 0 {
		return usagef("fixtures report: --memory-budget: a positive value, got %d", *budget)
	}
	if fs.NArg() > 0 {
		return usagef("fixtures report: extra arguments %v (the command takes only flags)", fs.Args())
	}

	snapB, err := fixturesReadSnapshot(*baseline)
	if err != nil {
		return fmt.Errorf("fixtures report: %w", err)
	}
	snapC, err := fixturesReadSnapshot(*candidate)
	if err != nil {
		return fmt.Errorf("fixtures report: %w", err)
	}

	report, err := fixturesReportBuild(snapB, snapC, *budget)
	if err != nil {
		return fmt.Errorf("fixtures report: %w", err)
	}

	var buf strings.Builder
	fixturesReportPrint(&buf, report)
	text := buf.String()
	out(stdout, "%s", text)
	if *outPath != "" {
		// Atomically (tmp + rename, like the snapshot): an interrupted
		// write leaves no half-file of the T-247 protocol (review T-244
		// P3-5).
		tmp := *outPath + ".tmp"
		if err := os.WriteFile(tmp, []byte(text), 0o644); err != nil {
			return fmt.Errorf("fixtures report: writing %s: %w", *outPath, err)
		}
		if err := os.Rename(tmp, *outPath); err != nil {
			_ = os.Remove(tmp)
			return fmt.Errorf("fixtures report: writing %s: %w", *outPath, err)
		}
		_, _ = fmt.Fprintf(stderr, "report written: %s\n", *outPath)
	}
	if *failExit && report.Fail {
		return fmt.Errorf("fixtures report: a FAILED verdict on %s", strings.Join(failedVerdictNames(report), ", "))
	}
	return nil
}

// failedVerdictNames — the names of failed verdicts (the --fail-exit message).
func failedVerdictNames(r *fixturesReport) []string {
	var names []string
	for _, v := range r.Verdicts {
		if v.Status == verdictFail {
			names = append(names, v.Name)
		}
	}
	return names
}

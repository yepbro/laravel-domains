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
	verdictPass = "ПРОЙДЕН"
	verdictFail = "НЕ ПРОЙДЕН"
	verdictNone = "НЕТ ДАННЫХ"
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
		return nil, fmt.Errorf("снапшоты сняты по разным выборкам: %d ключей только в baseline, %d только в candidate (дампы/страты расходятся — сравнение недействительно)", onlyB, onlyC)
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
		r.Warnings = append(r.Warnings, fmt.Sprintf("seed дампов расходится: baseline %d, candidate %d — выборки могли собраться из разных состояний БД", b.Dump.Seed, c.Dump.Seed))
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
			r.Warnings = append(r.Warnings, fmt.Sprintf("%s: в снапшоте %d строк при %d ключах дампа — знаменатель успешности усечён", s.name, len(s.snap.Rows), s.snap.Dump.Keys))
		}
	}
	if b.Label == c.Label {
		r.Warnings = append(r.Warnings, "метки снапшотов совпадают — убедитесь, что это разные прогоны, а не один файл")
	}
	if b.Engine != "chromium" {
		r.Warnings = append(r.Warnings, fmt.Sprintf("baseline-движок %q — ТЗ §5.1 требует baseline-chromium (свежий прогон chromium на той же выборке)", b.Engine))
	}
	cumulative := fixturesMetricsCumulative(b, c)
	if cumulative {
		r.Warnings = append(r.Warnings, "воркер не перезапускался между прогонами (process_start_time совпадает): счётчики candidate накопительные — причины финализаций, время страницы и browser-ошибки включают baseline-прогон, вердикты по ним деградированы; темп по стримам q:analyze и успешность по строкам БД валидны")
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
			r.Warnings = append(r.Warnings, fmt.Sprintf("q:render %s не пуста (%d записей) — прогон не завершён (или Trim не дошёл), темп и успешность отражают недоработанный прогон", s.name, s.snap.Streams.RenderLen))
		}
		if s.snap.Streams.FallbackLen > 0 {
			r.Warnings = append(r.Warnings, fmt.Sprintf("фолбэк %s не доработан (q:render-fallback: %d записей) — успешность занижена, снимайте снапшот после доработки", s.name, s.snap.Streams.FallbackLen))
		}
		if s.snap.Streams.DeadLen > 0 {
			r.Warnings = append(r.Warnings, fmt.Sprintf("q:dead %s: %d записей — задачи исчерпали доставки, успешность и причины стоит разобрать", s.name, s.snap.Streams.DeadLen))
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
		r.Warnings = append(r.Warnings, "метрики candidate без событий (задач и наблюдений времени 0): --metrics указывает на пустой/чужой воркер или воркер перезапущен после прогона — вердикты по метрикам деградированы")
	}
	if r.MetricsValidC {
		r.BrowserErrC = c.Metrics.Errors["browser"]
		if fb := fixturesFinalizedSum(c.Metrics.FallbackPublished); fb > 0 {
			r.Warnings = append(r.Warnings, fmt.Sprintf("публикаций в фолбэк за прогон candidate: %.0f — при доработанном воркером B они уже входят в успешность (снимайте снапшот после дренажа q:render-fallback); без B вердикт успешности — «движок без фолбэка», строже порога ТЗ §5.1 п. 4", fb))
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
		r.Warnings = append(r.Warnings, "версии правил анализатора в прогонах расходятся (rules_version) — порог детекции <1% сравнивает разные наборы правил, вердикт деградирован")
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
		out = append(out, fixturesReportVerdict{Name: "детекция Laravel < 1%",
			Status: verdictNone, Detail: "нет сравнимых строк (прогон не записал DOM или анализатор не проштамповал results.rules_version — дождитесь доработки analyzer)"})
	case rulesDiffer:
		out = append(out, fixturesReportVerdict{Name: "детекция Laravel < 1%",
			Status: verdictNone, Detail: fmt.Sprintf("правила анализатора менялись между прогонами; расхождений %s (%d из %d)", pctf(r.DetectionRatePct), r.DetectionDiff, r.DetectionComparable)})
	case r.DetectionRatePct < 1:
		out = append(out, fixturesReportVerdict{Name: "детекция Laravel < 1%",
			Status: verdictPass, Detail: fmt.Sprintf("%s расхождений (%d из %d сравнимых)", pctf(r.DetectionRatePct), r.DetectionDiff, r.DetectionComparable)})
	default:
		out = append(out, fixturesReportVerdict{Name: "детекция Laravel < 1%",
			Status: verdictFail, Detail: fmt.Sprintf("%s расхождений (%d из %d сравнимых)", pctf(r.DetectionRatePct), r.DetectionDiff, r.DetectionComparable)})
	}

	// Success: candidate ≥ baseline − engine-independent losses (DNS/cert).
	switch {
	case r.IndepLossPctC != nil && r.SuccessPctC >= r.SuccessPctB-*r.IndepLossPctC:
		out = append(out, fixturesReportVerdict{Name: "успешность ≥ baseline − движко-независимые потери",
			Status: verdictPass, Detail: fmt.Sprintf("%s ≥ %s − %s", pctf(r.SuccessPctC), pctf(r.SuccessPctB), pctf(*r.IndepLossPctC))})
	case r.IndepLossPctC != nil:
		out = append(out, fixturesReportVerdict{Name: "успешность ≥ baseline − движко-независимые потери",
			Status: verdictFail, Detail: fmt.Sprintf("%s < %s − %s (DNS/cert)", pctf(r.SuccessPctC), pctf(r.SuccessPctB), pctf(*r.IndepLossPctC))})
	case r.SuccessPctC >= r.SuccessPctB:
		// The losses are unknown (no metrics) — a strict comparison: it
		// raises the bar for the candidate but lets no regression
		// through.
		out = append(out, fixturesReportVerdict{Name: "успешность ≥ baseline (строго: потери DNS/cert неизвестны)",
			Status: verdictPass, Detail: fmt.Sprintf("%s ≥ %s", pctf(r.SuccessPctC), pctf(r.SuccessPctB))})
	default:
		out = append(out, fixturesReportVerdict{Name: "успешность ≥ baseline (строго: потери DNS/cert неизвестны)",
			Status: verdictFail, Detail: fmt.Sprintf("%s < %s; снимите --metrics прогона для вычета движко-независимых потерь", pctf(r.SuccessPctC), pctf(r.SuccessPctB))})
	}

	// Pace not worse than baseline.
	switch {
	case r.PaceB == nil || r.PaceC == nil:
		out = append(out, fixturesReportVerdict{Name: "темп не хуже базового",
			Status: verdictNone, Detail: "темп не снят (Redis недоступен при снапшоте или q:analyze < 2 записей)"})
	case *r.PaceC >= *r.PaceB:
		out = append(out, fixturesReportVerdict{Name: "темп не хуже базового",
			Status: verdictPass, Detail: fmt.Sprintf("%.2f ≥ %.2f стр/с", *r.PaceC, *r.PaceB)})
	default:
		out = append(out, fixturesReportVerdict{Name: "темп не хуже базового",
			Status: verdictFail, Detail: fmt.Sprintf("%.2f < %.2f стр/с", *r.PaceC, *r.PaceB)})
	}

	// Memory < ⅓ of the chromium budget.
	switch {
	case r.RSSC == nil:
		out = append(out, fixturesReportVerdict{Name: "память < ⅓ бюджета chromium",
			Status: verdictNone, Detail: "RSS движка не задан (--engine-rss-max при снапшоте кандидата)"})
	case *r.RSSC*3 < r.Budget:
		out = append(out, fixturesReportVerdict{Name: "память < ⅓ бюджета chromium",
			Status: verdictPass, Detail: fmt.Sprintf("%s < ⅓ от %s", fixturesFormatBytes(*r.RSSC), fixturesFormatBytes(r.Budget))})
	default:
		out = append(out, fixturesReportVerdict{Name: "память < ⅓ бюджета chromium",
			Status: verdictFail, Detail: fmt.Sprintf("%s ≥ ⅓ от %s", fixturesFormatBytes(*r.RSSC), fixturesFormatBytes(r.Budget))})
	}

	// 0 engine core crashes.
	switch {
	case r.CrashesC != nil:
		status, word := verdictPass, "0 падений ядра движка"
		if *r.CrashesC > 0 {
			status, word = verdictFail, fmt.Sprintf("%d падений ядра движка", *r.CrashesC)
		}
		out = append(out, fixturesReportVerdict{Name: "0 crashes движка",
			Status: status, Detail: word + " (операторский вход --engine-crashes)"})
	case r.BrowserErrC > 0:
		out = append(out, fixturesReportVerdict{Name: "0 crashes движка",
			Status: verdictFail, Detail: fmt.Sprintf("%.0f событий browser-класса (движок недоступен) — прокси падений/недоступности ядра", r.BrowserErrC)})
	case r.MetricsValidC:
		out = append(out, fixturesReportVerdict{Name: "0 crashes движка",
			Status: verdictPass, Detail: "0 событий browser-класса по метрикам этапа (прокси; точный вход --engine-crashes)"})
	default:
		out = append(out, fixturesReportVerdict{Name: "0 crashes движка",
			Status: verdictNone, Detail: "нет ни операторского входа (--engine-crashes), ни валидных метрик этапа"})
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
	out(w, "Отчёт сравнения движков рендера (ТЗ рендера §5.1; T-244)\n")
	out(w, "Выборка: %d задач фикстуры (дамп seed %d)\n", r.Keys, r.Seed)
	out(w, "baseline:  %s (движок %s)\n", r.BaselineLabel, r.BaselineEngine)
	out(w, "candidate: %s (движок %s)\n", r.CandidateLabel, r.CandidateEngine)
	for _, warn := range r.Warnings {
		out(w, "предупреждение: %s\n", warn)
	}
	out(w, "\n")

	out(w, "Успешность рендеров\n")
	out(w, "  baseline:  %d/%d (%s)", r.SuccessRenderedB, r.Keys, pctf(r.SuccessPctB))
	if r.FinalizeB != nil && r.FinalizeB.RenderedRecords > 0 {
		out(w, " [по метрикам этапа: rendered-записей %.0f]", r.FinalizeB.RenderedRecords)
	}
	out(w, "\n")
	out(w, "  candidate: %d/%d (%s)", r.SuccessRenderedC, r.Keys, pctf(r.SuccessPctC))
	if r.FinalizeC != nil && r.FinalizeC.RenderedRecords > 0 {
		out(w, " [по метрикам этапа: rendered-записей %.0f]", r.FinalizeC.RenderedRecords)
	}
	if r.IndepLossPctC != nil {
		out(w, "; движко-независимые потери (DNS/cert): %s", pctf(*r.IndepLossPctC))
	}
	out(w, "\n\n")

	out(w, "Темп и время страницы\n")
	paceLine := func(name string, pace *float64, tasks int64, sec float64, avg, p95 *float64) {
		if pace != nil {
			out(w, "  %s: %.2f стр/с (%d записей q:analyze за %.0f с)", name, *pace, tasks, sec)
		} else {
			out(w, "  %s: темп нет данных (стримы q:analyze не сняты или < 2 записей)", name)
		}
		// Page time is a §5.1 item 3 metric of its own (the stage's
		// histogram): printed even without a pace — the metrics could have
		// been taken without Redis (review T-244 P2-2).
		if avg != nil {
			out(w, "; ср. %.2f с", *avg)
		}
		if p95 != nil {
			out(w, "; p95 %.2f с", *p95)
		}
		out(w, "\n")
	}
	paceLine("baseline", r.PaceB, r.PaceTasksB, r.PaceSecB, r.AvgB, r.P95B)
	paceLine("candidate", r.PaceC, r.PaceTasksC, r.PaceSecC, r.AvgC, r.P95C)
	out(w, "\n")

	out(w, "Причины финализаций без рендера (метрики этапа, словарь taskFinalizeReason)\n")
	finalizeLine := func(name string, m *fixturesSnapshotMetrics) {
		if m == nil {
			out(w, "  %s: нет данных (снапшот без --metrics)\n", name)
			return
		}
		out(w, "  %s: тарпит %.0f, cert %.0f, dns %.0f, класс задачи %.0f",
			name, m.TarpitFinalized, m.CertFinalized, m.DNSFinalized, fixturesFinalizedSum(m.TaskFinalized))
		if len(m.TaskFinalized) > 0 {
			out(w, " (%s)", fixturesReasonList(m.TaskFinalized))
		}
		if fb := fixturesFinalizedSum(m.FallbackPublished); fb > 0 {
			out(w, "; публикаций в фолбэк %.0f (%s)", fb, fixturesReasonList(m.FallbackPublished))
		}
		out(w, "\n")
	}
	finalizeLine("baseline", r.FinalizeB)
	finalizeLine("candidate", r.FinalizeC)
	if r.Cumulative {
		out(w, "  (воркер не перезапускался: счётчики candidate включают baseline-прогон)\n")
	}
	out(w, "\n")

	out(w, "Диф детекции Laravel (протокол T-213)\n")
	out(w, "  сравнимые вердикты: %d из %d; Laravel-вердиктов baseline %d, candidate %d\n",
		r.DetectionComparable, r.Keys, r.DetectionFoundB, r.DetectionFoundC)
	out(w, "  расхождения вердиктов: %d (%s)\n\n", r.DetectionDiff, pctf(r.DetectionRatePct))

	out(w, "Структурные метрики DOM (нормализованное сравнение)\n")
	out(w, "  пары с DOM обоих прогонов: %d; идентичный DOM (content_hash): %d (%s)\n",
		r.DOMPairs, r.DOMIdentical, pctf(r.DOMIdenticalPct))
	out(w, "  медианы отношений candidate/baseline: длина %s, узлы %s, скрипты %s\n",
		ratioOrNone(r.DOMMedLenRatio), ratioOrNone(r.DOMMedNodesRatio), ratioOrNone(r.DOMMedScriptsRatio))
	out(w, "  аномальные пары по длине (вне ×0.5…×2): %d (%s)", r.DOMAnomalousPairs, pctf(r.DOMAnomalousPct))
	if r.DOMScriptZeroPairs > 0 {
		out(w, "; пар, где один из прогонов не выполнил ни одного <script>: %d", r.DOMScriptZeroPairs)
	}
	out(w, "\n\n")

	out(w, "Память RSS движка\n")
	out(w, "  baseline: %s; candidate: %s; бюджет chromium %s (⅓ = %s)\n\n",
		rssOrNone(r.RSSB), rssOrNone(r.RSSC), fixturesFormatBytes(r.Budget), fixturesFormatBytes(r.Budget/3))

	out(w, "Вердикты против порогов §5.1 п.4\n")
	for _, v := range r.Verdicts {
		out(w, "  [%s] %s: %s\n", v.Status, v.Name, v.Detail)
	}
}

// rssOrNone — RSS as a string or "no data".
func rssOrNone(v *int64) string {
	if v == nil {
		return "нет данных"
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
	baseline := fs.String("baseline", "", "снапшот baseline-chromium (обязателен)")
	candidate := fs.String("candidate", "", "снапшот движка-кандидата (обязателен)")
	outPath := fs.String("out", "", "файл отчёта (кроме stdout; для протокола T-247)")
	budget := fs.Int64("memory-budget", fixturesDefaultMemoryBudget, "бюджет памяти chromium для порога ⅓, байты")
	failExit := fs.Bool("fail-exit", false, "код 1 при любом вердикте НЕ ПРОЙДЕН (для скриптов)")
	if handled, err := parseFlags(fs, fixturesHelp, args, stdout); handled {
		return err
	}
	if strings.TrimSpace(*baseline) == "" || strings.TrimSpace(*candidate) == "" {
		return usagef("укажите оба снапшота: crawlerctl fixtures report --baseline A.snapshot.json --candidate B.snapshot.json")
	}
	if *budget <= 0 {
		return usagef("fixtures report: --memory-budget: положительное значение, получено %d", *budget)
	}
	if fs.NArg() > 0 {
		return usagef("fixtures report: лишние аргументы %v (команда принимает только флаги)", fs.Args())
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
			return fmt.Errorf("fixtures report: запись %s: %w", *outPath, err)
		}
		if err := os.Rename(tmp, *outPath); err != nil {
			_ = os.Remove(tmp)
			return fmt.Errorf("fixtures report: запись %s: %w", *outPath, err)
		}
		_, _ = fmt.Fprintf(stderr, "отчёт записан: %s\n", *outPath)
	}
	if *failExit && report.Fail {
		return fmt.Errorf("fixtures report: вердикт НЕ ПРОЙДЕН по %s", strings.Join(failedVerdictNames(report), ", "))
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

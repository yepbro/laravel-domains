// Tests of the mutants triaged in the CI run of 11/09/2026 (half A):
// analyzer additions — aggregation counter errors (zone/history
// cumulative, including RowsAffected 0), the findings=0 path without a
// transition (no upsert call), the unfound drain bounds (retention=1,
// the event/rows/failure metrics), the UnfoundDrain metric bounds
// (rows=1), tick select and re-analysis bucket errors (zero values on
// errors), the lookback anchor and the loop pass without loaded rules (no panic on a nil set).
package analyzer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"crawler/internal/content"
	"crawler/internal/metrics"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// seedUnfoundFixture is the find removal by re-analysis fixture: the
// laravel_domains row exists, the delivery analyzes a page without signs.
func seedUnfoundFixture(t *testing.T, opts Options) (*anFixture, *anDrainRecorder) {
	t.Helper()
	f := newANFixture(t, anSet(1001), opts)
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), nil)
	f.pool.domains[anDomainKey{anKey.zone, anKey.domain}] = &anDomain{
		score: 0.9, foundOnMask: mustMask(t, 1), rulesVersion: 999, updatedAt: time.Now(),
	}
	rec := &anDrainRecorder{j: f.pool.j, stats: content.RetentionStats{Rows: 3, Refs: 2}}
	f.w.opt.DrainDomain = rec.drain
	return f, rec
}

// TestUnfoundDrain_MetricsAndRetentionBoundaries — the unfound event
// hits the metric, the drain with retention=1 runs (the "≤ 0" bound),
// the drained rows are summed into the metric.
func TestUnfoundDrain_MetricsAndRetentionBoundaries(t *testing.T) {
	reg := metrics.New("analyzer")
	f, rec := seedUnfoundFixture(t, Options{MinScore: 0.6, Retention: 1})
	f.w.met = NewMetrics(reg)
	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if rec.count() != 1 {
		t.Fatalf("the drain with retention=1: calls = %d, want 1 (the minimal working window)", rec.count())
	}
	if got := anMetricValue(t, reg, "crawler_analyzer_laravel_unfound_total", nil); got != 1 {
		t.Errorf("unfound_total = %v, want 1", got)
	}
	if got := anMetricValue(t, reg, "crawler_analyzer_unfound_drained_rows_total", nil); got != 3 {
		t.Errorf("drained_rows = %v, want 3", got)
	}
	if got := anMetricValue(t, reg, "crawler_analyzer_unfound_drain_errors_total", nil); got != 0 {
		t.Errorf("drain_errors = %v, want 0", got)
	}
}

// TestUnfoundDrain_ErrorMetrics — a drain failure hits the error
// metric, the event and the row removal are committed.
func TestUnfoundDrain_ErrorMetrics(t *testing.T) {
	reg := metrics.New("analyzer")
	f, _ := seedUnfoundFixture(t, Options{MinScore: 0.6, Retention: time.Hour})
	f.w.met = NewMetrics(reg)
	rec := &anDrainRecorder{j: f.pool.j, err: errors.New("drain boom")}
	f.w.opt.DrainDomain = rec.drain
	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("ProcessResult: %v (a drain failure does not fail the delivery)", err)
	}
	if got := anMetricValue(t, reg, "crawler_analyzer_unfound_drain_errors_total", nil); got != 1 {
		t.Errorf("drain_errors = %v, want 1", got)
	}
	if got := anMetricValue(t, reg, "crawler_analyzer_laravel_unfound_total", nil); got != 1 {
		t.Errorf("unfound_total = %v, want 1", got)
	}
	if _, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain); ok {
		t.Error("the laravel_domains row was not removed")
	}
}

// TestUnfoundDrain_NotUnfoundNoMetric — without an unfound event the
// metrics are not touched (cutting w.met.Unfound() out is observable).
func TestUnfoundDrain_NotUnfoundNoMetric(t *testing.T) {
	reg := metrics.New("analyzer")
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6, Retention: time.Hour})
	f.w.met = NewMetrics(reg)
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), nil)
	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"crawler_analyzer_laravel_unfound_total",
		"crawler_analyzer_unfound_drained_rows_total",
		"crawler_analyzer_unfound_drain_errors_total",
	} {
		if got := anMetricValue(t, reg, name, nil); got > 0 {
			t.Errorf("%s = %v, want 0/no series (no event)", name, got)
		}
	}
}

// TestMetricsUnfoundDrainRowOne — a drained rows value of 1 is a valid
// increment (the rows ≤ 0 bound); zero on a clean registry does NOT create a series.
func TestMetricsUnfoundDrainRowOne(t *testing.T) {
	reg := metrics.New("analyzer")
	m := NewMetrics(reg)
	m.UnfoundDrain(1)
	if got := anMetricValue(t, reg, "crawler_analyzer_unfound_drained_rows_total", nil); got != 1 {
		t.Errorf("rows=1: %v, want 1", got)
	}
	m.UnfoundDrain(0)
	m.UnfoundDrain(-3)
	if got := anMetricValue(t, reg, "crawler_analyzer_unfound_drained_rows_total", nil); got != 1 {
		t.Errorf("zero/negative changed the value: %v, want 1", got)
	}
}

// TestAggregateZoneCounterErrors — zone delta and history cumulative
// failures (including RowsAffected 0) fail the delivery with context
// and the original cause.
func TestAggregateZoneCounterErrors(t *testing.T) {
	hash := anTestHash(7)
	// A zone delta failure on row appearance (the original cause in the
	// error — the return-cut mutant would give only the "affects 0 rows" tail).
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])
	f.pool.zoneLaravelErr = errors.New("zone: boom")
	_, err := f.w.ProcessResult(context.Background(), anTask)
	if err == nil || !strings.Contains(err.Error(), "Laravel zone counter") ||
		!strings.Contains(err.Error(), "zone: boom") {
		t.Errorf("the zone delta failure = %v, want the zone counter context and the original", err)
	}

	// The zone row is missing (UPDATE 0) — the mandatory-row error.
	f2 := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	f2.store.blocks[hash] = anPageHTML()
	f2.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])
	f2.pool.zoneNoRow = true
	if _, err := f2.w.ProcessResult(context.Background(), anTask); err == nil ||
		!strings.Contains(err.Error(), "0 rows affected") {
		t.Errorf("the zone UPDATE 0 = %v, want «0 rows affected»", err)
	}

	// A history cumulative failure (appeared: a fresh confident find).
	f3 := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	f3.store.blocks[hash] = anPageHTML()
	f3.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])
	f3.pool.counterDeltaErr = errors.New("cumulative: boom")
	task := anTask
	task.Fresh = true
	if _, err := f3.w.ProcessResult(context.Background(), task); err == nil ||
		!strings.Contains(err.Error(), "history counter") ||
		!strings.Contains(err.Error(), "cumulative: boom") {
		t.Errorf("the cumulative failure = %v, want the history counter context and the original", err)
	}

	// A cumulative without a row (INSERT 0 0) — the mandatory-row error.
	f4 := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	f4.store.blocks[hash] = anPageHTML()
	f4.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])
	f4.pool.counterNoRow = true
	if _, err := f4.w.ProcessResult(context.Background(), task); err == nil ||
		!strings.Contains(err.Error(), "history counter") {
		t.Errorf("the cumulative UPDATE 0 = %v, want the mandatory-row error", err)
	}
}

// TestAggregateLostPathCounterErrors — zone delta and cumulative
// failures on the find LOSS path (a transition without fresh): errors fail the delivery.
func TestAggregateLostPathCounterErrors(t *testing.T) {
	// A zone delta failure (−1) on the find removal.
	f, _ := seedUnfoundFixture(t, Options{MinScore: 0.6, Retention: time.Hour})
	f.pool.zoneLaravelErr = errors.New("zone: boom")
	if _, err := f.w.ProcessResult(context.Background(), anTask); err == nil ||
		!strings.Contains(err.Error(), "zone: boom") {
		t.Errorf("the loss: the zone delta failure = %v, want the original cause", err)
	}
	// A lost cumulative failure (a fresh find loss writes lost history).
	f2, _ := seedUnfoundFixture(t, Options{MinScore: 0.6, Retention: time.Hour})
	f2.pool.counterDeltaErr = errors.New("cumulative: boom")
	task := anTask
	task.Fresh = true
	if _, err := f2.w.ProcessResult(context.Background(), task); err == nil ||
		!strings.Contains(err.Error(), "cumulative: boom") {
		t.Errorf("the loss: the cumulative failure = %v, want the original cause", err)
	}
}

// TestAggregateRepeatDeliveryNoShift — a repeated confident delivery
// with an existing row (a rule version change — the row was moved to
// stale and delivered again): no transition — the zone counter and
// the history are untouched (the return-cut mutant would call the delta).
func TestAggregateRepeatDeliveryNoShift(t *testing.T) {
	hash := anTestHash(7)
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])
	// The first delivery — the row appeared.
	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("the first delivery: %v", err)
	}
	if got := f.pool.zoneLaravel[anKey.zone]; got != 1 {
		t.Fatalf("after the first delivery the zone counter = %d, want 1", got)
	}
	// A rule version change: the row went stale, the repeated confident
	// delivery reaches the aggregation with existed=true — no deltas, no history.
	f.sets.swap(anSet(1002))
	historyBefore := len(f.pool.history)
	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("the repeated delivery: %v", err)
	}
	if got := f.pool.zoneLaravel[anKey.zone]; got != 1 {
		t.Errorf("the repeated delivery moved the zone counter: %d, want 1", got)
	}
	if len(f.pool.history) != historyBefore {
		t.Errorf("the repeated delivery wrote history: %d → %d", historyBefore, len(f.pool.history))
	}
}

// TestAnalyzeConsumeTailRowsAffected — a tail bucket decrement with a
// non-standard RowsAffected — the mandatory-row error.
func TestAnalyzeConsumeTailRowsAffected(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), nil)
	f.pool.consumeRowsAff = 2
	if _, err := f.w.ProcessResult(context.Background(), anTask); err == nil ||
		!strings.Contains(err.Error(), "re-analysis tail bucket: 2 rows affected") {
		t.Errorf("the bucket's RowsAffected = %v, want «2 rows affected»", err)
	}
}

// TestUnfoundDrain_RetentionZeroNoCall — retention=0 with the seam set:
// the drain is NOT called at all (content is kept forever), the
// event is recorded by the metric.
func TestUnfoundDrain_RetentionZeroNoCall(t *testing.T) {
	reg := metrics.New("analyzer")
	f, rec := seedUnfoundFixture(t, Options{MinScore: 0.6, Retention: 0})
	f.w.met = NewMetrics(reg)
	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if rec.count() != 0 {
		t.Errorf("drain calls with retention=0 = %d, want 0 (kept forever)", rec.count())
	}
	if _, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain); ok {
		t.Error("the laravel_domains row was not removed")
	}
	if got := anMetricValue(t, reg, "crawler_analyzer_laravel_unfound_total", nil); got != 1 {
		t.Errorf("unfound_total = %v, want 1 (the event is recorded)", got)
	}
}

// TestAggregateNoFindingsNoUpsert — findings=0 without a transition (no
// row existed): the aggregate upsert does not run (the return-cut
// mutant would write with an empty aggregate).
func TestAggregateNoFindingsNoUpsert(t *testing.T) {
	f, rec := seedUnfoundFixture(t, Options{MinScore: 0.6, Retention: time.Hour})
	// No transition: remove the laravel_domains row — DELETE 0.
	delete(f.pool.domains, anDomainKey{anKey.zone, anKey.domain})
	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if rec.count() != 0 {
		t.Errorf("the drain without a transition = %d, want 0", rec.count())
	}
	for _, ev := range f.pool.j.snapshot() {
		if ev == anEvAggUpsert {
			t.Error("the laravel_domains upsert with zero findings without a transition")
		}
	}
}

// TestReanalyzeMatureTailsErrors — guard query errors: Query/Scan/Err
// and an empty response — errors with zero values (a pin of zeros on
// each branch); a connection drop before the first row is an
// iteration error, not an "empty response".
func TestReanalyzeMatureTailsErrors(t *testing.T) {
	// A Query error.
	pool := newReanPool()
	pool.setQueryErr(errors.New("db unavailable"))
	if tails, hour, err := reanalyzeMatureTails(context.Background(), pool); err == nil ||
		tails != 0 || !hour.IsZero() {
		t.Errorf("the Query failure: (%v/%v, %v), want zeros and an error", tails, hour, err)
	}
	// An empty response — a protocol break, the values are zero.
	pool2 := newReanPool()
	pool2.guardEmpty = true
	if tails, hour, err := reanalyzeMatureTails(context.Background(), pool2); err == nil ||
		tails != 0 || !hour.IsZero() ||
		!strings.Contains(err.Error(), "empty response") {
		t.Errorf("the empty response: (%v/%v, %v), want zeros and «empty response»", tails, hour, err)
	}
	// A drop before the first row (rows.Err on an empty set) is an
	// iteration error, NOT an "empty aggregate response".
	pool3 := newReanPool()
	pool3.guardEmpty = true
	pool3.guardRowsErr = errors.New("connection drop")
	if tails, hour, err := reanalyzeMatureTails(context.Background(), pool3); err == nil ||
		tails != 0 || !hour.IsZero() || !strings.Contains(err.Error(), "connection drop") {
		t.Errorf("the drop before the row: (%v/%v, %v), want zeros and the original", tails, hour, err)
	}
	// A Scan error — the "reading" context, the values are zero.
	pool4 := newReanPool()
	pool4.guardScanErr = errors.New("scan boom")
	if tails, hour, err := reanalyzeMatureTails(context.Background(), pool4); err == nil ||
		tails != 0 || !hour.IsZero() ||
		!strings.Contains(err.Error(), "read") {
		t.Errorf("the Scan failure: (%v/%v, %v), want zeros and the read context", tails, hour, err)
	}
	// rows.Err after a row — the original error, the values are zero.
	pool5 := newReanPool()
	pool5.guardRowsErr = errors.New("drop")
	if tails, hour, err := reanalyzeMatureTails(context.Background(), pool5); err == nil ||
		tails != 0 || !hour.IsZero() || !strings.Contains(err.Error(), "drop") {
		t.Errorf("rows.Err after a row: (%v/%v, %v), want zeros and the original", tails, hour, err)
	}
}

// TestReanalyzeTickSelectErrors — a tick select error (the guard passed):
// the "tick select" context; row Scan/Err — with their contexts.
func TestReanalyzeTickSelectErrors(t *testing.T) {
	hour := time.Now().Truncate(time.Hour)
	seed := func() *reanPool {
		pool := newReanPool()
		pool.guardTails = 1000 // the guard is open — the select runs
		pool.guardHour = hour
		pool.seedZone(1, "test")
		pool.seedDomain(1, 1, "a", 1) // status new — not deleted
		pool.seedRowAt(1, 1, 0, 0, nil, hour.Add(-2*time.Hour))
		return pool
	}
	// A Query error of the tick select.
	pool := seed()
	pool.tickQueryErr = errors.New("db unavailable")
	if _, err := ReanalyzeTick(context.Background(), pool, &reanQueue{}, 1, &ReanalyzeCursor{}, ReanalyzeOptions{}); err == nil ||
		!strings.Contains(err.Error(), "tick select") {
		t.Errorf("the tick Query = %v, want the «tick select» context", err)
	}
	// A row Scan error.
	pool2 := seed()
	pool2.tickScanErr = errors.New("scan boom")
	if _, err := ReanalyzeTick(context.Background(), pool2, &reanQueue{}, 1, &ReanalyzeCursor{}, ReanalyzeOptions{}); err == nil ||
		!strings.Contains(err.Error(), "read") {
		t.Errorf("the tick Scan = %v, want the read context", err)
	}
	// rows.Err.
	pool3 := seed()
	pool3.tickRowsErr = errors.New("drop")
	if _, err := ReanalyzeTick(context.Background(), pool3, &reanQueue{}, 1, &ReanalyzeCursor{}, ReanalyzeOptions{}); err == nil ||
		!strings.Contains(err.Error(), "drop") {
		t.Errorf("the tick rows.Err = %v, want the original", err)
	}
}

// cancelQueue is a queue that cancels ctx on the first publish (a
// cancellation in the middle of a tick batch).
type cancelQueue struct {
	reanQueue
	cancel context.CancelFunc
	done   bool
}

func (q *cancelQueue) Publish(ctx context.Context, stream string, msg queue.Message) (string, error) {
	id, err := q.reanQueue.Publish(ctx, stream, msg) // a publish with the live ctx
	if !q.done {
		q.done = true
		q.cancel() // the cancellation AFTER a successful publish — the loop follows
	}
	return id, err
}

// TestReanalyzeTickCancelMidBatch — a cancellation between batches: the
// exact wrap "analyzer: re-analysis: context canceled" (the re-check
// cut mutant would give "tick select: …"), the outcome is a batch before the cancel.
func TestReanalyzeTickCancelMidBatch(t *testing.T) {
	pool := newReanPool()
	pool.guardTails = 1000
	pool.guardHour = time.Now().Truncate(time.Hour)
	pool.seedZone(1, "test")
	base := time.Now().Add(-2 * time.Hour)
	for i := int64(1); i <= 3; i++ {
		pool.seedDomain(1, i, fmt.Sprintf("d%d", i), model.StatusAlive)
		pool.seedRowAt(1, i, 0, model.VariantMain, nil, base.Add(time.Duration(i)*time.Minute))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q := &cancelQueue{cancel: cancel}
	res, err := ReanalyzeTick(ctx, pool, q, 1, &ReanalyzeCursor{}, ReanalyzeOptions{BatchSize: 1})
	if err == nil || err.Error() != "analyzer: re-analysis: context canceled" {
		t.Fatalf("the error = %v, want the exact wrap «analyzer: re-analysis: context canceled»", err)
	}
	if res.Selected == 0 || res.Published == 0 {
		t.Errorf("the outcome = %+v, want at least one batch before the cancel", res)
	}
}

// TestReanalyzeLookbackPin — the window anchor is a day.
func TestReanalyzeLookbackPin(t *testing.T) {
	if DefaultReanalyzeLookback != 24*time.Hour {
		t.Errorf("DefaultReanalyzeLookback = %v, want 24h", DefaultReanalyzeLookback)
	}
}

// TestReanalyzeLoopNilRulesNoPanic — the loop without a loaded rule set
// skips the startup pass without a panic (the Warn+return cut mutant
// would dereference a nil set) and exits on cancellation; after the
// set is loaded the passes run.
func TestReanalyzeLoopNilRulesNoPanic(t *testing.T) {
	pool := newReanPool()
	pool.guardTails = 0 // the guard is closed — no heavy select
	q := &reanQueue{}
	sets := &reanSource{set: nil} // the set is not loaded
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		done <- ReanalyzeLoop(ctx, pool, q, sets, ReanalyzeOptions{Interval: time.Nanosecond})
	}()
	// The startup drain AND the ticker ticks must run with a nil set
	// without a panic (the Warn+return cut mutant in tick would
	// dereference a nil set); wait a few ticks and cancel.
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ReanalyzeLoop: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not exit on cancellation")
	}
	if n := q.count(); n != 0 {
		t.Errorf("publications without a rule set = %d, want 0", n)
	}
}

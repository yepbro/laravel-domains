// Unit tests of the render engines panel (T-246, renders.go):
// exposition text format parsing (labels, quotes/commas in values,
// shape degradations), aggregation of the two worker build exposures
// per engines and outcomes, rates between passes (the
// CollectorOptions.Now clock, a worker restart — a zero delta), the
// q:render-fallback depth, section degradations (addresses not set —
// "n/a" without an error; an exposure unavailable — an error).
package dashboard

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"crawler/internal/queue"
)

// --- exposition parsing -----------------------------------------------------------

// TestParsePromExposition — a minimal parser: samples with and without
// labels, HELP/TYPE skipped, value escapes, tabular shape errors.
func TestParsePromExposition(t *testing.T) {
	expo := `# HELP crawler_render_records_total Записи по признаку rendered
# TYPE crawler_render_records_total counter
crawler_render_records_total{rendered="true",engine="moli"} 1000
crawler_render_records_total{rendered="false"} 42
crawler_render_tarpit_finalized_total 7
weird_metric{a="x,y",b="q\"r"} 1.5
`
	samples, err := parsePromExposition(strings.NewReader(expo))
	if err != nil {
		t.Fatalf("разбор: %v", err)
	}
	if len(samples) != 4 {
		t.Fatalf("сэмплов = %d, want 4 (HELP/TYPE пропущены):\n%+v", len(samples), samples)
	}
	byName := map[string]promSample{}
	for _, s := range samples {
		byName[s.Name+"|"+s.Labels["rendered"]] = s
	}
	rec := byName["crawler_render_records_total|true"]
	if rec.Value != 1000 || rec.Labels["engine"] != "moli" {
		t.Errorf("запись с метками = %+v, want engine=moli 1000", rec)
	}
	if s := byName["crawler_render_records_total|false"]; s.Value != 42 || s.Labels["engine"] != "" {
		t.Errorf("запись rendered=false = %+v, want 42 без engine", s)
	}
	if s := byName["crawler_render_tarpit_finalized_total|"]; s.Value != 7 {
		t.Errorf("сэмпл без меток = %+v, want 7", s)
	}
	// A comma inside quotes is part of the value, an escaped quote is
	// unescaped.
	w := byName["weird_metric|"]
	if w.Labels["a"] != "x,y" || w.Labels["b"] != `q"r` || w.Value != 1.5 {
		t.Errorf("метки с запятой/кавычкой = %+v", w.Labels)
	}

	for _, tc := range []struct{ name, expo, wantErr string }{
		{"метки без закрывающей", `m{x="1" 5`, "закрывающей"},
		{"нет значения", `m{x="1"}`, "нет значения"},
		{"значение не число", `m{x="1"} abc`, "значение"},
		{"метка без кавычек", `m{x=1} 5`, "кавычках"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parsePromExposition(strings.NewReader(tc.expo)); err == nil ||
				!strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ошибка = %v, want содержит %q", err, tc.wantErr)
			}
		})
	}
}

// --- section assembly ---------------------------------------------------------------

// renderLens — a StreamLener over a depth map (the panel's
// q:render-fallback depth does not have to follow the seq fakeLenner
// of the queues section).
type renderLens struct {
	lens map[string]int64
	err  error
}

func (l *renderLens) Len(_ context.Context, stream string) (int64, error) {
	if l.err != nil {
		return 0, l.err
	}
	return l.lens[stream], nil
}

// renderWorkerExpo — a small controllable /metrics exposure of a
// render worker: values change between passes (the rate), a restart —
// a reset.
type renderWorkerExpo struct {
	engine            string
	rendered          float64
	finalized         float64
	tarpits           float64
	published         float64
	consumedRendered  float64
	consumedFinalized float64
	consumedSkipped   float64
	tasks             float64
	rss               float64
}

func (w renderWorkerExpo) exposition() string {
	var b strings.Builder
	b.WriteString("# TYPE crawler_render_records_total counter\n")
	b.WriteString("crawler_render_records_total{rendered=\"true\",engine=\"" + w.engine + "\"} " +
		formatFloat(w.rendered) + "\n")
	b.WriteString("crawler_render_records_total{rendered=\"false\"} 999\n")
	b.WriteString("crawler_stage_tasks_total{stage=\"render\"} " +
		formatFloat(w.tasks) + "\n")
	b.WriteString("crawler_render_task_finalized_total{reason=\"timeout\",engine=\"" + w.engine + "\"} " +
		formatFloat(w.finalized) + "\n")
	// Tarpits and RSS — as in a live exposure: the tarpit family is
	// lazy (no series without events), process_resident_memory_bytes —
	// always.
	if w.tarpits > 0 {
		b.WriteString("crawler_render_tarpit_finalized_total{engine=\"" + w.engine + "\"} " +
			formatFloat(w.tarpits) + "\n")
	}
	if w.published > 0 {
		b.WriteString("crawler_render_fallback_published_total{reason=\"timeout\"} " +
			formatFloat(w.published) + "\n")
	}
	if w.consumedRendered+w.consumedFinalized+w.consumedSkipped > 0 {
		b.WriteString("crawler_render_fallback_consumed_total{result=\"rendered\"} " +
			formatFloat(w.consumedRendered) + "\n")
		b.WriteString("crawler_render_fallback_consumed_total{result=\"finalized\"} " +
			formatFloat(w.consumedFinalized) + "\n")
		b.WriteString("crawler_render_fallback_consumed_total{result=\"skipped\"} " +
			formatFloat(w.consumedSkipped) + "\n")
	}
	if w.rss > 0 {
		b.WriteString("process_resident_memory_bytes " + formatFloat(w.rss) + "\n")
	}
	return b.String()
}

// formatFloat — a compact number notation for the exposure (integers
// without a tail, fractions as is).
func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// newRenderCollector — a collector with a controllable clock (rates
// are computed between passes) and a depth map; set — the clock
// advance.
func newRenderCollector(t *testing.T, lens *renderLens, urls []string) (*Collector, func(time.Time)) {
	t.Helper()
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	set := func(t time.Time) { now = t }
	coll := newSnapCollector(t, healthyPool(), lens, func(o *CollectorOptions) {
		if urls == nil {
			urls = []string{} // "addresses not set": nil would pull the cfg default
		}
		o.RenderMetrics = urls
		o.Now = clock
	})
	return coll, set
}

// TestCollectRenderPanel — aggregation of two worker builds: engines
// by the engine label (sorted by name), success over attributed
// finalizations, the fallback published/consumed/depth, rates — the
// delta between passes (the first pass — the baseline, rates 0).
func TestCollectRenderPanel(t *testing.T) {
	// Exposures are read by the server goroutines, the test mutates
	// the values between passes — a lock (a race under -race).
	var mu sync.Mutex
	workerA := renderWorkerExpo{engine: "moli", rendered: 1000, finalized: 100,
		tarpits: 25, published: 10, tasks: 2000, rss: 512 << 20}
	setA := func(w renderWorkerExpo) {
		mu.Lock()
		defer mu.Unlock()
		workerA = w
	}
	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		expo := workerA.exposition()
		mu.Unlock()
		_, _ = w.Write([]byte(expo))
	}))
	defer srvA.Close()
	workerB := renderWorkerExpo{engine: "chromium", rendered: 50,
		consumedRendered: 5, consumedFinalized: 1, consumedSkipped: 2,
		tasks: 300, rss: 256 << 20}
	setB := func(w renderWorkerExpo) {
		mu.Lock()
		defer mu.Unlock()
		workerB = w
	}
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		expo := workerB.exposition()
		mu.Unlock()
		_, _ = w.Write([]byte(expo))
	}))
	defer srvB.Close()

	lens := &renderLens{lens: map[string]int64{queue.StreamRenderFallback: 3}}
	coll, setClock := newRenderCollector(t, lens, []string{srvA.URL, srvB.URL})
	ctx := context.Background()

	// The first pass: the rate baseline (0), engines sorted, moli
	// success = 1000/1100.
	snap := coll.CollectOnce(ctx)
	if snap.Render == nil {
		t.Fatal("render = nil, want секция собрана")
	}
	if len(snap.Errors) != 0 {
		t.Fatalf("errors = %v, want пусто", snap.Errors)
	}
	if len(snap.Render.Engines) != 2 ||
		snap.Render.Engines[0].Engine != "chromium" ||
		snap.Render.Engines[1].Engine != "moli" {
		t.Fatalf("engines = %+v, want chromium,moli по алфавиту", snap.Render.Engines)
	}
	moli := snap.Render.Engines[1]
	if moli.RenderedTotal != 1000 || moli.FinalizedTotal != 100 {
		t.Errorf("moli кумулятивы = %+v, want rendered 1000 finalized 100", moli)
	}
	if moli.SuccessPct == nil || math.Abs(*moli.SuccessPct-1000.0/1100*100) > 1e-9 {
		t.Errorf("moli success = %v, want 1000/1100", moli.SuccessPct)
	}
	// S-P3-2: the tarpit rate — the share of tarpits among the engine
	// finalizations (25/(1000+100+25)), success does not change the
	// denominator.
	if moli.TarpitTotal != 25 || moli.TarpitPct == nil ||
		math.Abs(*moli.TarpitPct-25.0/1125*100) > 1e-9 {
		t.Errorf("moli тарпиты = %+v (%v), want 25 и 25/1125", moli.TarpitTotal, moli.TarpitPct)
	}
	if moli.RSSBytes != 512<<20 {
		t.Errorf("moli RSS = %d, want %d (process_resident_memory_bytes)", moli.RSSBytes, 512<<20)
	}
	chromium := snap.Render.Engines[0]
	if chromium.RenderedTotal != 50 || chromium.FinalizedTotal != 0 || chromium.SuccessPct == nil || *chromium.SuccessPct != 100 {
		t.Errorf("chromium = %+v, want 50/0 и 100%%", chromium)
	}
	// The lazy tarpit family (no series) — 0 and a rate of 0 given
	// existing finalizations, not "—" and not a missing engine.
	if chromium.TarpitTotal != 0 || chromium.TarpitPct == nil || *chromium.TarpitPct != 0 {
		t.Errorf("chromium тарпиты = %d (%v), want 0 и 0%%", chromium.TarpitTotal, chromium.TarpitPct)
	}
	if chromium.RSSBytes != 256<<20 {
		t.Errorf("chromium RSS = %d, want %d", chromium.RSSBytes, 256<<20)
	}
	for _, e := range snap.Render.Engines {
		if e.RenderedPerSec != 0 {
			t.Errorf("%s: темп первого прохода = %v, want 0 (база)", e.Engine, e.RenderedPerSec)
		}
	}
	fb := snap.Render.Fallback
	if fb.PublishedTotal != 10 || fb.PublishedPerSec != 0 {
		t.Errorf("published = %+v, want 10 и темп 0 (база)", fb)
	}
	// S-P3-2: the fallback rate — the share of published of stage
	// pages (10/(2000+300), the denominator — both exposures).
	if fb.RatePct == nil || math.Abs(*fb.RatePct-10.0/2300*100) > 1e-9 {
		t.Errorf("rate_pct = %v, want 10/2300", fb.RatePct)
	}
	if fb.ConsumedTotal != 8 || fb.ConsumedPerSec != 0 {
		t.Errorf("consumed = %+v, want 8 и темп 0 (база)", fb)
	}
	if fb.ConsumedByResult["rendered"] != 5 ||
		fb.ConsumedByResult["finalized"] != 1 || fb.ConsumedByResult["skipped"] != 2 {
		t.Errorf("consumed по исходам = %v, want 5/1/2", fb.ConsumedByResult)
	}
	if fb.Depth == nil || *fb.Depth != 3 {
		t.Errorf("depth = %v, want 3", fb.Depth)
	}

	// The second pass after 10 s: moli +100 renders and +2
	// publications — rates of 10/s and 0.2/s; worker B processed 2
	// records — a consumption rate of 0.2/s (the rate pair — the
	// "worker B cannot keep up" diagnostics, T-246 review); the
	// engines' rendered without movement — 0; the fallback rate — by
	// cumulatives (12/2400).
	setA(renderWorkerExpo{engine: "moli", rendered: 1100, finalized: 100,
		tarpits: 25, published: 12, tasks: 2100, rss: 512 << 20})
	setB(renderWorkerExpo{engine: "chromium", rendered: 50,
		consumedRendered: 7, consumedFinalized: 1, consumedSkipped: 2,
		tasks: 300, rss: 256 << 20})
	setClock(time.Date(2026, 11, 6, 12, 0, 10, 0, time.UTC))
	snap = coll.CollectOnce(ctx)
	if snap.Render == nil {
		t.Fatal("второй проход: render = nil")
	}
	moli = snap.Render.Engines[1]
	if moli.RenderedPerSec != 10 {
		t.Errorf("moli темп = %v, want 10 (100 рендеров за 10 с)", moli.RenderedPerSec)
	}
	if snap.Render.Engines[0].RenderedPerSec != 0 {
		t.Errorf("chromium темп = %v, want 0", snap.Render.Engines[0].RenderedPerSec)
	}
	if snap.Render.Fallback.PublishedPerSec != 0.2 {
		t.Errorf("published темп = %v, want 0.2 (2 за 10 с)", snap.Render.Fallback.PublishedPerSec)
	}
	if snap.Render.Fallback.ConsumedPerSec != 0.2 {
		t.Errorf("consumed темп = %v, want 0.2 (2 доработки за 10 с)", snap.Render.Fallback.ConsumedPerSec)
	}
	if snap.Render.Fallback.ConsumedTotal != 10 {
		t.Errorf("consumed = %d, want 10 (7+1+2)", snap.Render.Fallback.ConsumedTotal)
	}
	if pct := snap.Render.Fallback.RatePct; pct == nil || math.Abs(*pct-12.0/2400*100) > 1e-9 {
		t.Errorf("rate_pct второго прохода = %v, want 12/2400", pct)
	}

	// Worker A restart (the cumulative dropped) — a zero delta, not a
	// negative one; B without movement — the consumption rate is 0
	// too; B's stage pages are still there — the fallback rate is an
	// honest 0 (no publications), not "—".
	setA(renderWorkerExpo{engine: "moli", rendered: 5, tasks: 10})
	setClock(time.Date(2026, 11, 6, 12, 0, 20, 0, time.UTC))
	snap = coll.CollectOnce(ctx)
	if got := snap.Render.Engines[1].RenderedPerSec; got != 0 {
		t.Errorf("темп после рестарта = %v, want 0 (сброс базы)", got)
	}
	if pct := snap.Render.Engines[1].SuccessPct; pct == nil || *pct != 100 {
		t.Errorf("успешность после рестарта = %v, want 100 (5/5)", pct)
	}
	if pct := snap.Render.Fallback.RatePct; pct == nil || *pct != 0 {
		t.Errorf("fallback-рейт после рестарта = %v, want 0 (публикаций нет, этап жив)", pct)
	}
	if got := snap.Render.Fallback.ConsumedPerSec; got != 0 {
		t.Errorf("consumed темп без движения = %v, want 0", got)
	}
}

// TestCollectRenderDegradation — addresses not set — the section is
// "n/a" without an error (the health style); an unavailable exposure —
// the section degraded with a reason, the other sections alive; a
// broken exposure — a parse error.
func TestCollectRenderDegradation(t *testing.T) {
	// Not configured — not an error.
	coll, _ := newRenderCollector(t, &renderLens{}, nil)
	snap := coll.CollectOnce(context.Background())
	if snap.Render != nil {
		t.Error("render без адресов не nil, want «н/д»")
	}
	if _, ok := snap.Errors["render"]; ok {
		t.Errorf("адреса не заданы — ошибка секции: %v", snap.Errors)
	}

	// The exposure unavailable (a closed server) — the section
	// degrades, queues/zones alive (§5.9).
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dead.URL
	dead.Close()
	coll, _ = newRenderCollector(t, &renderLens{}, []string{deadURL})
	snap = coll.CollectOnce(context.Background())
	if snap.Render != nil {
		t.Error("render при недоступной экспозиции не nil")
	}
	if snap.Errors["render"] == "" {
		t.Errorf("errors без причины render: %v", snap.Errors)
	}
	if len(snap.Errors) != 1 || snap.Zones == nil || snap.Queues == nil {
		t.Errorf("деградация задела чужие секции: %v", snap.Errors)
	}

	// A broken exposure (labels not closed) — a parse error with a
	// reason.
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("m{x=\"1\" 5\n"))
	}))
	defer broken.Close()
	coll, _ = newRenderCollector(t, &renderLens{}, []string{broken.URL})
	snap = coll.CollectOnce(context.Background())
	if snap.Render != nil || snap.Errors["render"] == "" {
		t.Errorf("битая экспозиция: render=%v errors=%v, want деградация", snap.Render, snap.Errors)
	}

	// The depth unavailable (Redis down) — Depth nil, the section
	// alive: a panel without the queue remainder is better than no
	// panel.
	alive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(renderWorkerExpo{engine: "chromium", rendered: 10}.exposition()))
	}))
	defer alive.Close()
	coll, _ = newRenderCollector(t, &renderLens{err: errors.New("redis: timeout")}, []string{alive.URL})
	snap = coll.CollectOnce(context.Background())
	if snap.Render == nil || snap.Render.Fallback.Depth != nil {
		t.Errorf("render=%v depth=%v, want секция жива с Depth nil", snap.Render, snap.Render.Fallback.Depth)
	}
	// An exposure without stage_tasks (no stage pages) — the fallback
	// rate is "—" (nil), not a panic and not 0.
	if snap.Render.Fallback.RatePct != nil {
		t.Errorf("rate_pct без страниц этапа = %v, want nil", *snap.Render.Fallback.RatePct)
	}
	if len(snap.Errors) != 1 || snap.Errors["queues"] == "" {
		t.Errorf("отказ Redis не отражён секцией очередей: %v", snap.Errors)
	}
}

// TestCollectRenderNotConfiguredByDefault — a collector without
// options (the default config: no addresses) — render always nil,
// /api/status serves null; the JSON field is present (the front
// contract).
func TestCollectRenderNotConfiguredByDefault(t *testing.T) {
	coll := newSnapCollector(t, healthyPool(), &fakeLenner{}, nil)
	snap := coll.CollectOnce(context.Background())
	if snap.Render != nil {
		t.Fatal("render при дефолтном конфиге не nil")
	}
}

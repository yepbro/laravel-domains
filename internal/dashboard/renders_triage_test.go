// Tests of the mutant triage of the CI run of 09.11.2026 (half A):
// finishers for renders.go — the exact parser values of the exposition
// (label bracket/quote boundaries with escapes, "}" inside quotes, "{"
// at line start, empty and whitespace lines, error line numbers,
// cutting the " #" tail, exponents), value unescape (a trailing slash,
// the \\, \", \n and unknown escapes), the add aggregation (sums per
// engines, RSS attribution, an empty engine), rateOf/observe (a
// window of 0 — a rate of 0, not Inf; a restart; no movement), the
// success/tarpit shares with a zero denominator (nil, not NaN), the
// fallback rate with tasks=1, an exposure status ≠ 200, a broken URL,
// the body limit.
package dashboard

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestPromLabelsEnd — the index of the closing bracket with quotes and
// escapes.
func TestPromLabelsEnd(t *testing.T) {
	cases := []struct {
		s    string
		want int
	}{
		{`{a="1"} 5`, 6},
		{`{a="x}y"} 5`, 8}, // } inside quotes — part of the value
		{`{a="\""} 5`, 7},  // an escaped quote
		{`{a="\}"`, -1},    // the escape swallows the closing bracket
		{`{a="1`, -1},      // not closed
		{`{}`, 1},          // empty labels
		{`{a="x\\"} 1`, 8}, // an escaped slash
		{`{a\} 5`, 3},      // an escape outside quotes does NOT escape: the bracket closes
		{`{"x"="1"} 5`, 8}, // a quote right after the bracket
	}
	for _, c := range cases {
		if got := promLabelsEnd(c.s, 0); got != c.want {
			t.Errorf("promLabelsEnd(%q) = %d, want %d", c.s, got, c.want)
		}
	}
}

// TestParsePromLabels — labels: pairs, commas/quotes in values,
// escapes, an empty string, pair errors.
func TestParsePromLabels(t *testing.T) {
	// An empty string/space — no labels (nil).
	for _, s := range []string{"", "  "} {
		labels, err := parsePromLabels(s)
		if err != nil || labels != nil {
			t.Errorf("parsePromLabels(%q) = (%v, %v), want nil,nil", s, labels, err)
		}
	}
	labels, err := parsePromLabels(`a="1",b="two"`)
	if err != nil || !reflect.DeepEqual(labels, map[string]string{"a": "1", "b": "two"}) {
		t.Errorf("the pairs = (%v, %v)", labels, err)
	}
	// A comma and a quote in a value.
	labels, err = parsePromLabels(`a="x,y",b="q\"r"`)
	if err != nil || labels["a"] != "x,y" || labels["b"] != `q"r` {
		t.Errorf("values with a comma/quote = (%v, %v)", labels, err)
	}
	// Escapes: \\ → a slash, \n → n (the current contract), unknown —
	// literal.
	labels, err = parsePromLabels(`a="p\\ath",b="li\ne",c="e\x"`)
	if err != nil || labels["a"] != `p\ath` || labels["b"] != "line" || labels["c"] != `e\x` {
		t.Errorf("the escapes = (%v, %v)", labels, err)
	}
	// A trailing slash in a value — literal (an unterminated escape).
	labels, err = parsePromLabels(`a="trail\"`)
	if err != nil || labels["a"] != `trail\` {
		t.Errorf("the trailing escape = (%v, %v)", labels, err)
	}
	// An empty value is valid (the len(v) < 2 boundary).
	labels, err = parsePromLabels(`a=""`)
	if err != nil || labels["a"] != "" {
		t.Errorf("an empty value = (%v, %v)", labels, err)
	}
	// Shape errors: a value without quotes, an unclosed quote, no "=",
	// a trailing unterminated escape (a shape error, not a panic), a
	// pair consisting of only a quote, a value with a closing quote
	// without an opening one.
	for _, bad := range []string{`a=1`, `a="1`, `a`, `a="1",b`, `a="x\`, `a="`, `a=xy"`} {
		if _, err := parsePromLabels(bad); err == nil {
			t.Errorf("%q: no error, want a label shape error", bad)
		}
	}
	// A pair with an invalid value before the comma: the flush error on
	// the separator (not swallowed by the following pairs).
	if _, err := parsePromLabels(`a=1,b="2"`); err == nil {
		t.Error("a broken pair before the comma: want an error in place")
	}
	// An empty pair between commas/in the tail is ignored (not an
	// error).
	labels, err = parsePromLabels(`a="1",,b="2"`)
	if err != nil || len(labels) != 2 || labels["a"] != "1" || labels["b"] != "2" {
		t.Errorf("an empty pair: (%v, %v)", labels, err)
	}
	labels, err = parsePromLabels(`a="1",`)
	if err != nil || len(labels) != 1 {
		t.Errorf("a trailing comma: (%v, %v)", labels, err)
	}
	// Spaces around a pair are trimmed (a trailing space does not break
	// the closing quote of the value).
	labels, err = parsePromLabels(`a="1" `)
	if err != nil || labels["a"] != "1" {
		t.Errorf("a pair with a trailing space: (%v, %v)", labels, err)
	}
	// An escape outside quotes does not escape a comma: the pair breaks
	// off and is declared broken; a name with a comma cannot be
	// assembled this way.
	if _, err := parsePromLabels(`a\,b="1"`); err == nil {
		t.Error("an escape outside quotes: want a pair shape error")
	}
	// An escaped quote in a value keeps the quote open: the comma after
	// it is part of the value, the second pair is not split off.
	labels, err = parsePromLabels(`a="q\",b="2"`)
	if err != nil || len(labels) != 1 || labels["a"] != `q",b="2` {
		t.Errorf("an escape inside quotes: (%v, %v)", labels, err)
	}
}

// TestPromUnescape — value escapes and a trailing slash (no panic).
func TestPromUnescape(t *testing.T) {
	cases := map[string]string{
		`p\\ath`: `p\ath`,
		`q\"r`:   `q"r`,
		`li\ne`:  "line",
		`e\x`:    `e\x`,
		`trail\`: `trail\`,
		`plain`:  "plain",
		`\`:      `\`,
		`\n`:     "n",
	}
	for in, want := range cases {
		if got := promUnescape(in); got != want {
			t.Errorf("promUnescape(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestParsePromExpositionExact — an exact parse of a canonical
// exposition: names, labels (including a name from a "{" at line
// start), values (exponent, negatives), HELP/TYPE/blank-line skips.
func TestParsePromExpositionExact(t *testing.T) {
	expo := "# HELP m Справка\n" +
		"# TYPE m counter\n" +
		"\n" +
		"   \n" +
		"m{a=\"1\",engine=\"moli\"} 1000\n" +
		"m 1e3\n" +
		"m{x=\"1\"} -2.5\n" +
		"m{} 3\n" +
		"{anon=\"yes\"} 7\n" +
		"m 5 # комментарий\n"
	samples, err := parsePromExposition(strings.NewReader(expo))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := []promSample{
		{Name: "m", Labels: map[string]string{"a": "1", "engine": "moli"}, Value: 1000},
		{Name: "m", Labels: nil, Value: 1000},
		{Name: "m", Labels: map[string]string{"x": "1"}, Value: -2.5},
		{Name: "m", Labels: nil, Value: 3},
		{Name: "", Labels: map[string]string{"anon": "yes"}, Value: 7},
		{Name: "m", Labels: nil, Value: 5},
	}
	if !reflect.DeepEqual(samples, want) {
		t.Errorf("samples =\n%+v\nwant:\n%+v", samples, want)
	}
}

// TestParsePromExpositionErrorLines — shape errors carry the exact line
// number and distinguish "no value" from "value not a number".
func TestParsePromExpositionErrorLines(t *testing.T) {
	cases := []struct {
		name    string
		expo    string
		wantSub string
	}{
		{"labels not closed (line 2)", "# T x\nm{a=\"1 5\n", "line 2: labels without a closing"},
		{"no value (line 1)", "m{x=\"1\"}\n", "line 1: no value"},
		{"no value after the hash", "m #x\n", "line 1: no value"},
		{"the value is not a number (line 3)", "m 1\nm 2\nm abc\n", "line 3: value"},
		{"a label without quotes (line 1)", "m{x=1} 5\n", "line 1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parsePromExposition(strings.NewReader(tc.expo))
			if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("the error = %v, want it to contain %q", err, tc.wantSub)
			}
		})
	}
}

// errReader — a reader yielding a prefix and then an error (a
// connection drop in the middle of an exposure).
type errReader struct {
	prefix []byte
	err    error
	done   bool
}

func (r *errReader) Read(p []byte) (int, error) {
	if len(r.prefix) > 0 {
		n := copy(p, r.prefix)
		r.prefix = r.prefix[n:]
		return n, nil
	}
	if !r.done {
		r.done = true
		return 0, r.err
	}
	return 0, r.err
}

// TestParsePromExpositionReaderError — a read error in the middle of a
// stream — "reading the exposure" with the original one (not a silent
// truncated parse).
func TestParsePromExpositionReaderError(t *testing.T) {
	r := &errReader{prefix: []byte("m 1\n"), err: errors.New("connection drop")}
	if _, err := parsePromExposition(r); err == nil ||
		!strings.Contains(err.Error(), "reading the exposition") ||
		!strings.Contains(err.Error(), "drop") {
		t.Errorf("the read error = %v, want reading the exposition with the original one", err)
	}
}

// TestRenderBodyLimitPin — the exposure body reading limit.
func TestRenderBodyLimitPin(t *testing.T) {
	if renderBodyLimit != 4<<20 {
		t.Errorf("renderBodyLimit = %d, want 4 MiB", renderBodyLimit)
	}
}

// TestRenderCountersAdd — a sample reduction: sums per engines and
// outcomes (two records of one engine in one exposure), stage pages,
// RSS attribution to the engine of the first rendered-true series with
// an engine (an empty engine before a valid record and foreign
// instrument names do not interfere); a second exposure of the same
// engine is summed.
func TestRenderCountersAdd(t *testing.T) {
	var agg renderCounters
	agg.rendered, agg.finalized, agg.tarpits = map[string]float64{}, map[string]float64{}, map[string]float64{}
	agg.consumed, agg.rss = map[string]float64{}, map[string]float64{}
	agg.add([]promSample{
		// A foreign instrument with rendered=true and an engine — not an
		// engine source.
		{Name: fbPublishedMetric, Labels: map[string]string{"rendered": "true", "engine": "spooky"}},
		// records with rendered=false — does not open the engine.
		{Name: renderRecordsMetric, Labels: map[string]string{"rendered": "false", "engine": "zzz"}},
		{Name: renderRecordsMetric, Labels: map[string]string{"rendered": "true"}}, // an empty engine
		{Name: renderRecordsMetric, Labels: map[string]string{"rendered": "true", "engine": "moli"}, Value: 100},
		{Name: renderRecordsMetric, Labels: map[string]string{"rendered": "true", "engine": "moli"}, Value: 50}, // sum 150
		// Another rendered-true series of a different engine AFTER the
		// working one — the attribution stays with the first (break).
		{Name: renderRecordsMetric, Labels: map[string]string{"rendered": "true", "engine": "late"}},
		{Name: renderRecordsMetric, Labels: map[string]string{"rendered": "false", "engine": "moli"}, Value: 9},
		{Name: renderFinalizedMetric, Labels: map[string]string{"engine": "moli"}, Value: 10},
		{Name: renderFinalizedMetric, Labels: map[string]string{"engine": "moli"}, Value: 5}, // 15
		{Name: renderTarpitMetric, Labels: map[string]string{"engine": "moli"}, Value: 2},
		{Name: renderTarpitMetric, Labels: map[string]string{"engine": "moli"}, Value: 3}, // 5
		{Name: fbPublishedMetric, Labels: map[string]string{"reason": "timeout"}, Value: 3},
		{Name: fbPublishedMetric, Labels: map[string]string{"reason": "other"}, Value: 4}, // 7
		{Name: fbConsumedMetric, Labels: map[string]string{"result": "rendered"}, Value: 1},
		{Name: fbConsumedMetric, Labels: map[string]string{"result": "rendered"}, Value: 2}, // 3
		{Name: stageTasksMetric, Labels: map[string]string{"stage": "render"}, Value: 40},
		{Name: stageTasksMetric, Labels: map[string]string{"stage": "http"}, Value: 999}, // a foreign stage
		{Name: processRSSMetric, Labels: nil, Value: 123},                               // → the moli engine
	})
	// A second exposure of the same engine (a second worker): the RSS
	// and the other cumulatives are summed across exposures.
	agg.add([]promSample{
		{Name: renderRecordsMetric, Labels: map[string]string{"rendered": "true", "engine": "moli"}, Value: 25},
		{Name: renderTarpitMetric, Labels: map[string]string{"engine": "moli"}, Value: 7},
		{Name: processRSSMetric, Labels: nil, Value: 100},
	})
	if agg.rendered["moli"] != 175 {
		t.Errorf("rendered[moli] = %v, want 175 (100+50+25)", agg.rendered["moli"])
	}
	if agg.finalized["moli"] != 15 {
		t.Errorf("finalized[moli] = %v, want 15", agg.finalized["moli"])
	}
	if agg.tarpits["moli"] != 12 {
		t.Errorf("tarpits[moli] = %v, want 12 (2+3+7)", agg.tarpits["moli"])
	}
	if agg.published != 7 {
		t.Errorf("published = %v, want 7 (3+4)", agg.published)
	}
	if agg.consumed["rendered"] != 3 {
		t.Errorf("consumed[rendered] = %v, want 3", agg.consumed["rendered"])
	}
	if agg.tasks != 40 {
		t.Errorf("tasks = %v, want 40 (stage=render only)", agg.tasks)
	}
	if agg.rss["moli"] != 223 {
		t.Errorf("rss[moli] = %v, want 223 (123+100)", agg.rss["moli"])
	}
	for _, ghost := range []string{"spooky", "zzz", "late"} {
		if _, ok := agg.rss[ghost]; ok {
			t.Errorf("the RSS attributed to a foreign engine %s", ghost)
		}
	}
}

// TestRateOfBoundaries — a window of 0/negative and no growth — a rate
// of 0; a positive window — the exact share.
func TestRateOfBoundaries(t *testing.T) {
	if got := rateOf(10, 20, 0); got != 0 {
		t.Errorf("rateOf dt=0 = %v, want 0 (not Inf)", got)
	}
	if got := rateOf(10, 20, -time.Second); got != 0 {
		t.Errorf("rateOf dt<0 = %v, want 0", got)
	}
	if got := rateOf(20, 20, time.Second); got != 0 {
		t.Errorf("rateOf without growth = %v, want 0", got)
	}
	if got := rateOf(10, 20, 10*time.Second); got != 1 {
		t.Errorf("rateOf = %v, want 1/s", got)
	}
	// A one-nanosecond window — a legitimate positive: the growth is
	// counted (the "dt <= 1" mutant would swallow it into zero; 10 in
	// 1 ns = 1e10/s).
	if got := rateOf(10, 20, time.Nanosecond); got < 1e9 {
		t.Errorf("rateOf dt=1ns = %v, want ~1e10/s (a positive window)", got)
	}
}

// TestRenderRatesObserve — a rate with a window of 0 (the clock did not
// move) — 0, not Inf; a negative window (the clock moved back) — also
// 0; a fractional window (half a second) — the rate is computed.
func TestRenderRatesObserve(t *testing.T) {
	st := renderRateState{seen: true, at: time.Unix(1000, 0),
		rendered: map[string]float64{"moli": 10}, published: 5, consumed: 3}
	cur := renderCounters{
		rendered:  map[string]float64{"moli": 20},
		published: 7,
		consumed:  map[string]float64{"rendered": 5},
	}
	// The clock did not move: dt = 0 — rates 0.
	out := st.observe(cur, time.Unix(1000, 0))
	if out.rendered["moli"] != 0 || out.published != 0 || out.consumed != 0 {
		t.Errorf("dt=0: the rates = %+v, want zeros", out)
	}
	// The clock moved back: dt < 0 — zeros too (not negatives).
	st2 := renderRateState{seen: true, at: time.Unix(2000, 0),
		rendered: map[string]float64{"moli": 10}, published: 5, consumed: 3}
	out = st2.observe(cur, time.Unix(1000, 0))
	if out.rendered["moli"] != 0 || out.published != 0 || out.consumed != 0 {
		t.Errorf("dt<0: the rates = %+v, want zeros", out)
	}
	// A fractional window of 0.5 s: rate = delta/0.5 (a doubled delta) —
	// a window shorter than a second works fully; consumption — the sum
	// over outcomes (two consumed branches).
	st3 := renderRateState{seen: true, at: time.Unix(1000, 0),
		rendered: map[string]float64{"moli": 10}, published: 5, consumed: 3}
	cur2 := renderCounters{
		rendered:  map[string]float64{"moli": 20},
		published: 7,
		consumed:  map[string]float64{"rendered": 5, "finalized": 1},
	}
	out = st3.observe(cur2, time.Unix(1000, 0).Add(500*time.Millisecond))
	if out.rendered["moli"] != 20 || out.published != 4 || out.consumed != 6 {
		t.Errorf("dt=0.5s: the rates = %+v, want 20/4/6 (consumed = (5+1−3)/0.5)", out)
	}
}

// TestCollectRenderZeroDenominators — an engine with only RSS (no
// finalizations): success and the tarpit rate — nil "—", not NaN/0.
func TestCollectRenderZeroDenominators(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(
			"crawler_render_records_total{rendered=\"true\",engine=\"only-rss\"} 0\n" +
				"process_resident_memory_bytes " + formatFloat(1<<20) + "\n"))
	}))
	defer srv.Close()
	coll, _ := newRenderCollector(t, &renderLens{}, []string{srv.URL})
	snap := coll.CollectOnce(context.Background())
	if snap.Render == nil || len(snap.Render.Engines) != 1 {
		t.Fatalf("engines = %+v", snap.Render)
	}
	e := snap.Render.Engines[0]
	if e.SuccessPct != nil {
		t.Errorf("the success with a zero denominator = %v, want nil", *e.SuccessPct)
	}
	if e.TarpitPct != nil {
		t.Errorf("the tarpit rate with a zero denominator = %v, want nil", *e.TarpitPct)
	}
}

// TestCollectRenderUnitDenominators — a denominator of 1 (the minimum
// non-zero): the shares are computed (0% and 100%), not extinguished
// by nil.
func TestCollectRenderUnitDenominators(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(
			"crawler_render_records_total{rendered=\"true\",engine=\"e1\"} 0\n" +
				"crawler_render_task_finalized_total{reason=\"timeout\",engine=\"e1\"} 1\n" +
				"crawler_render_tarpit_finalized_total{engine=\"e1\"} 0\n" +
				"crawler_stage_tasks_total{stage=\"render\"} 1\n"))
	}))
	defer srv.Close()
	coll, _ := newRenderCollector(t, &renderLens{}, []string{srv.URL})
	snap := coll.CollectOnce(context.Background())
	e := snap.Render.Engines[0]
	if e.SuccessPct == nil || *e.SuccessPct != 0 {
		t.Errorf("the success with a denominator of 1 = %v, want 0%%", e.SuccessPct)
	}
	if e.TarpitPct == nil || *e.TarpitPct != 0 {
		t.Errorf("the tarpit rate with a denominator of 1 = %v, want 0%%", e.TarpitPct)
	}
	// Exactly one stage page, no publications: the fallback rate is an
	// honest 0%, not "—".
	if snap.Render.Fallback.RatePct == nil || *snap.Render.Fallback.RatePct != 0 {
		t.Errorf("rate_pct at tasks=1 = %v, want 0%%", snap.Render.Fallback.RatePct)
	}
}

// TestFetchRenderMetricsBadURL — a broken URL — an exposure error (no
// panic on a nil request).
func TestFetchRenderMetricsBadURL(t *testing.T) {
	coll, _ := newRenderCollector(t, &renderLens{}, []string{"::://broken"})
	if _, err := coll.fetchRenderMetrics(context.Background(), "::://broken"); err == nil {
		t.Error("a broken URL: want an exposition error")
	}
}

// TestFetchRenderMetricsBadStatus — a status ≠ 200 with a valid body —
// a "status" error, the body is not parsed as an exposure.
func TestFetchRenderMetricsBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("metric_ok 1\n"))
	}))
	defer srv.Close()
	coll, _ := newRenderCollector(t, &renderLens{}, []string{srv.URL})
	_, err := coll.fetchRenderMetrics(context.Background(), srv.URL)
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Errorf("a 500 status: %v, want an error with the status", err)
	}
	// The section degrades entirely, render = nil.
	snap := coll.CollectOnce(context.Background())
	if snap.Render != nil || snap.Errors["render"] == "" {
		t.Errorf("render=%v errors=%v, want a section degradation", snap.Render, snap.Errors)
	}
}

// TestFetchRenderMetricsTruncatedBody — a body drop in the middle of a
// response (Content-Length larger than the fact) — a "reading" error,
// not a truncated parse.
func TestFetchRenderMetricsTruncatedBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "1000")
		_, _ = w.Write([]byte("m 1\n")) // the body is shorter than promised — a drop
	}))
	defer srv.Close()
	coll, _ := newRenderCollector(t, &renderLens{}, []string{srv.URL})
	_, err := coll.fetchRenderMetrics(context.Background(), srv.URL)
	if err == nil || !strings.Contains(err.Error(), "read") {
		t.Errorf("a body drop = %v, want a read error", err)
	}
}

// TestCollectRenderCountersNoURLs — no addresses: an empty aggregate
// without touching the sources (nil maps, no exposures).
func TestCollectRenderCountersNoURLs(t *testing.T) {
	coll, _ := newRenderCollector(t, &renderLens{}, nil)
	agg, err := coll.collectRenderCounters(context.Background())
	if err != nil {
		t.Fatalf("without addresses: %v", err)
	}
	if agg.rendered != nil || agg.finalized != nil || agg.tarpits != nil ||
		agg.consumed != nil || agg.rss != nil || agg.published != 0 || agg.tasks != 0 {
		t.Errorf("the aggregate without addresses = %+v, want zero (nil maps)", agg)
	}
}

// TestCollectRenderSameClockNoInf — two passes with the same clock:
// rates 0 (a window of 0), not Inf/NaN — the panels do not break with a
// spike.
func TestCollectRenderSameClockNoInf(t *testing.T) {
	expo := "crawler_render_records_total{rendered=\"true\",engine=\"moli\"} 100\n" +
		"crawler_render_fallback_published_total{reason=\"timeout\"} 5\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(expo))
	}))
	defer srv.Close()
	coll, _ := newRenderCollector(t, &renderLens{}, []string{srv.URL})
	for i := 0; i < 2; i++ {
		snap := coll.CollectOnce(context.Background())
		if snap.Render == nil {
			t.Fatalf("pass %d: render nil", i)
		}
		if math.IsInf(snap.Render.Engines[0].RenderedPerSec, 0) ||
			math.IsNaN(snap.Render.Engines[0].RenderedPerSec) {
			t.Fatalf("pass %d: the rate = %v, want 0", i, snap.Render.Engines[0].RenderedPerSec)
		}
		if math.IsInf(snap.Render.Fallback.PublishedPerSec, 0) {
			t.Fatalf("pass %d: the published rate = %v", i, snap.Render.Fallback.PublishedPerSec)
		}
	}
	// Another pass with a live section.
	if snap := coll.CollectOnce(context.Background()); snap.Render == nil {
		t.Fatal("the section died")
	}
}

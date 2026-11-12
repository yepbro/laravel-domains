// Unit tests for crawlerctl fixtures snapshot (T-244): Prometheus
// exposition text format parsing (labels, escapes, buckets), collecting
// the render stage's instruments, structural DOM metrics
// (nodes/scripts), stream boundaries by record IDs, the snapshot's SQL
// contract and the file write/read roundtrip. A live snapshot run
// against the dev stack is the task's self-check (the results go into
// the report), not a test.
package main

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"crawler/internal/content"
)

// TestFixturesParsePromMetrics — exposition text format parsing: a
// sample without labels, with labels, escapes in values, HELP/TYPE
// skipped, broken lines are errors.
func TestFixturesParsePromMetrics(t *testing.T) {
	data := []byte(strings.Join([]string{
		`# HELP crawler_stage_tasks_total Задачи этапов.`,
		`# TYPE crawler_stage_tasks_total counter`,
		`crawler_stage_tasks_total{stage="dns"} 12`,
		`crawler_stage_tasks_total{stage="render"} 34`,
		`process_start_time_seconds 1.76e9`,
		`crawler_render_task_finalized_total{reason="net::ERR_CONNECTION_REFUSED"} 3`,
		`crawler_stage_seconds_bucket{stage="render",le="0.5"} 10`,
		`crawler_stage_seconds_bucket{stage="render",le="+Inf"} 100`,
		`crawler_render_records_total{rendered="true"} 42 1700000000000`,
		`weird_label{path="C:\\dir\\\"file",mode="a\nb"} 1`,
		`comma_label{list="a,b,c",stage="render"} 7`,
		`exemplar_metric{stage="render"} 5 # {trace_id="x,y"} 0.3 1234`,
		``,
	}, "\n"))
	samples, err := fixturesParsePromMetrics(data)
	if err != nil {
		t.Fatalf("разбор: %v", err)
	}
	byName := map[string][]fixturesPromSample{}
	for _, s := range samples {
		byName[s.Name] = append(byName[s.Name], s)
	}
	if got := byName["crawler_stage_tasks_total"][1]; got.Value != 34 || got.Labels["stage"] != "render" {
		t.Errorf("tasks_total render = %+v", got)
	}
	if got := byName["process_start_time_seconds"][0]; got.Value != 1.76e9 {
		t.Errorf("process_start = %+v", got)
	}
	if got := byName["crawler_render_task_finalized_total"][0]; got.Labels["reason"] != "net::ERR_CONNECTION_REFUSED" {
		t.Errorf("reason = %+v", got.Labels)
	}
	if got := byName["crawler_stage_seconds_bucket"][1]; got.Labels["le"] != "+Inf" || got.Value != 100 {
		t.Errorf("bucket +Inf = %+v", got)
	}
	if got := byName["crawler_render_records_total"][0]; got.Value != 42 {
		t.Errorf("records (с таймстампом в хвосте) = %+v", got)
	}
	if got := byName["weird_label"][0]; got.Labels["path"] != `C:\dir\"file` || got.Labels["mode"] != "a\nb" {
		t.Errorf("экраны меток = %q / %q", got.Labels["path"], got.Labels["mode"])
	}
	if got := byName["comma_label"][0]; got.Labels["list"] != "a,b,c" || got.Value != 7 {
		t.Errorf("запятая в значении метки = %+v", got)
	}
	if got := byName["exemplar_metric"][0]; got.Value != 5 {
		t.Errorf("сэмпл с exemplar-хвостом = %+v, хочу значение 5", got)
	}

	if _, err := fixturesParsePromMetrics([]byte("metric{bad 1\n")); err == nil {
		t.Errorf("битые метки не пойманы")
	}
	if _, err := fixturesParsePromMetrics([]byte("metric\n")); err == nil {
		t.Errorf("сэмпл без значения не пойман")
	}
	if _, err := fixturesParsePromMetrics([]byte("metric xyz\n")); err == nil {
		t.Errorf("нечисловое значение не поймано")
	}
}

// TestFixturesMetricsFromSamples — stage instrument collection: samples
// of foreign stages are filtered, the reason dictionaries are collected,
// the buckets are sorted.
func TestFixturesMetricsFromSamples(t *testing.T) {
	samples := []fixturesPromSample{
		{Name: "crawler_stage_tasks_total", Labels: map[string]string{"stage": "dns"}, Value: 10},
		{Name: "crawler_stage_tasks_total", Labels: map[string]string{"stage": "render"}, Value: 77},
		{Name: "crawler_stage_seconds_sum", Labels: map[string]string{"stage": "render"}, Value: 210},
		{Name: "crawler_stage_seconds_count", Labels: map[string]string{"stage": "render"}, Value: 70},
		{Name: "crawler_stage_seconds_bucket", Labels: map[string]string{"stage": "render", "le": "+Inf"}, Value: 70},
		{Name: "crawler_stage_seconds_bucket", Labels: map[string]string{"stage": "render", "le": "5"}, Value: 60},
		{Name: "crawler_stage_seconds_bucket", Labels: map[string]string{"stage": "render", "le": "1"}, Value: 30},
		{Name: "crawler_stage_errors_total", Labels: map[string]string{"stage": "render", "type": "browser"}, Value: 2},
		{Name: "crawler_stage_errors_total", Labels: map[string]string{"stage": "http", "type": "timeout"}, Value: 99},
		{Name: "crawler_render_task_finalized_total", Labels: map[string]string{"reason": "timeout"}, Value: 4},
		{Name: "crawler_render_task_finalized_total", Labels: map[string]string{"reason": "net::ERR_TIMED_OUT"}, Value: 6},
		{Name: "crawler_render_fallback_published_total", Labels: map[string]string{"reason": "timeout"}, Value: 3},
		{Name: "crawler_render_tarpit_finalized_total", Value: 1},
		{Name: "crawler_render_dns_finalized_total", Value: 2},
		{Name: "crawler_render_cert_finalized_total", Value: 3},
		{Name: "process_start_time_seconds", Value: 1000},
	}
	m := fixturesMetricsFromSamples(samples)
	if m.Tasks != 77 {
		t.Errorf("tasks = %v, хочу 77", m.Tasks)
	}
	if m.SecondsSum != 210 || m.SecondsCount != 70 {
		t.Errorf("seconds = %v/%v, хочу 210/70", m.SecondsSum, m.SecondsCount)
	}
	if len(m.Buckets) != 3 || m.Buckets[0].LE != 1 || m.Buckets[2].LE != math.MaxFloat64 {
		t.Errorf("корзины не отсортированы/без сентинела +Inf: %+v", m.Buckets)
	}
	if m.Errors["browser"] != 2 || m.Errors["timeout"] != 0 {
		t.Errorf("errors = %v (http-стадия должна была отфильтроваться)", m.Errors)
	}
	if m.TaskFinalized["net::ERR_TIMED_OUT"] != 6 || m.TaskFinalized["timeout"] != 4 {
		t.Errorf("task_finalized = %v", m.TaskFinalized)
	}
	if m.FallbackPublished["timeout"] != 3 || m.TarpitFinalized != 1 || m.DNSFinalized != 2 || m.CertFinalized != 3 {
		t.Errorf("фолбэк/тарпит/dns/cert = %v/%v/%v/%v", m.FallbackPublished, m.TarpitFinalized, m.DNSFinalized, m.CertFinalized)
	}
	if m.ProcessStartSeconds != 1000 {
		t.Errorf("process_start = %v", m.ProcessStartSeconds)
	}
}

// fakeStreamClient — a scenario-driven Redis client: stream lengths and
// boundaries.
type fakeStreamClient struct {
	lens    map[string]int64
	firstID string
	lastID  string
}

func (f *fakeStreamClient) XLen(_ context.Context, key string) *redis.IntCmd {
	return redis.NewIntResult(f.lens[key], nil)
}

func (f *fakeStreamClient) XRangeN(_ context.Context, _, _, _ string, _ int64) *redis.XMessageSliceCmd {
	return redis.NewXMessageSliceCmdResult([]redis.XMessage{{ID: f.firstID}}, nil)
}

func (f *fakeStreamClient) XRevRangeN(_ context.Context, _, _, _ string, _ int64) *redis.XMessageSliceCmd {
	return redis.NewXMessageSliceCmdResult([]redis.XMessage{{ID: f.lastID}}, nil)
}

// TestFixturesSnapshotStreams — a snapshot of the streams: the lengths
// and the q:analyze boundaries by record IDs; an empty q:analyze — zero
// boundaries.
func TestFixturesSnapshotStreams(t *testing.T) {
	fake := &fakeStreamClient{
		lens:    map[string]int64{"q:render": 0, "q:render-fallback": 2, "q:analyze": 500, "q:dead": 1},
		firstID: "1762000000000-0",
		lastID:  "1762000300000-7",
	}
	s, err := fixturesSnapshotTakeStreams(context.Background(), fake)
	if err != nil {
		t.Fatalf("стримы: %v", err)
	}
	if s.RenderLen != 0 || s.FallbackLen != 2 || s.AnalyzeLen != 500 || s.DeadLen != 1 {
		t.Fatalf("длины = %+v", s)
	}
	if s.AnalyzeFirstMs != 1762000000000 || s.AnalyzeLastMs != 1762000300000 {
		t.Fatalf("границы q:analyze = %d..%d", s.AnalyzeFirstMs, s.AnalyzeLastMs)
	}

	fake.lens["q:analyze"] = 0
	s, err = fixturesSnapshotTakeStreams(context.Background(), fake)
	if err != nil {
		t.Fatalf("стримы: %v", err)
	}
	if s.AnalyzeFirstMs != 0 || s.AnalyzeLastMs != 0 {
		t.Fatalf("пустой q:analyze с границами: %+v", s)
	}
}

// TestFixturesStreamIDMs — a record ID: milliseconds and junk
// rejection.
func TestFixturesStreamIDMs(t *testing.T) {
	if ms, ok := fixturesStreamIDMs("1762000000123-42"); !ok || ms != 1762000000123 {
		t.Errorf("ID = %d/%v", ms, ok)
	}
	for _, bad := range []string{"1762000000123", "0-0", "-1-0", "x-1"} {
		if _, ok := fixturesStreamIDMs(bad); ok {
			t.Errorf("ID %q принят", bad)
		}
	}
}

// TestFixturesSnapshotDOMStats — the DOM's nodes and scripts: the
// structure, not bytes (a normalized comparison of the live DOM).
func TestFixturesSnapshotDOMStats(t *testing.T) {
	dom := []byte(`<!DOCTYPE html><html><head><script>1</script></head>` +
		`<body><p>текст</p><script>2</script><!--комментарий--><div><span/></div></body></html>`)
	nodes, scripts := fixturesSnapshotDOMStats(dom)
	// The tree: document, html, head, script, text, body, p, text,
	// script, text (the comment), div, span — the exact number is not
	// hardcoded, we check the order and the non-zero parts; exactly 2
	// scripts.
	if scripts != 2 {
		t.Errorf("скриптов %d, хочу 2", scripts)
	}
	if nodes <= scripts {
		t.Errorf("узлов %d ≤ скриптов", nodes)
	}
	if n, s := fixturesSnapshotDOMStats(nil); n != 0 || s != 0 {
		t.Errorf("пустой DOM = %d/%d, хочу 0/0", n, s)
	}
}

// fakeContentGetter — a scenario-driven content store: hash → DOM.
type fakeContentGetter map[string][]byte

func (f fakeContentGetter) Get(_ context.Context, hash content.Hash) ([]byte, error) {
	dom, ok := f[hash.String()]
	if !ok {
		return nil, errors.New("нет контента")
	}
	return dom, nil
}

// TestFixturesSnapshotParseDOM — structural metrics over the store:
// counted rows, missing content skipped.
func TestFixturesSnapshotParseDOM(t *testing.T) {
	dom := []byte("<html><body><p>x</p><script>y</script></body></html>")
	hash := content.HashOf(dom)
	store := fakeContentGetter{hash.String(): dom}
	good := fixturesSnapshotRow{HasContent: true, ContentHash: hash.String()}
	missing := fixturesSnapshotRow{HasContent: true, ContentHash: "00" + hash.String()[2:]}
	broken := fixturesSnapshotRow{HasContent: true, ContentHash: "zz"}
	rows := []fixturesSnapshotRow{good, missing, broken, {HasContent: false}}

	skipped := fixturesSnapshotParseDOM(context.Background(), store, rows)
	if skipped != 2 {
		t.Fatalf("пропусков %d, хочу 2 (нет контента, битый hex)", skipped)
	}
	if !rows[0].DOMParsed || rows[0].DOMScripts != 1 || rows[0].DOMNodes == 0 {
		t.Fatalf("строка 0 не разобрана: %+v", rows[0])
	}
	if rows[1].DOMParsed || rows[2].DOMParsed || rows[3].DOMParsed {
		t.Fatalf("лишние строки разобраны: %+v", rows)
	}
}

// TestFixturesSnapshotRowsSQLContract — the snapshot's SQL contract: the
// dump's keys as unnest arrays, the content and the analyzer's verdicts
// via LEFT JOINs.
func TestFixturesSnapshotRowsSQLContract(t *testing.T) {
	for _, want := range []string{
		"unnest($1::int2[], $2::int8[], $3::int2[], $4::int2[])",
		"JOIN subdomain_results AS r",
		"LEFT JOIN contents AS c ON c.hash = r.content_hash",
		"LEFT JOIN laravel_findings AS f",
		"c.raw_len",
		"f.is_laravel, f.score, f.rules_version",
	} {
		if !strings.Contains(fixturesSnapshotRowsSQL, want) {
			t.Errorf("SQL снапшота без %q:\n%s", want, fixturesSnapshotRowsSQL)
		}
	}
}

// TestFixturesSnapshotFileRoundtrip — snapshot file write/read: an
// atomic tmp+rename, a field roundtrip, a foreign format version is an
// error.
func TestFixturesSnapshotFileRoundtrip(t *testing.T) {
	dir := t.TempDir()
	rss := int64(96 << 20)
	crashes := int64(0)
	rowRules, findingRules := int32(3), int32(7)
	snap := &fixturesSnapshot{
		FormatVersion: fixturesSnapshotVersion,
		Label:         "moli-1",
		Engine:        "moli",
		CreatedAt:     time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC),
		Dump:          fixturesSnapshotDump{Seed: 242, Keys: 2, Strata: map[string]fixturesStratumStat{"ok": {Quota: 2, Taken: 2}}},
		Rows: []fixturesSnapshotRow{
			{ZoneID: 1, DomainID: 5, SubdomainID: 0, Variant: 0, Rendered: true, HasContent: true,
				ContentHash: "aabb", DOMLen: 100, DOMParsed: true, DOMNodes: 9, DOMScripts: 1,
				RulesVersion: &rowRules, LaravelFound: true, IsLaravel: true,
				Score: 0.9, FindingRulesVersion: &findingRules},
			{ZoneID: 1, DomainID: 6, SubdomainID: 0, Variant: 0},
		},
		Streams:       &fixturesSnapshotStreams{AnalyzeLen: 2, AnalyzeFirstMs: 100, AnalyzeLastMs: 200},
		Metrics:       &fixturesSnapshotMetrics{Tasks: 2, TaskFinalized: map[string]float64{"timeout": 1}, Errors: map[string]float64{}},
		EngineRSSMax:  &rss,
		EngineCrashes: &crashes,
	}
	path := filepath.Join(dir, "moli-1"+fixturesSnapshotFileSuffix)
	if err := fixturesWriteSnapshotFile(path, snap); err != nil {
		t.Fatalf("запись: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("tmp-файл остался: %v", err)
	}

	got, err := fixturesReadSnapshot(path)
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if got.Label != snap.Label || got.Engine != snap.Engine || len(got.Rows) != 2 {
		t.Fatalf("roundtrip полей: %+v", got)
	}
	if got.Rows[0].ContentHash != "aabb" || got.Rows[0].FindingRulesVersion == nil || *got.Rows[0].FindingRulesVersion != 7 {
		t.Fatalf("roundtrip строки: %+v", got.Rows[0])
	}
	if got.Rows[0].RulesVersion == nil || *got.Rows[0].RulesVersion != 3 {
		t.Fatalf("roundtrip штампа строки (rules_version): %+v", got.Rows[0])
	}
	if got.Streams == nil || got.Metrics == nil || got.EngineRSSMax == nil || *got.EngineRSSMax != rss {
		t.Fatalf("roundtrip источников: %+v", got)
	}
	if got.Metrics.TaskFinalized["timeout"] != 1 {
		t.Fatalf("roundtrip словаря причин: %+v", got.Metrics.TaskFinalized)
	}

	snap.FormatVersion = 99
	if err := fixturesWriteSnapshotFile(path, snap); err != nil {
		t.Fatalf("запись v99: %v", err)
	}
	if _, err := fixturesReadSnapshot(path); err == nil {
		t.Errorf("чужая версия формата принята")
	}

	snap.FormatVersion = fixturesSnapshotVersion
	snap.Rows = nil
	if err := fixturesWriteSnapshotFile(path, snap); err != nil {
		t.Fatalf("запись пустого: %v", err)
	}
	if _, err := fixturesReadSnapshot(path); err == nil {
		t.Errorf("снапшот без строк принят")
	}
}

// TestRunFixturesSnapshotAndReport — command usage: help, flag
// validation before infrastructure (code 2), a metrics source failure
// (code 1), a report over snapshot files without infrastructure (code 0)
// and --fail-exit on a corrupted run.
func TestRunFixturesSnapshotAndReport(t *testing.T) {
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	cases := []runCase{
		{name: "snapshot --help", args: []string{"fixtures", "snapshot", "--help"}, code: exitOK, inOut: "--engine-rss-max"},
		{name: "snapshot help", args: []string{"fixtures", "snapshot", "help"}, code: exitOK, inOut: "--label ИМЯ"},
		{name: "report --help", args: []string{"fixtures", "report", "--help"}, code: exitOK, inOut: "--fail-exit"},
		{name: "snapshot без --dir", args: []string{"fixtures", "snapshot", "--label", "x", "--engine", "moli"},
			code: exitUsage, inErr: "укажите каталог дампа"},
		{name: "snapshot без --label", args: []string{"fixtures", "snapshot", "--dir", "d", "--engine", "moli"},
			code: exitUsage, inErr: "--label"},
		{name: "snapshot метка с путём", args: []string{"fixtures", "snapshot", "--dir", "d", "--label", "a/b", "--engine", "moli"},
			code: exitUsage, inErr: "--label"},
		{name: "snapshot движок мусор", args: []string{"fixtures", "snapshot", "--dir", "d", "--label", "x", "--engine", "gecko"},
			code: exitUsage, inErr: "--engine"},
		{name: "snapshot rss отрицательный", args: []string{"fixtures", "snapshot", "--dir", "d", "--label", "x", "--engine", "moli", "--engine-rss-max", "-1"},
			code: exitUsage, inErr: "--engine-rss-max"},
		{name: "snapshot лишний аргумент", args: []string{"fixtures", "snapshot", "--dir", "d", "--label", "x", "--engine", "moli", "stray"},
			code: exitUsage, inErr: "лишние аргументы"},
		{name: "snapshot валидный — нет конфигурации", args: []string{"fixtures", "snapshot", "--dir", "d", "--label", "x", "--engine", "moli"},
			code: exitError, inErr: "fixtures snapshot:"},
		{name: "report без --candidate", args: []string{"fixtures", "report", "--baseline", "b.json"},
			code: exitUsage, inErr: "укажите оба снапшота"},
		{name: "report бюджет нулевой", args: []string{"fixtures", "report", "--baseline", "b", "--candidate", "c", "--memory-budget", "0"},
			code: exitUsage, inErr: "--memory-budget"},
		{name: "report лишний аргумент", args: []string{"fixtures", "report", "--baseline", "b", "--candidate", "c", "stray"},
			code: exitUsage, inErr: "лишние аргументы"},
		{name: "report снапшот не найден", args: []string{"fixtures", "report", "--baseline", "nope.json", "--candidate", "nope2.json"},
			code: exitError, inErr: "fixtures report:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tc.args, &stdout, &stderr)
			if code != tc.code {
				t.Errorf("run(%q) код = %d, хочу %d; stderr:\n%s", tc.args, code, tc.code, stderr.String())
			}
			if tc.inErr != "" && !strings.Contains(stderr.String(), tc.inErr) {
				t.Errorf("run(%q) stderr не содержит %q:\n%s", tc.args, tc.inErr, stderr.String())
			}
			if tc.inOut != "" && !strings.Contains(stdout.String(), tc.inOut) {
				t.Errorf("run(%q) stdout не содержит %q:\n%s", tc.args, tc.inOut, stdout.String())
			}
		})
	}

	// Report over snapshot files: no infrastructure needed — code 0 and
	// all sections in stdout; a corrupted run + --fail-exit — code 1.
	dir := t.TempDir()
	b := snapOf("chromium-1", "chromium", snapRows(100, nil))
	cGood := snapOf("moli-1", "moli", snapRows(100, nil))
	cBad := snapOf("moli-2", "moli", snapRows(100, func(i int, r *fixturesSnapshotRow) {
		if i < 5 {
			r.IsLaravel = !r.IsLaravel // 5% detection divergence
		}
	}))
	pathB := filepath.Join(dir, "b.snapshot.json")
	pathGood := filepath.Join(dir, "good.snapshot.json")
	pathBad := filepath.Join(dir, "bad.snapshot.json")
	for p, s := range map[string]*fixturesSnapshot{pathB: b, pathGood: cGood, pathBad: cBad} {
		if err := fixturesWriteSnapshotFile(p, s); err != nil {
			t.Fatalf("запись %s: %v", p, err)
		}
	}
	outPath := filepath.Join(dir, "report.txt")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"fixtures", "report", "--baseline", pathB, "--candidate", pathGood, "--out", outPath}, &stdout, &stderr); code != exitOK {
		t.Fatalf("report: код %d, stderr:\n%s", code, stderr.String())
	}
	for _, want := range []string{"Вердикты против порогов §5.1 п.4", "детекция Laravel < 1%"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("stdout без %q:\n%s", want, stdout.String())
		}
	}
	if _, err := os.Stat(outPath); err != nil {
		t.Errorf("отчёт не записан в --out: %v", err)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"fixtures", "report", "--baseline", pathB, "--candidate", pathBad, "--fail-exit"}, &stdout, &stderr); code != exitError {
		t.Fatalf("report --fail-exit: код %d, хочу 1; stdout:\n%s", code, stdout.String())
	}
	if !strings.Contains(stderr.String(), "НЕ ПРОЙДЕН") {
		t.Errorf("stderr без вердикта:\n%s", stderr.String())
	}
	if !strings.Contains(stdout.String(), "НЕ ПРОЙДЕН") {
		t.Errorf("stdout без строк вердикта:\n%s", stdout.String())
	}
}

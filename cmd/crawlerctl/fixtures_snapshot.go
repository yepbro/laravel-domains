// ---------------------------------------------------------------- snapshot
//
// crawlerctl fixtures snapshot (T-244, the render engine selection task
// spec tasks/TZ_render_engines.md §5.1 item 3, §6.1 item 5): capturing
// one bench run's results into a self-sufficient JSON snapshot — the
// input of the comparison report (fixtures report). A repeated fixture
// import (T-243) returns the rows to the dump state and sweeps the run's
// content, so the "after the run" state cannot be read back from the DB:
// the snapshot is taken right after the run finishes (an empty q:render,
// the fallback drained) and before the next import/publish.
//
// Composition (the §5.1 item 3 comparison metrics):
//
//	DB (by the fixture dump's keys):
//	  - rendered/the content reference — the share of the run's
//	    successful renders: after the import content_hash = NULL (T-243),
//	    a reference appears only for a DOM written by the run — success =
//	    rows with content / the dump's tasks (the run must either render
//	    the task or finalize/leave it untouched — everything except
//	    writing a DOM honestly reduces the share);
//	  - the DOM length (contents.raw_len) and content_hash — structural
//	    metrics (the live DOM is non-deterministic byte-wise, the spec's
//	    byte-level diff is not applied; a hash match is a strong "same
//	    DOM" signal);
//	  - DOM nodes/scripts — reading the DOM from the content store
//	    (PackStore ReadOnly, the crawlerctl domain --content style) and
//	    parsing it;
//	  - the analyzer's verdicts (laravel_findings.is_laravel) — the
//	    material of the detection diff against baseline-chromium (the
//	    T-213 protocol);
//	Redis (the configuration's redis.addr): the q:analyze stream
//	boundaries by record IDs (publish milliseconds) and the stream
//	lengths — the run's pace (rows/s) and the queue leftovers; the
//	streams are cleared between runs (the T-243 instructions), so the
//	snapshot belongs to exactly one run;
//	stage metrics (the --metrics URL of the render-worker's /metrics):
//	finalization counters by the taskFinalizeReason dictionary
//	(net::ERR_*), tarpit/cert/DNS finalizations, fallback publications,
//	the page-time histogram (avg/p95), the browser error class (the
//	engine unavailable) and the worker process start moment — the report
//	uses it to tell one run's counters (the worker restarted between
//	runs) from cumulative ones;
//	the operator: --engine-rss-max (the engine container's max RSS over
//	the run — the dev stack has no engine memory exporter; spec §4.6
//	item 4 promises one only to production telemetry) and
//	--engine-crashes (engine core crashes).
//
// Fields whose source is unavailable (the analyzer has not finished the
// tail, the content swept by retention) do not forbid the snapshot: the
// corresponding report lines degrade honestly to "no data".

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/net/html"

	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/queue"
)

// fixturesSnapshotVersion — the snapshot format version (the snapshot →
// report contract; a composition change — a new version).
const fixturesSnapshotVersion = 1

// fixturesSnapshotFileSuffix — the default snapshot file name suffix
// (<dump dir>/<label>.snapshot.json — one bench's snapshots sit next to
// their dump).
const fixturesSnapshotFileSuffix = ".snapshot.json"

// fixturesLabelRe — an allowed snapshot label: no path separators (the
// label is part of the default file name).
var fixturesLabelRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`)

// fixturesSnapshotRow — a fixture row in the snapshot: the post-run
// state + the analyzer's verdict. The keys are the dump's original
// identifiers (T-242); the fields are compressed to what the report
// needs.
type fixturesSnapshotRow struct {
	ZoneID      int16  `json:"z"`
	DomainID    int64  `json:"d"`
	SubdomainID int16  `json:"s"`
	Variant     int16  `json:"v"`
	Rendered    bool   `json:"rendered"`               // the row's rendered (the run sets it together with the content)
	HasContent  bool   `json:"content"`                // content_hash NOT NULL — the DOM was written by the run
	ContentHash string `json:"content_hash,omitempty"` // the hex id of the DOM (the store's dedup)
	DOMLen      int    `json:"dom_len"`                // contents.raw_len, bytes (0 — no content)
	DOMParsed   bool   `json:"dom_parsed"`             // nodes/scripts counted over the store's DOM
	DOMNodes    int    `json:"dom_nodes"`              // HTML tree nodes (all, including text)
	DOMScripts  int    `json:"dom_scripts"`            // <script> elements
	// RulesVersion — the subdomain_results ROW's rules_version: the render
	// sets NULL (renderUpdateSQL), the analyzer stamps it after passing
	// over this DOM; NULL on a row with content = the analyzer has not
	// finished it in this run, the DB findings could be left over from
	// the previous one (review T-244 P1-2).
	RulesVersion *int32 `json:"row_rules_version,omitempty"`
	// LaravelFound — whether a laravel_findings row exists. A missing row
	// is the analyzer's NORMAL verdict "not a single hit"
	// (findingDeleteSQL removes it), not "not finished": the detection
	// diff treats it as is_laravel=false (review T-244 P1-1).
	// The row's RulesVersion guarantees freshness.
	LaravelFound        bool    `json:"laravel_found"`
	IsLaravel           bool    `json:"is_laravel"`                      // laravel_findings.is_laravel (when found)
	Score               float32 `json:"score"`                           // the findings' score (when found)
	FindingRulesVersion *int32  `json:"finding_rules_version,omitempty"` // the findings' rule version (when found)
}

// fixturesSnapshotDump — the dump manifest's digest for cross-checking a
// snapshot pair: the report requires both to be taken over one
// selection.
type fixturesSnapshotDump struct {
	Seed   int64                          `json:"seed"`
	Keys   int                            `json:"keys"`
	Strata map[string]fixturesStratumStat `json:"strata"`
}

// fixturesSnapshotStreams — a snapshot of the bench's Redis streams
// after the run: the q:analyze boundaries by ID (publish milliseconds
// by the worker) — the pace; the lengths — the queue leftovers and the
// fallback stream's size.
type fixturesSnapshotStreams struct {
	AnalyzeLen     int64 `json:"analyze_len"`      // the XLEN of q:analyze = tasks finished by the run
	AnalyzeFirstMs int64 `json:"analyze_first_ms"` // the first record's ms (0 — an empty stream)
	AnalyzeLastMs  int64 `json:"analyze_last_ms"`  // the last record's ms
	RenderLen      int64 `json:"render_len"`       // the XLEN of q:render (an undrained leftover)
	FallbackLen    int64 `json:"fallback_len"`     // the XLEN of q:render-fallback
	DeadLen        int64 `json:"dead_len"`         // the XLEN of q:dead
}

// fixturesHistBucket — a histogram's cumulative bucket (le ≤ …);
// +Inf — the closing one.
type fixturesHistBucket struct {
	LE    float64 `json:"le"`
	Count float64 `json:"count"`
}

// fixturesSnapshotMetrics — a snapshot of the render stage's instruments
// from the render-worker's /metrics (the counters are since the process
// start; the worker restarts between runs, in which case the values are
// exactly the run).
type fixturesSnapshotMetrics struct {
	ProcessStartSeconds float64              `json:"process_start_seconds"` // process_start_time_seconds (0 — the instrument not exported)
	Tasks               float64              `json:"stage_tasks_render"`    // crawler_stage_tasks_total{stage="render"}
	SecondsSum          float64              `json:"stage_seconds_sum"`     // crawler_stage_seconds_sum{stage="render"}
	SecondsCount        float64              `json:"stage_seconds_count"`   // crawler_stage_seconds_count{stage="render"}
	Buckets             []fixturesHistBucket `json:"buckets"`               // crawler_stage_seconds_bucket{stage="render"} (le ascending)
	Errors              map[string]float64   `json:"errors"`                // crawler_stage_errors_total{stage="render"} by type
	RenderedRecords     float64              `json:"rendered_records_true"` // crawler_render_records_total{rendered="true"}
	TarpitFinalized     float64              `json:"tarpit_finalized_total"`
	CertFinalized       float64              `json:"cert_finalized_total"`
	DNSFinalized        float64              `json:"dns_finalized_total"`
	TaskFinalized       map[string]float64   `json:"task_finalized_total"`     // reason → count (the net::ERR_* dictionary, T-172)
	FallbackPublished   map[string]float64   `json:"fallback_published_total"` // reason → count (T-238)
}

// fixturesSnapshot — a run snapshot (format v1): a JSON file, the input
// of fixtures report. Rows — by the dump's keys; Streams/Metrics are
// absent (null) if the source was not captured; Engine* — the operator's
// input (absent — the report degrades along the corresponding verdict
// line).
type fixturesSnapshot struct {
	FormatVersion int                      `json:"format_version"`
	Label         string                   `json:"label"`
	Engine        string                   `json:"engine"` // chromium|moli|lightpanda
	CreatedAt     time.Time                `json:"created_at"`
	Dump          fixturesSnapshotDump     `json:"dump"`
	Rows          []fixturesSnapshotRow    `json:"rows"`
	Streams       *fixturesSnapshotStreams `json:"streams,omitempty"`
	Metrics       *fixturesSnapshotMetrics `json:"metrics,omitempty"`
	EngineRSSMax  *int64                   `json:"engine_rss_max_bytes,omitempty"` // the engine's max RSS over the run (docker stats)
	EngineCrashes *int64                   `json:"engine_crashes,omitempty"`       // engine core crashes over the run
}

// ---------------------------------------------------------------- SQL

// fixturesSnapshotRowsSQL — the fixture rows' post-run state by the
// dump's keys (unnest arrays — the batch operation style of internal/db):
// the content reference and its length (JOIN contents), the analyzer's
// verdict (LEFT JOIN laravel_findings — the analyzer may not have
// finished the tail).
const fixturesSnapshotRowsSQL = "SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, " +
	"r.rendered, r.content_hash, c.raw_len, r.rules_version, " +
	"f.is_laravel, f.score, f.rules_version " +
	"FROM unnest($1::int2[], $2::int8[], $3::int2[], $4::int2[]) " +
	"AS k(zone_id, domain_id, subdomain_id, variant) " +
	"JOIN subdomain_results AS r ON r.zone_id = k.zone_id AND r.domain_id = k.domain_id " +
	"AND r.subdomain_id = k.subdomain_id AND r.variant = k.variant " +
	"LEFT JOIN contents AS c ON c.hash = r.content_hash " +
	"LEFT JOIN laravel_findings AS f ON f.zone_id = r.zone_id AND f.domain_id = r.domain_id " +
	"AND f.subdomain_id = r.subdomain_id AND f.variant = r.variant"

// ---------------------------------------------------------------- DB

// fixturesSnapshotReadRows — the SQL part of the snapshot: rows by the
// dump's keys.
func fixturesSnapshotReadRows(ctx context.Context, pool db.DB, keys []fixturesKey) ([]fixturesSnapshotRow, error) {
	zones := make([]int16, len(keys))
	domains := make([]int64, len(keys))
	subs := make([]int16, len(keys))
	variants := make([]int16, len(keys))
	for i, k := range keys {
		zones[i], domains[i], subs[i], variants[i] = k.zone, k.domain, k.subdomain, k.variant
	}
	rows, err := pool.Query(ctx, fixturesSnapshotRowsSQL, zones, domains, subs, variants)
	if err != nil {
		return nil, fmt.Errorf("снапшот: чтение строк фикстуры: %w", err)
	}
	defer rows.Close()
	out := make([]fixturesSnapshotRow, 0, len(keys))
	for rows.Next() {
		var r fixturesSnapshotRow
		var contentHash []byte
		var rawLen *int32
		var isLaravel *bool
		var score *float32
		if err := rows.Scan(&r.ZoneID, &r.DomainID, &r.SubdomainID, &r.Variant,
			&r.Rendered, &contentHash, &rawLen, &r.RulesVersion,
			&isLaravel, &score, &r.FindingRulesVersion); err != nil {
			return nil, fmt.Errorf("снапшот: чтение строк фикстуры: %w", err)
		}
		r.HasContent = contentHash != nil
		if r.HasContent {
			r.ContentHash = hex.EncodeToString(contentHash)
		}
		if rawLen != nil {
			r.DOMLen = int(*rawLen)
		}
		if isLaravel != nil {
			r.LaravelFound = true
			r.IsLaravel = *isLaravel
		}
		if score != nil {
			r.Score = *score
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("снапшот: чтение строк фикстуры: %w", err)
	}
	return out, nil
}

// fixturesSnapshotDOMStats — structural DOM metrics (nodes/scripts):
// the live DOM is non-deterministic byte-wise but the structure is
// stable — the normalized comparison of spec §5.1 item 3. All tree nodes
// (including text) and <script> elements are counted.
func fixturesSnapshotDOMStats(dom []byte) (nodes, scripts int) {
	if len(dom) == 0 {
		return 0, 0 // an empty DOM is not html.Parse's 4-node skeleton — there is no content
	}
	tree, err := html.Parse(bytes.NewReader(dom))
	if err != nil {
		return 0, 0 // a best-effort parser: errors are not fatal, but nothing is counted
	}
	var walk func(n *html.Node)
	walk = func(n *html.Node) {
		nodes++
		if n.Type == html.ElementNode && n.Data == "script" {
			scripts++
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(tree)
	return nodes, scripts
}

// contentGetter — a content reader (narrowing content.ContentStore to
// the Get the snapshot needs; *content.PackStore satisfies it).
type contentGetter interface {
	Get(ctx context.Context, hash content.Hash) ([]byte, error)
}

// fixturesSnapshotParseDOM — filling in the structural metrics of the
// snapshot rows from the content store's DOM. Missing content
// (retention/a repeated import) and read errors are a row degradation
// (DOMParsed=false), not a snapshot failure: the skip counter is
// returned for the operator's report.
func fixturesSnapshotParseDOM(ctx context.Context, store contentGetter, rows []fixturesSnapshotRow) (skipped int) {
	for i := range rows {
		if !rows[i].HasContent {
			continue
		}
		raw, err := hex.DecodeString(rows[i].ContentHash)
		if err != nil || len(raw) != content.HashLen {
			skipped++
			continue
		}
		var hash content.Hash
		copy(hash[:], raw)
		dom, err := store.Get(ctx, hash)
		if err != nil || len(dom) == 0 {
			skipped++
			continue
		}
		rows[i].DOMNodes, rows[i].DOMScripts = fixturesSnapshotDOMStats(dom)
		rows[i].DOMParsed = true
	}
	return skipped
}

// ---------------------------------------------------------------- Redis

// fixturesSnapshotTakeStreams — the bench's streams: the XLEN of
// q:render/q:render-fallback/q:analyze/q:dead and the q:analyze
// boundaries by record IDs (ms). Redis Streams IDs are the publish's
// "<ms>-<seq>"; before a run the streams are cleared (the T-243
// instructions), so the boundaries belong to the run.
func fixturesSnapshotTakeStreams(ctx context.Context, rdb redisStreamClient) (*fixturesSnapshotStreams, error) {
	s := &fixturesSnapshotStreams{}
	for _, st := range []struct {
		stream string
		dst    *int64
	}{
		{queue.StreamRender, &s.RenderLen},
		{queue.StreamRenderFallback, &s.FallbackLen},
		{queue.StreamAnalyze, &s.AnalyzeLen},
		{queue.StreamDead, &s.DeadLen},
	} {
		n, err := rdb.XLen(ctx, st.stream).Result()
		if err != nil {
			return nil, fmt.Errorf("снапшот: XLEN %s: %w", st.stream, err)
		}
		*st.dst = n
	}
	if s.AnalyzeLen > 0 {
		first, err := rdb.XRangeN(ctx, queue.StreamAnalyze, "-", "+", 1).Result()
		if err != nil {
			return nil, fmt.Errorf("снапшот: XRANGE %s: %w", queue.StreamAnalyze, err)
		}
		last, err := rdb.XRevRangeN(ctx, queue.StreamAnalyze, "+", "-", 1).Result()
		if err != nil {
			return nil, fmt.Errorf("снапшот: XREVRANGE %s: %w", queue.StreamAnalyze, err)
		}
		if len(first) > 0 && len(last) > 0 {
			var ok1, ok2 bool
			s.AnalyzeFirstMs, ok1 = fixturesStreamIDMs(first[0].ID)
			s.AnalyzeLastMs, ok2 = fixturesStreamIDMs(last[0].ID)
			if !ok1 || !ok2 {
				return nil, fmt.Errorf("снапшот: ID %s/%s без миллисекунд — темп не снимается", first[0].ID, last[0].ID)
			}
		}
	}
	return s, nil
}

// redisStreamClient — narrowing *redis.Client down to what the snapshot
// needs (XLen/XRangeN/XRevRangeN; the unit tests' seam).
type redisStreamClient interface {
	XLen(ctx context.Context, key string) *redis.IntCmd
	XRangeN(ctx context.Context, stream, start, stop string, count int64) *redis.XMessageSliceCmd
	XRevRangeN(ctx context.Context, stream, start, stop string, count int64) *redis.XMessageSliceCmd
}

// fixturesStreamIDMs — the milliseconds of a Redis Stream record ID
// ("<ms>-<seq>"); false — a non-standard ID.
func fixturesStreamIDMs(id string) (int64, bool) {
	ms, _, found := strings.Cut(id, "-")
	if !found {
		return 0, false
	}
	v, err := strconv.ParseInt(ms, 10, 64)
	if err != nil || v <= 0 {
		return 0, false
	}
	return v, true
}

// ---------------------------------------------------------------- metrics

// fixturesPromSample — an exposition text format sample (a value and
// labels).
type fixturesPromSample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// fixturesParsePromMetrics — a minimal exposition text format parser
// (the needed slice: samples with and without labels, HELP/TYPE are
// skipped). A own parser instead of expfmt: prometheus/common is an
// indirect dependency in the module graph, and the tool only needs a
// fixed slice of instruments.
func fixturesParsePromMetrics(data []byte) ([]fixturesPromSample, error) {
	var out []fixturesPromSample
	for n, line := range strings.Split(string(data), "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" || line[0] == '#' {
			continue
		}
		name := ""
		labels := map[string]string{}
		rest := ""
		if i := strings.IndexByte(line, '{'); i >= 0 {
			end := fixturesLabelsEnd(line, i)
			if end < 0 {
				return nil, fmt.Errorf("метрики: строка %d: метки без закрывающей скобки", n+1)
			}
			name = line[:i]
			parsed, err := fixturesParsePromLabels(line[i+1 : end])
			if err != nil {
				return nil, fmt.Errorf("метрики: строка %d: %w", n+1, err)
			}
			labels = parsed
			rest = line[end+1:]
		} else {
			name = strings.Fields(line)[0] // a label-less sample: the name is the line's first token
			rest = strings.TrimPrefix(strings.TrimSpace(line), name)
		}
		// The value is the first token after the name/labels; an
		// exposition-format timestamp and an OpenMetrics exemplar tail
		// ("# {…} …") may follow — not our case, cut off.
		if i := strings.Index(rest, " #"); i >= 0 {
			rest = rest[:i]
		}
		restFields := strings.Fields(rest)
		if len(restFields) == 0 {
			return nil, fmt.Errorf("метрики: строка %d: нет значения", n+1)
		}
		v, err := strconv.ParseFloat(restFields[0], 64)
		if err != nil {
			return nil, fmt.Errorf("метрики: строка %d: значение %q: %w", n+1, restFields[0], err)
		}
		out = append(out, fixturesPromSample{Name: name, Labels: labels, Value: v})
	}
	return out, nil
}

// fixturesLabelsEnd — the index of the '}' closing the label list
// (start — the index of the opening '{'); '}' and '{' inside value
// quotes are part of the value. -1 — the brace is not closed.
func fixturesLabelsEnd(s string, start int) int {
	inQuote := false
	for i := start + 1; i < len(s); i++ {
		switch {
		case s[i] == '\\' && inQuote && i+1 < len(s):
			i++
		case s[i] == '"':
			inQuote = !inQuote
		case s[i] == '}' && !inQuote:
			return i
		}
	}
	return -1
}

// fixturesParsePromLabels — a label list of name="value" pairs separated
// by commas. The separator is a comma OUTSIDE quotes: a comma inside a
// value is legal (text format); an exemplar tail ('# …' after the
// sample's value) is cut off earlier, when the value is split from the
// tail (see fixturesParsePromMetrics).
func fixturesParsePromLabels(s string) (map[string]string, error) {
	labels := map[string]string{}
	for _, pair := range fixturesSplitLabelPairs(s) {
		if pair == "" {
			continue
		}
		k, v, found := strings.Cut(pair, "=")
		if !found || len(v) < 2 || v[0] != '"' || v[len(v)-1] != '"' {
			return nil, fmt.Errorf("метка %q без значения в кавычках", pair)
		}
		labels[strings.TrimSpace(k)] = fixturesUnescapeProm(v[1 : len(v)-1])
	}
	return labels, nil
}

// fixturesSplitLabelPairs — splitting the label list by commas outside
// quotes (a comma inside quotes is part of the value).
func fixturesSplitLabelPairs(s string) []string {
	var parts []string
	var b strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		switch {
		case s[i] == '\\' && inQuote && i+1 < len(s):
			b.WriteByte(s[i])
			b.WriteByte(s[i+1])
			i++
		case s[i] == '"':
			inQuote = !inQuote
			b.WriteByte(s[i])
		case s[i] == ',' && !inQuote:
			parts = append(parts, b.String())
			b.Reset()
		default:
			b.WriteByte(s[i])
		}
	}
	if b.Len() > 0 {
		parts = append(parts, b.String())
	}
	return parts
}

// fixturesUnescapeProm — the text format's label value escapes: \\, \"
// and \n; an unknown escape is kept literally (a backslash + the
// character) — Windows paths in labels must not silently lose slashes.
func fixturesUnescapeProm(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+1 >= len(s) {
			b.WriteByte(s[i])
			continue
		}
		switch s[i+1] {
		case '\\':
			b.WriteByte('\\')
		case '"':
			b.WriteByte('"')
		case 'n':
			b.WriteByte('\n')
		default:
			b.WriteByte('\\')
			b.WriteByte(s[i+1])
		}
		i++
	}
	return b.String()
}

// fixturesSnapshotMetricsURL — a GET of /metrics and collection of the
// render stage's instruments (a 10 s timeout — a hung endpoint does not
// stall a snapshot that must be taken before the repeated import).
// Missing individual instruments (a build without them) — zeros, not an
// error: the report tells "no instrument" from "a run without events"
// by the stage's tasks (Tasks == 0 ⇒ the metrics of a foreign/empty
// worker — the verdicts over them degrade, review T-244 P2-1).
func fixturesSnapshotMetricsURL(ctx context.Context, url string) (*fixturesSnapshotMetrics, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("метрики %s: %w", url, err)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("метрики %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("метрики %s: статус %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, fmt.Errorf("метрики %s: чтение: %w", url, err)
	}
	samples, err := fixturesParsePromMetrics(body)
	if err != nil {
		return nil, fmt.Errorf("метрики %s: %w", url, err)
	}
	return fixturesMetricsFromSamples(samples), nil
}

// fixturesMetricsFromSamples — collecting the render stage's instruments
// from the /metrics samples: point counters by labels, the histogram by
// buckets.
func fixturesMetricsFromSamples(samples []fixturesPromSample) *fixturesSnapshotMetrics {
	m := &fixturesSnapshotMetrics{
		Errors:            map[string]float64{},
		TaskFinalized:     map[string]float64{},
		FallbackPublished: map[string]float64{},
	}
	// value — the first matching sample (a correct exposition has no
	// duplicate names; after a process restart with a shared registry
	// they do not arise either — the instruments are the same).
	one := func(name string, match func(labels map[string]string) bool) (float64, bool) {
		for _, s := range samples {
			if s.Name == name && match(s.Labels) {
				return s.Value, true
			}
		}
		return 0, false
	}
	all := func(name string, each func(labels map[string]string, v float64)) {
		for _, s := range samples {
			if s.Name == name {
				each(s.Labels, s.Value)
			}
		}
	}
	anyLabel := func(map[string]string) bool { return true }
	stageRender := func(l map[string]string) bool { return l["stage"] == "render" }

	if v, ok := one("process_start_time_seconds", anyLabel); ok {
		m.ProcessStartSeconds = v
	}
	if v, ok := one("crawler_stage_tasks_total", stageRender); ok {
		m.Tasks = v
	}
	if v, ok := one("crawler_stage_seconds_sum", stageRender); ok {
		m.SecondsSum = v
	}
	if v, ok := one("crawler_stage_seconds_count", stageRender); ok {
		m.SecondsCount = v
	}
	if v, ok := one("crawler_render_records_total", func(l map[string]string) bool { return l["rendered"] == "true" }); ok {
		m.RenderedRecords = v
	}
	if v, ok := one("crawler_render_tarpit_finalized_total", anyLabel); ok {
		m.TarpitFinalized = v
	}
	if v, ok := one("crawler_render_cert_finalized_total", anyLabel); ok {
		m.CertFinalized = v
	}
	if v, ok := one("crawler_render_dns_finalized_total", anyLabel); ok {
		m.DNSFinalized = v
	}
	all("crawler_stage_errors_total", func(l map[string]string, v float64) {
		if l["stage"] == "render" {
			m.Errors[l["type"]] += v
		}
	})
	all("crawler_render_task_finalized_total", func(l map[string]string, v float64) {
		if reason := l["reason"]; reason != "" {
			m.TaskFinalized[reason] += v
		}
	})
	all("crawler_render_fallback_published_total", func(l map[string]string, v float64) {
		if reason := l["reason"]; reason != "" {
			m.FallbackPublished[reason] += v
		}
	})
	for _, s := range samples {
		if s.Name != "crawler_stage_seconds_bucket" || !stageRender(s.Labels) {
			continue
		}
		le, err := strconv.ParseFloat(s.Labels["le"], 64)
		if err != nil {
			continue // a foreign/broken label — skip the bucket, the report will see an incomplete histogram
		}
		if math.IsInf(le, 1) {
			// +Inf is not encodable in JSON (the snapshot): the closing
			// bucket is stored as the MaxFloat64 sentinel — the cumulative
			// is preserved, quantile does not interpolate into it (see
			// fixturesHistQuantile).
			le = math.MaxFloat64
		}
		m.Buckets = append(m.Buckets, fixturesHistBucket{LE: le, Count: s.Value})
	}
	sort.Slice(m.Buckets, func(i, j int) bool { return m.Buckets[i].LE < m.Buckets[j].LE })
	return m
}

// ---------------------------------------------------------------- files

// fixturesWriteSnapshotFile — an atomic snapshot write (tmp + rename,
// the T-242 dump style): an interrupted write leaves no half-file.
func fixturesWriteSnapshotFile(path string, snap *fixturesSnapshot) error {
	raw, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("снапшот %s: кодирование: %w", path, err)
	}
	raw = append(raw, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("снапшот %s: %w", path, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("снапшот %s: переименование: %w", path, err)
	}
	return nil
}

// fixturesReadSnapshot — reading a snapshot (the report's input): the
// format and version are checked before the computations.
func fixturesReadSnapshot(path string) (*fixturesSnapshot, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("снапшот %s: %w", path, err)
	}
	var snap fixturesSnapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return nil, fmt.Errorf("снапшот %s: %w", path, err)
	}
	if snap.FormatVersion != fixturesSnapshotVersion {
		return nil, fmt.Errorf("снапшот %s: формат v%d не поддерживается (инструмент понимает v%d)",
			path, snap.FormatVersion, fixturesSnapshotVersion)
	}
	if len(snap.Rows) == 0 {
		return nil, fmt.Errorf("снапшот %s: без строк — пустой дамп или чужой файл", path)
	}
	return &snap, nil
}

// ---------------------------------------------------------------- command

// fixturesSnapshotCmd — crawlerctl fixtures snapshot: flag parsing, dump
// reading, connections (the DB always; Redis and /metrics — the
// snapshot's sources), assembly and file writing.
func fixturesSnapshotCmd(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("fixtures snapshot")
	dir := fs.String("dir", "", "каталог дампа фикстуры (ключи прогона; обязателен)")
	label := fs.String("label", "", "метка снапшота/прогона (обязательна; файл — <dir>/<label>.snapshot.json)")
	engine := fs.String("engine", "", "движок прогона: chromium|moli|lightpanda (обязателен)")
	metricsURL := fs.String("metrics", "", "URL /metrics render-worker стенда (пусто — без метрик этапа)")
	rssMax := fs.Int64("engine-rss-max", 0, "макс. RSS движка за прогон, байты (docker stats; 0 — не задан)")
	crashes := fs.Int64("engine-crashes", -1, "падения ядра движка за прогон (>= 0 — задано)")
	outPath := fs.String("out", "", "файл снапшота (по умолчанию <dir>/<label>.snapshot.json)")
	noDOM := fs.Bool("no-dom", false, "не читать DOM хранилища (без узлов/скриптов)")
	if handled, err := parseFlags(fs, fixturesHelp, args, stdout); handled {
		return err
	}
	if strings.TrimSpace(*dir) == "" {
		return usagef("укажите каталог дампа: crawlerctl fixtures snapshot --dir /path/to/fixtures")
	}
	if *label == "" || !fixturesLabelRe.MatchString(*label) {
		return usagef("укажите метку снапшота --label ИМЯ (буквы/цифры/._-, без разделителей пути)")
	}
	switch *engine {
	case config.EngineChromium, config.EngineMoli, config.EngineLightpanda:
	default:
		return usagef("fixtures snapshot: --engine: ожидается %s|%s|%s, задано %q",
			config.EngineChromium, config.EngineMoli, config.EngineLightpanda, *engine)
	}
	if *rssMax < 0 {
		return usagef("fixtures snapshot: --engine-rss-max: отрицательное значение %d", *rssMax)
	}
	if fs.NArg() > 0 {
		return usagef("fixtures snapshot: лишние аргументы %v (команда принимает только флаги)", fs.Args())
	}

	ctx, stop := signalCtx()
	defer stop()

	dump, err := fixturesReadDump(*dir)
	if err != nil {
		return fmt.Errorf("fixtures snapshot: %w", err)
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("fixtures snapshot: %w", err)
	}
	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2           // connection budget per spec 6.3 — same as the services
	poolOpts.AppName = "crawlerctl" // label in pg_stat_activity (T-177)
	pool, err := db.Open(ctx, poolOpts)
	if err != nil {
		return fmt.Errorf("fixtures snapshot: %w", err)
	}
	defer pool.Close()

	keys := make([]fixturesKey, 0, len(dump.Results))
	for _, r := range dump.Results {
		keys = append(keys, fixturesKey{r.ZoneID, r.DomainID, r.SubdomainID, r.Variant})
	}
	rows, err := fixturesSnapshotReadRows(ctx, pool, keys)
	if err != nil {
		return fmt.Errorf("fixtures snapshot: %w", err)
	}
	if len(rows) != len(keys) {
		// Dump rows missing from the DB (deleted between the export and
		// the run) would silently reduce the success denominator — a
		// warning instead of an unnoticed truncation (review T-244 P2-4).
		out(stdout, "снапшот: предупреждение: в БД %d строк из %d ключей дампа — знаменатель успешности усечён\n",
			len(rows), len(keys))
	}

	snap := &fixturesSnapshot{
		FormatVersion: fixturesSnapshotVersion,
		Label:         *label,
		Engine:        *engine,
		CreatedAt:     time.Now().UTC(),
		Dump: fixturesSnapshotDump{
			Seed:   dump.Manifest.Seed,
			Keys:   len(dump.Results),
			Strata: dump.Manifest.Strata,
		},
		Rows: rows,
	}
	if *rssMax > 0 {
		v := *rssMax
		snap.EngineRSSMax = &v
	}
	if *crashes >= 0 {
		v := *crashes
		snap.EngineCrashes = &v
	}

	// Structural DOM metrics: reading the store with the same pool
	// (PackStore ReadOnly — the crawlerctl domain --content style). An
	// unavailable store/content — a degradation (DOMParsed=false), not a
	// snapshot failure.
	if !*noDOM {
		logger := slog.New(slog.NewTextHandler(stderr, nil))
		if _, err := content.SetDictFile(cfg.Content.ZstdDictionary, logger); err != nil {
			out(stdout, "снапшот: хранилище контента недоступно (%v) — без узлов/скриптов DOM\n", err)
		} else if store, err := content.OpenPackStore(pool, content.PackOptions{Dir: cfg.Content.Dir, ReadOnly: true}); err != nil {
			out(stdout, "снапшот: хранилище контента недоступно (%v) — без узлов/скриптов DOM\n", err)
		} else {
			content.WarnDictMismatch(store.Dir(), logger)
			if skipped := fixturesSnapshotParseDOM(ctx, store, snap.Rows); skipped > 0 {
				out(stdout, "снапшот: DOM не прочитан у %d строк (контент выметен/отсутствует) — без узлов/скриптов\n", skipped)
			}
			_ = store.Close()
		}
	}

	// Redis streams: the q:analyze boundaries — the pace; unavailability
	// — a degradation.
	rdb := queue.NewClient(cfg.Redis.Addr)
	if streams, err := fixturesSnapshotTakeStreams(ctx, rdb); err != nil {
		out(stdout, "снапшот: Redis недоступен (%v) — без темпа и остатков очередей\n", err)
	} else {
		snap.Streams = streams
	}
	if err := rdb.Close(); err != nil {
		out(stdout, "снапшот: закрытие Redis: %v\n", err)
	}

	// Stage metrics: the URL is given explicitly — unavailability is an
	// operator error.
	if *metricsURL != "" {
		m, err := fixturesSnapshotMetricsURL(ctx, *metricsURL)
		if err != nil {
			return fmt.Errorf("fixtures snapshot: %w", err)
		}
		snap.Metrics = m
	}

	path := *outPath
	if path == "" {
		path = filepath.Join(*dir, *label+fixturesSnapshotFileSuffix)
	}
	if err := fixturesWriteSnapshotFile(path, snap); err != nil {
		return fmt.Errorf("fixtures snapshot: %w", err)
	}

	rendered := 0
	unanalyzed := 0
	for _, r := range snap.Rows {
		if r.Rendered && r.HasContent {
			rendered++
			if r.RulesVersion == nil {
				unanalyzed++ // the analyzer has not passed over the run's DOM
			}
		}
	}
	out(stdout, "снапшот %s: движок %s, задач %d, успешных рендеров %d (%.2f%%)\n",
		*label, *engine, len(snap.Rows), rendered, pct(rendered, len(snap.Rows)))
	if unanalyzed > 0 {
		out(stdout, "  предупреждение: %d успешных рендеров без штампа анализатора (rules_version NULL) — вердикты по ним от прошлого анализа, диф детекции по ним деградирует\n", unanalyzed)
	}
	if snap.Streams != nil {
		out(stdout, "  q:analyze %d записей, q:render остаток %d, q:render-fallback %d, q:dead %d\n",
			snap.Streams.AnalyzeLen, snap.Streams.RenderLen, snap.Streams.FallbackLen, snap.Streams.DeadLen)
	}
	if snap.Metrics != nil {
		out(stdout, "  метрики: задач этапа %.0f, причин финализаций %d, тарпит/cert/dns %.0f/%.0f/%.0f\n",
			snap.Metrics.Tasks, len(snap.Metrics.TaskFinalized),
			snap.Metrics.TarpitFinalized, snap.Metrics.CertFinalized, snap.Metrics.DNSFinalized)
	}
	out(stdout, "снапшот записан: %s\n", path)
	return nil
}

// pct — a share in percent (0 when dividing by zero).
func pct(part, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total) * 100
}

// Acceptance tests per the criteria of spec §11.4 (T-094): every item of
// the "Acceptance criteria" specification is a separate Test case against
// the live dev stack (PostgreSQL, Redis, unbound/coredns, testweb,
// mockserver, chromium). The suite gathers the criteria into an explicit
// acceptance map and does not duplicate the neighboring files: their
// scenarios are the source of the detail coverage; here, for every §11.4
// item, the wording (as a comment above the case) and the minimal
// scenario closing the gap are given:
//
//	item 1  TestAccept01 — add/delete + IDN zones without duplicates (details: ingest_test)
//	item 2  TestAccept02 — subdomains independently, variants/aliases §5.5 (http_test)
//	item 3  TestAccept03 — SPA: the DOM and cookies; HTML; dedup/compression; reading
//	                     back (content_test, the live render tests);
//	                     TestAccept03_RenderFallback — the scenario of the render
//	                     fallback queue (the render spec of stage 35 §4.4/§4.5, T-238):
//	                     the primary engine's tarpit → q:render-fallback without
//	                     a marker → the finishing by chromium worker B →
//	                     q:analyze
//	item 4  TestAccept04 — version/score analysis + reanalyze without a refetch
//	item 5  TestAccept05 — selections from laravel_domains/laravel_findings (e2e)
//	item 6  TestAccept06 — zone progress updates automatically + metrics
//	item 7  — a target server under a 24 h load: outside the dev bench, task
//	         T-093 (blocked until the production hardware is deployed)
//	item 8  TestAccept08 — the recheck interval (6 months by default,
//	                     changed by config)
//	item 9  TestAccept09 — a service stop: the lease returns "stuck"
//	                     domains (e2e — a PostgreSQL restart, resilience —
//	                     a Redis loss and a worker death)
//	item 10 TestAccept10 — a search by name in any writing form (schema_test —
//	                     the domain_info contract; crawlerctl — SQL over it)
//	item 11 TestAccept11 — real customer samples through the pipeline
//
// The level — as in e2e_test.go (T-091): library calls of the components
// of all stages without starting service processes; every stage runs the
// same code as the cmd/* builds. Isolation — an isolated schema
// (migratedSchema), Redis DB 15 (resilienceRedisEnv) with q:* cleanup,
// directories — t.TempDir.
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"crawler/internal/analyzer"
	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/dnsclient"
	"crawler/internal/dnsworker"
	"crawler/internal/httpfetch"
	"crawler/internal/httpworker"
	"crawler/internal/idn"
	"crawler/internal/ingest"
	"crawler/internal/metrics"
	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/queue"
	"crawler/internal/render"
	"crawler/internal/renderworker"
	"crawler/internal/rules"
	"crawler/internal/scheduler"
)

// The stream consumer groups are unique to the suite (the e2e-constants
// style): Redis DB 15 is cleaned up before/after, the names do not clash
// with the neighbors.
const (
	accDNSGroup     = "acc-dns"
	accHTTPGroup    = "acc-http"
	accAnalyzeGroup = "acc-an"
	accRenderGroup  = "acc-render"
	accZone         = "crawl.test"
	// accIDNZone/accIDNLabel — the punycode forms of сайт.проверка.test
	// (a testbed zone and domain, the "composite zones and IDN" scenario).
	accIDNZone    = "xn--80adjurfhd.test"
	accIDNZoneUni = "проверка.test"
	accIDNLabel   = "xn--80aswg"
	accIDNLabelU  = "сайт"
	// accScoreTol — the tolerance of the score cross-check with the
	// samples' meta.yaml (float4 in the DB + rounding the expectation to 3
	// digits; as in e2e/the T-075 corpus).
	accScoreTol = 0.001
	// accSamplesDir — the customer samples corpus relative to the test
	// directory (in tools the container mounts /testdata, not the
	// environment variable — a path from the directory, the rulesDir style
	// in e2e).
	accSamplesDir = "../../testdata/laravel-samples"
)

// accEnv — the pipeline of the acceptance stages on the live stack (like
// e2eEnv, without the PostgreSQL drop: resilience to a DB restart is e2e
// territory).
type accEnv struct {
	pool     *pgxpool.Pool
	rdb      *redis.Client
	q        *queue.RedisQueue
	store    *content.PackStore
	saver    *httpworker.ContentSaver
	recorder *httpworker.Recorder
	hworker  *httpworker.Worker
	analyzer *analyzer.Worker
	rules    *rules.Set
}

// newAccEnv assembles the environment: a schema with migrations, a queue,
// the content store and the DNS/HTTP/analyze stage components (the wiring
// as in the cmd/* builds). The render pipeline is connected pointwise
// (TestAccept03).
func newAccEnv(t *testing.T) *accEnv {
	t.Helper()
	pool := migratedSchema(t)
	rdb := resilienceRedisEnv(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})

	store, err := content.OpenPackStore(pool, content.PackOptions{
		Dir:           t.TempDir(),
		PackSize:      8 << 10,
		MaxStoreBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("OpenPackStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	saver, err := httpworker.NewContentSaver(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	recorder, err := httpworker.NewRecorder(pool, q, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// The transport — as in the cmd/http-worker build: redirect hosts are
	// resolved by the same resolver as the DNS stage (spec 5.5 item 2).
	resolve := dnsEnv(t)
	fetcher := httpTestFetcher(t, func(o *httpfetch.Options) {
		o.Resolver = httpfetch.NewCachingResolver(
			httpworker.NewDNSLookup(resolve, false), httpfetch.CacheOptions{})
	})
	hworker, err := httpworker.New(httpworker.ChainFetch(fetcher), httpworker.Options{
		MaxRedirects: httpfetch.DefaultMaxRedirects,
		Store:        store,
	})
	if err != nil {
		t.Fatal(err)
	}

	loader, err := rules.NewReloader(accRulesDir(t), nil)
	if err != nil {
		t.Fatalf("rules set: %v", err)
	}
	an, err := analyzer.New(pool, q, store, loader, analyzer.Options{
		MinScore: rules.DefaultMinScore,
	})
	if err != nil {
		t.Fatal(err)
	}

	return &accEnv{
		pool: pool, rdb: rdb, q: q, store: store,
		saver: saver, recorder: recorder,
		hworker: hworker, analyzer: an, rules: loader.Set(),
	}
}

// accResolver — a DNS client to the bench's live unbound (dnsEnv); cached
// for the duration of the test, the stage pipeline takes the resolver once.
func accResolver(t *testing.T) dnsclient.Resolver {
	t.Helper()
	return dnsEnv(t)
}

// accRulesDir — the working rules/: APP_RULES_DIR when the directory
// exists (the service containers), otherwise the path from the test
// directory to the repository root (in tools the variable is inherited
// but the directory itself is not mounted — the T-091 lesson).
func accRulesDir(t *testing.T) string {
	t.Helper()
	if dir := os.Getenv("APP_RULES_DIR"); dir != "" {
		if _, err := os.Stat(dir); err == nil {
			return dir
		}
	}
	if _, err := os.Stat("../../rules"); err != nil {
		t.Fatalf("rules set unavailable: %v", err)
	}
	return "../../rules"
}

// accModifiedRulesDir — a copy of the working set with a probe rule firing
// on exactly one pipeline sample (symfony-negative): the canonical hash
// (Version) changes, and reanalyze must reapply the rules — the target
// sample gains the probe hit and a higher score, the others keep their
// verdicts (TestAccept04).
const (
	// accProbeRule — the id of the probe rule in the set copy.
	accProbeRule = "t094_reanalyze_probe"
	// accProbeSample — the probe's target sample: a stable substring of
	// its HTML does not occur in the other samples or the corpus rules.
	accProbeSample = "symfony-negative"
	// accProbeWeight — the probe weight: noticeable in the score (noisy-OR)
	// but does not push the weak negative over the is_laravel threshold of
	// 0.6.
	accProbeWeight = 0.05
)

func accModifiedRulesDir(t *testing.T) string {
	t.Helper()
	src := accRulesDir(t)
	dst := t.TempDir()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatalf("copy rule %s: %v", e.Name(), err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), raw, 0o644); err != nil {
			t.Fatalf("copy rule %s: %v", e.Name(), err)
		}
	}
	probe := "rules:\n" +
		"  - id: " + accProbeRule + "\n" +
		"    description: T-094 acceptance probe — fires on the sample " + accProbeSample + ", proving the rules are reapplied after reanalyze\n" +
		"    target: html\n" +
		"    regex: 'Hello Symfony!'\n" +
		"    weight: " + fmt.Sprintf("%g", accProbeWeight) + "\n"
	if err := os.WriteFile(filepath.Join(dst, "t094_probe.yaml"), []byte(probe), 0o644); err != nil {
		t.Fatalf("probe rule: %v", err)
	}
	return dst
}

// ---------------------------------------------------------------- inserts

// accInsertZone — a zone (a unicode name for display, an empty string → NULL).
func accInsertZone(t *testing.T, pool *pgxpool.Pool, name, unicode string) int16 {
	t.Helper()
	var id int16
	err := pool.QueryRow(context.Background(),
		"INSERT INTO zones (name, name_unicode) VALUES ($1, NULLIF($2, '')) RETURNING id",
		name, unicode,
	).Scan(&id)
	if err != nil {
		t.Fatalf("insert zone %s: %v", name, err)
	}
	return id
}

// accInsertDomains — the zone's pending domains (the full ingest pipeline —
// TestAccept01), returns the ids in the labels order. The zone counters
// are maintained like apply_add (domains_total/domains_pending +n, T-178):
// the dispatcher filters zones by domains_pending > 0 (T-183) — inserting
// bypassing the counters would leave the zone invisible to DispatchBatch
// (accPipeline).
func accInsertDomains(t *testing.T, pool *pgxpool.Pool, zoneID int16, labels []string) []int64 {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		"INSERT INTO domains (zone_id, label) SELECT $1, unnest($2::text[]) RETURNING id",
		zoneID, labels,
	)
	if err != nil {
		t.Fatalf("insert zone %d domains: %v", zoneID, err)
	}
	defer rows.Close()
	ids := make([]int64, 0, len(labels))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("read domain id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("insert zone %d domains: %v", zoneID, err)
	}
	if len(ids) != len(labels) {
		t.Fatalf("domains inserted: %d, want %d", len(ids), len(labels))
	}
	if _, err := pool.Exec(context.Background(),
		"UPDATE zones SET domains_total = domains_total + $2, domains_pending = domains_pending + $2 WHERE id = $1",
		zoneID, len(labels),
	); err != nil {
		t.Fatalf("zone %d counters: %v", zoneID, err)
	}
	return ids
}

// ---------------------------------------------------------------- pipeline

// accPipeline — the zone's domains through the dispatcher and the DNS/HTTP
// stages (like the corresponding e2e stages): pending → queued + q:dns →
// DNS episodes (the subdomain mask, q:http) → HTTP episodes
// (subdomain_results, q:analyze). Returns a "label → id" map.
func accPipeline(ctx context.Context, t *testing.T, env *accEnv, zone string, labels []string) map[string]int64 {
	t.Helper()
	zoneID := accInsertZone(t, env.pool, zone, "")
	return accPipelineZone(ctx, t, env, zoneID, zone, labels)
}

// accPipelineZone — accPipeline for a pre-created zone (the caller
// inserted the unicode display name — the TestAccept10 search).
func accPipelineZone(ctx context.Context, t *testing.T, env *accEnv, zoneID int16, zone string, labels []string) map[string]int64 {
	t.Helper()
	ids := accInsertDomains(t, env.pool, zoneID, labels)

	batch, err := scheduler.DispatchBatch(ctx, env.pool, scheduler.DispatchQuery{Limit: len(labels)})
	if err != nil {
		t.Fatalf("dispatch batch: %v", err)
	}
	if len(batch) != len(labels) {
		t.Fatalf("dispatched domains: %d, want %d", len(batch), len(labels))
	}
	if _, err := scheduler.EnqueueBatch(ctx, env.pool, env.q, batch); err != nil {
		t.Fatalf("enqueue batch: %v", err)
	}

	// The DNS stage — as the worker's processBatch (T-039): XACK after the write.
	deliveries := consumeTasks(t, env.q, queue.StreamDNS, accDNSGroup, "acc", 5)
	if len(deliveries) != 1 {
		t.Fatalf("q:dns deliveries: %d, want 1 (one batch)", len(deliveries))
	}
	err = queue.Processing(ctx, env.q, accDNSGroup, deliveries[0],
		func(c context.Context, d queue.Delivery) error {
			b, err := queue.Decode[queue.DNSBatch](d)
			if err != nil {
				return err
			}
			if len(b.Domains) != len(labels) {
				return fmt.Errorf("batch has %d domains, want %d", len(b.Domains), len(labels))
			}
			subs, err := dnsworker.LoadSubdomains(c, env.pool)
			if err != nil {
				return err
			}
			pending, err := dnsworker.PendingDomains(c, env.pool, b.Domains)
			if err != nil {
				return err
			}
			if len(pending) != len(labels) {
				return fmt.Errorf("pending domains: %d, want %d", len(pending), len(labels))
			}
			resolve := accResolver(t)
			for _, dom := range pending {
				if err := dnsworker.ProcessDomain(c, env.pool, env.q, resolve, dom, subs, true, nil, nil); err != nil {
					return err
				}
			}
			return nil
		})
	if err != nil {
		t.Fatalf("q:dns batch: %v", err)
	}

	// The HTTP stage — as the worker's processDelivery (T-053): episode →
	// content → write → XACK.
	tasks := consumeTasks(t, env.q, queue.StreamHTTP, accHTTPGroup, "acc", len(labels))
	if len(tasks) != len(labels) {
		t.Fatalf("q:http tasks: %d, want %d", len(tasks), len(labels))
	}
	for _, d := range tasks {
		err := queue.Processing(ctx, env.q, accHTTPGroup, d,
			func(c context.Context, del queue.Delivery) error {
				task, err := queue.Decode[queue.HTTPTask](del)
				if err != nil {
					return err
				}
				res, err := env.hworker.ProcessDomain(c, task)
				if err != nil {
					return err
				}
				if err := env.saver.Save(c, res); err != nil {
					return err
				}
				recorded, err := env.recorder.RecordAndPublish(c, res)
				if err != nil {
					return err
				}
				if !recorded {
					return fmt.Errorf("domain %s: result not recorded (the write guard, T-058)", task.Name)
				}
				return nil
			})
		if err != nil {
			t.Fatalf("HTTP episode: %v", err)
		}
	}

	out := make(map[string]int64, len(labels))
	for i, l := range labels {
		out[l] = ids[i]
	}
	return out
}

// accAnalyze — the analyzer stage: n q:analyze deliveries through the
// worker (as the worker's processDelivery of T-071). The analyzer is
// passed explicitly: the TestAccept04 re-analysis runs a second rule set.
func accAnalyze(ctx context.Context, t *testing.T, env *accEnv, an *analyzer.Worker, n int) {
	t.Helper()
	msgs := consumeTasks(t, env.q, queue.StreamAnalyze, accAnalyzeGroup, "acc", n)
	if len(msgs) != n {
		t.Fatalf("q:analyze deliveries: %d, want %d", len(msgs), n)
	}
	for _, d := range msgs {
		err := queue.Processing(ctx, env.q, accAnalyzeGroup, d,
			func(c context.Context, del queue.Delivery) error {
				res, err := queue.Decode[queue.Result](del)
				if err != nil {
					return err
				}
				processed, err := an.ProcessResult(c, res)
				if err != nil {
					return err
				}
				if !processed {
					return fmt.Errorf("row %d/%d/%d/%d not found or overwritten",
						res.ZoneID, res.DomainID, res.SubdomainID, int16(res.Variant))
				}
				return nil
			})
		if err != nil {
			t.Fatalf("analyze: %v", err)
		}
	}
}

// ---------------------------------------------------------------- readers

// accResultRow — a subdomain_results row for the checks (the
// subdomain/variant order is deterministic).
type accResultRow struct {
	subdomainID int16
	variant     int16
	scheme      int16
	httpStatus  *int16
	finalURL    *string
	aliasKind   int16
	aliasSub    *int16
	rendered    bool
	contentHash []byte
	cookies     *string
	errCode     *int16
}

func accRows(t *testing.T, pool *pgxpool.Pool, domainID int64) []accResultRow {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT subdomain_id, variant, scheme, http_status, final_url, alias_kind,
		       alias_subdomain_id, rendered, content_hash, cookies::text, error
		FROM subdomain_results WHERE domain_id = $1
		ORDER BY subdomain_id, variant`, domainID)
	if err != nil {
		t.Fatalf("read subdomain_results: %v", err)
	}
	defer rows.Close()
	var out []accResultRow
	for rows.Next() {
		var r accResultRow
		if err := rows.Scan(&r.subdomainID, &r.variant, &r.scheme, &r.httpStatus, &r.finalURL,
			&r.aliasKind, &r.aliasSub, &r.rendered, &r.contentHash, &r.cookies, &r.errCode); err != nil {
			t.Fatalf("subdomain_results row: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("subdomain_results iteration: %v", err)
	}
	return out
}

// accFinding — a laravel_findings row.
type accFinding struct {
	sub          int16
	variant      int16
	score        float32
	verMin       *string
	verMax       *string
	isLaravel    bool
	signals      string
	rulesVersion int32
}

func accFindings(t *testing.T, pool *pgxpool.Pool, zoneID int16, domainID int64) []accFinding {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT subdomain_id, variant, score, ver_min, ver_max, is_laravel, signals::text, rules_version
		FROM laravel_findings WHERE zone_id = $1 AND domain_id = $2`, zoneID, domainID)
	if err != nil {
		t.Fatalf("read laravel_findings: %v", err)
	}
	defer rows.Close()
	var out []accFinding
	for rows.Next() {
		var f accFinding
		if err := rows.Scan(&f.sub, &f.variant, &f.score, &f.verMin, &f.verMax,
			&f.isLaravel, &f.signals, &f.rulesVersion); err != nil {
			t.Fatalf("laravel_findings row: %v", err)
		}
		out = append(out, f)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("laravel_findings iteration: %v", err)
	}
	return out
}

// accAggregate — a domain's laravel_domains row.
type accAggregate struct {
	score  float32
	verMin *string
	verMax *string
	mask   int64
	rulesV int32
}

func accLaravelDomain(t *testing.T, pool *pgxpool.Pool, zoneID int16, domainID int64) (accAggregate, bool) {
	t.Helper()
	var a accAggregate
	err := pool.QueryRow(context.Background(), `
		SELECT score, ver_min, ver_max, found_on_mask, rules_version
		FROM laravel_domains WHERE zone_id = $1 AND domain_id = $2`, zoneID, domainID,
	).Scan(&a.score, &a.verMin, &a.verMax, &a.mask, &a.rulesV)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, false
	}
	if err != nil {
		t.Fatalf("read laravel_domains: %v", err)
	}
	return a, true
}

// accStr — a string or NULL → "".
func accStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// accHash — a content.Hash from a slice read from the DB.
func accHash(b []byte) content.Hash {
	var h content.Hash
	copy(h[:], b)
	return h
}

// accSignal — a signals jsonb row (T-071).
type accSignal struct {
	Rule string `json:"rule"`
}

// accFiredRules — the set of fired rule ids from the signals jsonb.
func accFiredRules(t *testing.T, signals string) map[string]bool {
	t.Helper()
	var sigs []accSignal
	if err := json.Unmarshal([]byte(signals), &sigs); err != nil {
		t.Fatalf("signals jsonb: %v", err)
	}
	m := make(map[string]bool, len(sigs))
	for _, s := range sigs {
		m[s.Rule] = true
	}
	return m
}

// accWaitFor — poll until ready or the deadline (the automatic updates of
// TestAccept06).
func accWaitFor(t *testing.T, what string, deadline time.Duration, ready func() bool) {
	t.Helper()
	end := time.Now().Add(deadline)
	for !ready() {
		if time.Now().After(end) {
			t.Fatalf("%s: did not happen within %s", what, deadline)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// accCounterSum — the sum of the counter series of the name metric from
// the g registry (a metric with labels — all series; label filters as
// "name, value" pairs restrict the sum to one series). The T-245 fallback
// balance check reads the published/consumed counters by the same Gather
// Prometheus scrapes.
func accCounterSum(t *testing.T, g prometheus.Gatherer, name string, label ...string) float64 {
	t.Helper()
	if len(label)%2 != 0 {
		t.Fatalf("accCounterSum: label filters as \"name, value\" pairs, got %d", len(label))
	}
	filter := map[string]string{}
	for i := 0; i < len(label); i += 2 {
		filter[label[i]] = label[i+1]
	}
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var sum float64
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
	series:
		for _, m := range mf.GetMetric() {
			for lv, want := range filter {
				found := false
				for _, l := range m.GetLabel() {
					if l.GetName() == lv && l.GetValue() == want {
						found = true
						break
					}
				}
				if !found {
					continue series
				}
			}
			sum += m.GetCounter().GetValue()
		}
	}
	return sum
}

// ---------------------------------------------------------------- item 1

// TestAccept01_IngestAddDeleteIDN — §11.4 item 1: "add/delete files from
// the watched folder apply correctly, including for IDN zones and domains
// (no duplicate zones in unicode/punycode forms)". The full criterion in
// one scenario through the real ingest pipeline: an add file in the
// unicode directory of the IDN zone, a second add in the punycode
// directory of the same zone (no zone duplicate), a delete file and a
// revival by an add file with the same list, an undo of the first add
// file by the same list from delete/ (the dedup contract of T-113 — a
// zone+sha256 match with a different action is not a duplicate; details —
// ingest_test).
func TestAccept01_IngestAddDeleteIDN(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	// The scenario's first add file: its list is undone at the end by the
	// same list from delete/ (undo, T-113). The content is shared as a
	// variable, not a literal copy — "the same list" is byte-exact by
	// construction.
	firstAdd := fileContent(
		"# acceptance §11.4 item 1",
		accIDNLabelU+"."+accIDNZoneUni, // сайт.проверка.test
		"магазин."+accIDNZoneUni,       // a second IDN domain
	)

	t.Run("add in the IDN zone's unicode directory", func(t *testing.T) {
		out := env.process(t, filepath.Join(accIDNZoneUni, "add", "unicode-form.txt"), firstAdd)
		if out.Kind != ingest.OutcomeApplied {
			t.Fatalf("file outcome = %q, want applied", out.Kind)
		}
		if out.Report.Added != 2 || out.Report.Existed != 0 {
			t.Errorf("counters: added=%d existed=%d, want 2/0", out.Report.Added, out.Report.Existed)
		}

		// The zone — one, in punycode, with a unicode name for display.
		z, ok := env.zone(t, accIDNZone)
		if !ok {
			t.Fatalf("zone %s not created", accIDNZone)
		}
		if z.unicode != accIDNZoneUni || !z.isActive {
			t.Errorf("zone: unicode=%q active=%v, want %q/true", z.unicode, z.isActive, accIDNZoneUni)
		}

		// The domains — punycode labels with the is_idn bit (spec 5.2 item 9).
		for _, label := range []string{"xn--80aswg", "xn--80aairftm"} {
			d, ok := env.domain(t, accIDNZone, label)
			if !ok {
				t.Fatalf("domain %s.%s not created", label, accIDNZone)
			}
			if d.status != int16(model.StatusNew) || d.checkState != int16(model.CheckStatePending) {
				t.Errorf("%s: status/check_state = %d/%d, want new/pending", label, d.status, d.checkState)
			}
			if d.flags&int16(model.FlagIDN) == 0 {
				t.Errorf("%s: the is_idn bit not set", label)
			}
		}
		if !existsOnDisk(t, filepath.Join(env.dir, "processed", accIDNZoneUni, "add", "unicode-form.txt")) {
			t.Error("the file not moved into processed/")
		}
	})

	t.Run("the same zone's punycode directory — no duplicate", func(t *testing.T) {
		out := env.process(t, filepath.Join(accIDNZone, "add", "punycode-form.txt"), fileContent(
			"XN--80ASWG.XN--80ADJURFHD.TEST.", // punycode, uppercase, a dot — the same domain
			accIDNLabel+"."+accIDNZone,        // the canonical punycode form
			"новый."+accIDNZoneUni,            // a new IDN domain
		))
		if out.Kind != ingest.OutcomeApplied {
			t.Fatalf("file outcome = %q, want applied", out.Kind)
		}
		if out.Report.Added != 1 || out.Report.Existed != 1 {
			t.Errorf("counters: added=%d existed=%d, want 1/1 (new + existing)", out.Report.Added, out.Report.Existed)
		}

		var zones int
		if err := env.pool.QueryRow(context.Background(),
			"SELECT count(*) FROM zones WHERE name = $1", accIDNZone).Scan(&zones); err != nil {
			t.Fatal(err)
		}
		if zones != 1 {
			t.Errorf("zone %s rows: %d, want 1 (unicode/punycode — one zone)", accIDNZone, zones)
		}
		var total int
		if err := env.pool.QueryRow(context.Background(), "SELECT count(*) FROM domains").Scan(&total); err != nil {
			t.Fatal(err)
		}
		if total != 3 {
			t.Errorf("domains in the DB: %d, want 3", total)
		}
	})

	t.Run("delete and a revival by an add file with the same list", func(t *testing.T) {
		gone := fileContent(
			accIDNLabelU + "." + accIDNZoneUni, // сайт.проверка.test
		)
		out := env.process(t, filepath.Join(accIDNZoneUni, "delete", "gone.txt"), gone)
		if out.Kind != ingest.OutcomeApplied {
			t.Fatalf("delete outcome = %q, want applied", out.Kind)
		}
		if out.Report.Deleted != 1 {
			t.Errorf("deleted = %d, want 1", out.Report.Deleted)
		}
		d, ok := env.domain(t, accIDNZone, accIDNLabel)
		if !ok {
			t.Fatal("a deleted domain must stay in the DB (§13.1)")
		}
		if d.status != int16(model.StatusDeleted) {
			t.Errorf("status after delete = %d, want deleted", d.status)
		}

		// The revival with the same list byte-for-byte (the same sha256 as
		// the delete file): the action is different — there is no exact
		// redelivery, the file is applied (the (zone_id, action, sha256)
		// dedup, T-113). Before T-113 the global key on a single sha256
		// forced a revive file to have unique content (63da59c) — the
		// pinning is removed, the same content with a different action is
		// legitimate.
		revive := env.process(t, filepath.Join(accIDNZoneUni, "add", "revive.txt"), gone)
		if revive.Kind != ingest.OutcomeApplied {
			t.Fatalf("revival outcome = %q, want applied (T-113: a sha256 duplicate of the delete file is not a delivery duplicate)", revive.Kind)
		}
		if revive.Report.Revived != 1 || revive.Report.Added != 1 {
			t.Errorf("revival: added=%d revived=%d, want 1/1 (Added includes Revived — the T-021 contract)", revive.Report.Added, revive.Report.Revived)
		}
		d, _ = env.domain(t, accIDNZone, accIDNLabel)
		if d.status != int16(model.StatusNew) || d.checkState != int16(model.CheckStatePending) {
			t.Errorf("after the revival: status/check_state = %d/%d, want new/pending", d.status, d.checkState)
		}
		if d.queuedAt != nil {
			t.Errorf("after the revival queued_at = %v, want NULL (back in the check cycle)", *d.queuedAt)
		}
	})

	t.Run("undo by the same list from delete/ — cancels the first add file", func(t *testing.T) {
		// The same list byte-for-byte as the scenario's first add file
		// (unicode-form.txt): the action is different — there is no exact
		// redelivery, the file IS PROCESSED and deletes the list's domains
		// (the T-113 contract; the global dedup on a single sha256 would
		// have skip-zeroed it as a duplicate — the pre-T-113 semantics).
		undo := env.process(t, filepath.Join(accIDNZoneUni, "delete", "undo.txt"), firstAdd)
		if undo.Kind != ingest.OutcomeApplied {
			t.Fatalf("the same list from delete/: outcome = %q, want applied (T-113: a different action is not a duplicate)", undo.Kind)
		}
		if undo.Report.Deleted != 2 {
			t.Errorf("undo: deleted=%d, want 2 (both domains of the first add file)", undo.Report.Deleted)
		}
		for _, label := range []string{accIDNLabel, "xn--80aairftm"} {
			if d, ok := env.domain(t, accIDNZone, label); !ok || d.status != int16(model.StatusDeleted) {
				t.Errorf("after undo: domain %s = %+v, want deleted (stayed in the DB, §13.1)", label, d)
			}
		}

		// The journal: every delivery is its own row; the two contents of
		// the scenario were delivered twice with different actions
		// (gone/revive, unicode-form/undo) plus a single punycode file —
		// 5 rows, 3 contents: the unique thing is the triple
		// (zone_id, action, sha256), not the sha256 (T-113).
		var total, contents int
		if err := env.pool.QueryRow(context.Background(),
			"SELECT count(*), count(DISTINCT sha256) FROM ingest_files").Scan(&total, &contents); err != nil {
			t.Fatal(err)
		}
		if total != 5 || contents != 3 {
			t.Errorf("ingest_files: %d rows, %d contents, want 5/3 (a row per delivery, not per content; no exact delivery duplicates in the scenario)", total, contents)
		}
	})
}

// ---------------------------------------------------------------- item 2

// TestAccept02_SubdomainsVariantsAliases — §11.4 item 2: "The check of
// each subdomain is performed independently; the results follow the
// variant and alias rules (section 5.5)". Four bench scenarios through the
// real DNS and HTTP stages: same-site (the apex and www are checked each
// on its own), different-sites (two protocol variants), equal-pair (the
// "identical" alias), www-to-bare (the "redirect" alias).
func TestAccept02_SubdomainsVariantsAliases(t *testing.T) {
	skipUnlessIntegration(t)
	env := newAccEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	labels := []string{"same-site", "different-sites", "equal-pair", "www-to-bare"}
	ids := accPipeline(ctx, t, env, accZone, labels)

	// The rows in total: same-site 2 (apex https + www http),
	// different-sites 2 (variants 0/1), equal-pair 2, www-to-bare 2 — one
	// row per check (§5.5 item 7); as many q:analyze messages.
	var totalRows int
	for _, l := range labels {
		n := len(accRows(t, env.pool, ids[l]))
		t.Logf("domain %s: %d rows", l, n)
		totalRows += n
	}
	if totalRows != 8 {
		t.Fatalf("subdomain_results rows across 4 domains: %d, want 8", totalRows)
	}
	if n := streamLen(t, env.rdb, queue.StreamAnalyze); n != int64(totalRows) {
		t.Errorf("q:analyze messages = %d, want %d (one per check)", n, totalRows)
	}

	t.Run("each subdomain has its own check (same-site)", func(t *testing.T) {
		rows := accRows(t, env.pool, ids["same-site"])
		if len(rows) != 2 {
			t.Fatalf("rows: %d, want 2 (apex and www independently)", len(rows))
		}
		apex, www := rows[0], rows[1]
		if apex.subdomainID != 0 || www.subdomainID != 1 {
			t.Fatalf("order: subdomain_id = %d, %d, want 0, 1", apex.subdomainID, www.subdomainID)
		}
		// The apex: the protocols converge on one site — a single variant 0
		// row, canonical https (spec 5.5 item 5).
		if apex.variant != int16(model.VariantMain) || apex.scheme != int16(model.SchemeHTTPS) {
			t.Errorf("apex: variant/scheme = %d/%d, want 0/https", apex.variant, apex.scheme)
		}
		if accStr(apex.finalURL) != "https://same-site.crawl.test/" || apex.aliasKind != int16(model.AliasNone) {
			t.Errorf("apex: url=%q alias=%d, want https://same-site…/ without an alias", accStr(apex.finalURL), apex.aliasKind)
		}
		// www is not configured in Caddy: https is rejected by SNI, http
		// answers — a row of its own check's scheme, not a "copy" of the
		// apex.
		if www.variant != int16(model.VariantMain) || www.scheme != int16(model.SchemeHTTP) {
			t.Errorf("www: variant/scheme = %d/%d, want 0/http (an independent check)", www.variant, www.scheme)
		}
		if accStr(www.finalURL) != "http://www.same-site.crawl.test/" {
			t.Errorf("www: final URL = %q", accStr(www.finalURL))
		}
		// The DNS mask carries both subdomains (§5.4 item 4).
		var mask int64
		if err := env.pool.QueryRow(ctx,
			"SELECT dns_mask FROM domains WHERE id = $1", ids["same-site"],
		).Scan(&mask); err != nil {
			t.Fatal(err)
		}
		if mask != 0b11 {
			t.Errorf("dns_mask = %b, want bits 0 and 1 (apex and www alive)", mask)
		}
	})

	t.Run("protocol variants (different-sites, §5.5 item 5)", func(t *testing.T) {
		rows := accRows(t, env.pool, ids["different-sites"])
		if len(rows) != 2 {
			t.Fatalf("rows: %d, want 2 (http and https — different sites)", len(rows))
		}
		if rows[0].subdomainID != 0 || rows[1].subdomainID != 0 {
			t.Fatalf("rows are not the apex: %d, %d", rows[0].subdomainID, rows[1].subdomainID)
		}
		main, alt := rows[0], rows[1]
		if main.variant != int16(model.VariantMain) || main.scheme != int16(model.SchemeHTTPS) {
			t.Errorf("variant 0: scheme = %d, want https (canonical)", main.scheme)
		}
		if alt.variant != int16(model.VariantHTTP) || alt.scheme != int16(model.SchemeHTTP) {
			t.Errorf("variant 1: scheme = %d, want http", alt.scheme)
		}
		if accStr(main.finalURL) == accStr(alt.finalURL) {
			t.Errorf("the final URLs match: %q — the sites must be different", accStr(main.finalURL))
		}
		if got := accStr(alt.finalURL); got != "http://different-sites-alt.crawl.test/" {
			t.Errorf("the http variant's final URL = %q, want http://different-sites-alt.crawl.test/ (a redirect to the satellite host)", got)
		}
		for i, r := range rows {
			if r.aliasKind != int16(model.AliasNone) || r.aliasSub != nil {
				t.Errorf("row %d: alias = %d/%v, want none", i, r.aliasKind, r.aliasSub)
			}
		}
	})

	t.Run("the \"identical\" alias (equal-pair, §5.5 item 6)", func(t *testing.T) {
		rows := accRows(t, env.pool, ids["equal-pair"])
		if len(rows) != 2 {
			t.Fatalf("rows: %d, want 2", len(rows))
		}
		apex, www := rows[0], rows[1]
		if apex.subdomainID != 0 || www.subdomainID != 1 {
			t.Fatalf("order: %d, %d, want apex, www", apex.subdomainID, www.subdomainID)
		}
		if apex.aliasKind != int16(model.AliasNone) {
			t.Errorf("apex (the original): alias_kind = %d, want 0", apex.aliasKind)
		}
		if len(apex.contentHash) != 16 {
			t.Errorf("apex: content not stored (content_hash = %d bytes)", len(apex.contentHash))
		}
		if www.aliasKind != int16(model.AliasIdentical) {
			t.Errorf("www: alias_kind = %d, want 1 (identical)", www.aliasKind)
		}
		if www.aliasSub == nil || *www.aliasSub != 0 {
			t.Errorf("www: alias_subdomain_id = %v, want 0 (the original is the apex)", www.aliasSub)
		}
		if www.contentHash != nil {
			t.Error("the www alias stores its own content — there must be one content (the original's)")
		}
	})

	t.Run("the \"redirect\" alias (www-to-bare, §5.5 item 6)", func(t *testing.T) {
		rows := accRows(t, env.pool, ids["www-to-bare"])
		if len(rows) != 2 {
			t.Fatalf("rows: %d, want 2", len(rows))
		}
		apex, www := rows[0], rows[1]
		if apex.aliasKind != int16(model.AliasNone) || len(apex.contentHash) != 16 {
			t.Errorf("apex: alias=%d content=%d bytes, want the original with content", apex.aliasKind, len(apex.contentHash))
		}
		if www.aliasKind != int16(model.AliasRedirect) {
			t.Errorf("www: alias_kind = %d, want 2 (a redirect to the original)", www.aliasKind)
		}
		if www.aliasSub == nil || *www.aliasSub != 0 {
			t.Errorf("www: alias_subdomain_id = %v, want 0 (the apex)", www.aliasSub)
		}
		// The alias row converges on the original's address (spec 5.5 item 6).
		if want := "https://www-to-bare.crawl.test/"; accStr(www.finalURL) != want {
			t.Errorf("www: final URL = %q, want %q", accStr(www.finalURL), want)
		}
		if www.contentHash != nil {
			t.Error("the www alias stores its own content")
		}
	})

	// The episodes' outcome — alive/done (§7).
	for _, l := range labels {
		st := httpDomainState(t, env.pool, ids[l])
		if st.status != model.StatusAlive || st.checkState != int16(model.CheckStateDone) {
			t.Errorf("%s: status/check_state = %s/%d, want alive/done", l, st.status, st.checkState)
		}
	}
}

func accZoneID(t *testing.T, pool *pgxpool.Pool, zone string) int16 {
	t.Helper()
	var id int16
	if err := pool.QueryRow(context.Background(),
		"SELECT id FROM zones WHERE name = $1", zone).Scan(&id); err != nil {
		t.Fatalf("zone %s: %v", zone, err)
	}
	return id
}

// ---------------------------------------------------------------- item 3

// accHTTPRenderer — a render adapter for the testbed: navigation over http
// instead of https. Caddy's internal CA is not trusted by the dev stack's
// Chromium (production renders sites with public certificates), so the
// renderworker pipeline is real in full (reading the target → render →
// Put the DOM → replacing the reference → publishing q:analyze); only the
// navigation scheme is substituted — the same page, the same JS (the live
// render tests go the same way, internal/render/*_live_test.go).
type accHTTPRenderer struct {
	inner renderworker.Renderer
}

func (r accHTTPRenderer) Run(ctx context.Context, task render.RenderTask) (render.RenderOutput, error) {
	if u, err := url.Parse(task.URL); err == nil && u.Scheme == "https" {
		u.Scheme = "http"
		task.URL = u.String()
	}
	return r.inner.Run(ctx, task)
}

// accFailRenderer — a renderer that always returns the given error: the
// primary engine failure scenario in the acceptance fallback scenario
// (a tarpit on every delivery — TestAccept03_RenderFallback).
type accFailRenderer struct{ err error }

func (r accFailRenderer) Run(context.Context, render.RenderTask) (render.RenderOutput, error) {
	return render.RenderOutput{}, r.err
}

// accChromiumWS — the CDP address of the external Chromium (like the live
// render tests: CRAWLER_RENDER_REMOTE_URL → CHROMIUM_WS_URL → the stack
// address).
func accChromiumWS() string {
	for _, key := range []string{"CRAWLER_RENDER_REMOTE_URL", "CHROMIUM_WS_URL"} {
		if u := os.Getenv(key); u != "" {
			return u
		}
	}
	return "ws://172.28.100.14:9222"
}

// accMonitorMetricsURL — the /metrics address of the real monitor service:
// the compose service name via docker DNS (Prometheus of the dev bench
// scrapes it, deploy/docker/prometheus/prometheus.yml), overridden by the
// environment.
func accMonitorMetricsURL() string {
	if u := os.Getenv("CRAWLER_MONITOR_METRICS_URL"); u != "" {
		return u
	}
	return "http://monitor:9100/metrics"
}

// TestAccept03_SPADomCookiesContent — §11.4 item 3: "For SPA pages the
// final DOM and cookies are stored; for the others — HTML; deduplication
// and compression work; the content can be read back". The bench's SPA
// scenario goes through the HTTP stage with the render pipeline (q:render)
// and a render-worker on a live Chromium; the parked pages check
// deduplication, the stored blocks — compression (len < raw_len), reading
// — a roundtrip.
func TestAccept03_SPADomCookiesContent(t *testing.T) {
	skipUnlessIntegration(t)
	env := newAccEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// The write render pipeline (T-066): a recorder with a render queue.
	gate := queue.NewBackpressureGate(env.rdb, queue.BackpressureOptions{})
	renderQ, err := httpworker.NewRenderQueue(env.q, gate, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := httpworker.NewRenderRecorder(env.pool, env.q, renderQ, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	env.recorder = rec

	ids := accPipeline(ctx, t, env, accZone, []string{"spa-shell", "parked-1", "parked-2"})

	t.Run("non-SPA: HTML stored, dedup and compression", func(t *testing.T) {
		p1 := accRows(t, env.pool, ids["parked-1"])
		p2 := accRows(t, env.pool, ids["parked-2"])
		if len(p1) != 1 || len(p2) != 1 {
			t.Fatalf("parked page rows: %d/%d, want 1/1", len(p1), len(p2))
		}
		h1, h2 := accHash(p1[0].contentHash), accHash(p2[0].contentHash)
		if h1 != h2 {
			t.Fatal("identical parked pages produced different content_hash — deduplication broken")
		}
		// One reference from each row — refcount = 2 on the shared block.
		row, ok := contentRowByHash(t, env.pool, h1)
		if !ok {
			t.Fatal("the parked pages' content not stored in contents")
		}
		if row.refc != 2 {
			t.Errorf("the shared block refcount = %d, want 2 (two rows, one block)", row.refc)
		}
		// Compression: the block in the pack is smaller than the original bytes (zstd, T-041).
		if row.clen >= row.rawLen {
			t.Errorf("compression broken: len=%d >= raw_len=%d", row.clen, row.rawLen)
		}
		// The content is read back (a roundtrip).
		body, err := env.store.Get(ctx, h1)
		if err != nil {
			t.Fatalf("content read: %v", err)
		}
		if !strings.Contains(string(body), "Future home of something quite cool") {
			t.Errorf("the content does not look like a parked page: %.80q", body)
		}
		if p1[0].rendered {
			t.Error("the non-SPA row must stay rendered = false (HTML, not DOM)")
		}
	})

	t.Run("SPA: the HTTP stage publishes q:render", func(t *testing.T) {
		rows := accRows(t, env.pool, ids["spa-shell"])
		if len(rows) != 1 {
			t.Fatalf("spa-shell rows: %d, want 1", len(rows))
		}
		if rows[0].rendered {
			t.Error("before the render rendered = false (raw HTML)")
		}
		if n := streamLen(t, env.rdb, queue.StreamRender); n != 1 {
			t.Fatalf("q:render messages = %d, want 1 (the SPA verdict T-057)", n)
		}
		// The render_pending flag is not set: the queue had capacity.
		var flags int16
		if err := env.pool.QueryRow(ctx,
			"SELECT flags FROM domains WHERE id = $1", ids["spa-shell"],
		).Scan(&flags); err != nil {
			t.Fatal(err)
		}
		if flags&int16(model.FlagRenderPending) != 0 {
			t.Error("the render_pending bit set despite a free render queue")
		}
	})

	t.Run("render-worker: the final DOM and cookies", func(t *testing.T) {
		before := accRows(t, env.pool, ids["spa-shell"])[0]
		rawHash := accHash(before.contentHash)
		analyzeBefore := streamLen(t, env.rdb, queue.StreamAnalyze)

		browser, err := render.StartBrowser(context.Background(), render.Options{
			RemoteURL:         accChromiumWS(),
			Contexts:          1,
			Timeout:           30 * time.Second,
			RestartEveryPages: 1000,
		})
		if err != nil {
			t.Fatalf("Chromium connection: %v", err)
		}
		t.Cleanup(browser.Close)
		pool := render.NewPool(browser)
		t.Cleanup(pool.Close)
		rworker, err := renderworker.New(env.pool, env.q, env.store, accHTTPRenderer{pool}, renderworker.Options{})
		if err != nil {
			t.Fatal(err)
		}

		deliveries := consumeTasks(t, env.q, queue.StreamRender, accRenderGroup, "acc", 1)
		if len(deliveries) != 1 {
			t.Fatalf("q:render deliveries: %d, want 1", len(deliveries))
		}
		err = queue.Processing(ctx, env.q, accRenderGroup, deliveries[0],
			func(c context.Context, d queue.Delivery) error {
				res, err := queue.Decode[queue.Result](d)
				if err != nil {
					return err
				}
				processed, err := rworker.ProcessResult(c, res)
				if err != nil {
					return err
				}
				if !processed {
					return errors.New("the render not recorded (the row moved out from under the task)")
				}
				return nil
			})
		if err != nil {
			t.Fatalf("spa-shell render: %v", err)
		}

		after := accRows(t, env.pool, ids["spa-shell"])[0]
		if !after.rendered {
			t.Error("rendered not set — the DOM did not replace the HTML")
		}
		newHash := accHash(after.contentHash)
		if newHash == rawHash {
			t.Error("content_hash unchanged — raw HTML stored instead of the DOM")
		}
		// The final DOM: the content after JS, absent from the raw HTML.
		dom, err := env.store.Get(ctx, newHash)
		if err != nil {
			t.Fatalf("DOM read from the store: %v", err)
		}
		if !strings.Contains(string(dom), "SPA-контент после рендера") {
			t.Errorf("the DOM lacks the JS-drawn content: %.120q", dom)
		}
		// Cookies: JS set a cookie — CDP captures the whole storage (spec 5.6).
		if after.cookies == nil || !strings.Contains(*after.cookies, `"js_session"`) {
			t.Errorf("render cookies without js_session: %v", after.cookies)
		}
		// The raw HTML lost its reference: the block is dead (T-044), Get does not serve it.
		if _, err := env.store.Get(ctx, rawHash); !errors.Is(err, content.ErrNotFound) {
			t.Errorf("reading the old HTML after the render: %v, want ErrNotFound (the reference removed)", err)
		}
		// Publishing to q:analyze after the content replacement (§7).
		if n := streamLen(t, env.rdb, queue.StreamAnalyze); n != analyzeBefore+1 {
			t.Errorf("q:analyze after the render: %d, want %d (+1)", n, analyzeBefore+1)
		}
	})
}

// TestAccept03_RenderFallback — the scenario of the render fallback queue
// (the render spec of stage 35, §4.4/§4.5, FR-7/8/10; the fallback
// acceptance map): an engine-dependent failure of the primary engine (the
// T-148 tarpit on exhausting render.timeout_attempts) does not finalize
// the row with a marker but publishes it to q:render-fallback — the row
// stays rendered=false without render_failed_at and without a q:analyze
// publication (the primary worker); chromium worker B — the same binary
// built differently (the q:render-fallback stream, a separate consumer
// group, FR-12) — finishes the row on a live Chromium: rendered=true with
// a content_hash change, a q:analyze publication (Fresh). A recursive
// fallback of the fallback is forbidden (FR-10) — worker B publishes only
// q:analyze. The primary worker is assembled with the candidate engine
// (engine=moli, S-P1-1: publications go only from the candidate engine —
// chromium does not publish the fallback; the engine here is only a
// field, the render is decided by the fake). Engine-independent causes
// (DNS T-207/cert T-159) get no fallback (FR-7) — the cause filter is
// covered by the unit tests (TestRenderWorker_DNSCertNoFallback,
// internal/renderworker).
func TestAccept03_RenderFallback(t *testing.T) {
	skipUnlessIntegration(t)
	env := newAccEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// The write render pipeline (as TestAccept03): the SPA row is published
	// to q:render by the live HTTP phase.
	gate := queue.NewBackpressureGate(env.rdb, queue.BackpressureOptions{})
	renderQ, err := httpworker.NewRenderQueue(env.q, gate, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := httpworker.NewRenderRecorder(env.pool, env.q, renderQ, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	env.recorder = rec
	ids := accPipeline(ctx, t, env, accZone, []string{"spa-shell"})

	analyzeBefore := streamLen(t, env.rdb, queue.StreamAnalyze)
	before := accRows(t, env.pool, ids["spa-shell"])[0]
	rawHash := accHash(before.contentHash)

	// The primary worker (worker A): the primary engine "hangs" — a tarpit
	// on every delivery; the fallback is enabled (render.fallback.enabled,
	// T-238). The claim is shortened (MinIdle 300 ms): the first delivery
	// returns the tarpit below the limit (attempts=1 < 2), the claim
	// returns with attempts=2 — a publication to q:render-fallback instead
	// of a marker.
	tarpit := &render.Error{Class: render.ClassTask, URL: "https://spa.example/",
		Err: fmt.Errorf("page timeout %s: %w: %w",
			25*time.Second, context.DeadlineExceeded, errors.New("navigation timeout"))}
	// The instrument registries of both builds (T-245): published is
	// counted by worker A, consumed — by worker B; the balance check is
	// the test's last block (step 4 of the spec §6.2 acceptance).
	regA := metrics.New("acc-render-fb-a")
	regB := metrics.New("acc-render-fb-b")
	optsA := renderworker.Options{
		Stream:          queue.StreamRender,
		Group:           "acc-render-fb",
		Consumer:        "acc-fb-a",
		Concurrency:     1,
		Consume:         4,
		TimeoutAttempts: 2,
		Recover:         queue.NewReliab(env.rdb, env.q, queue.ReliabOptions{MaxAttempts: 3, MinIdle: 300 * time.Millisecond}),
		RecoverInterval: 200 * time.Millisecond,
		Fallback:        renderworker.FallbackOptions{Enabled: true},
		// The candidate engine of worker A's build (S-P1-1): the
		// engine-based publication guard — chromium (including an empty
		// value) does not publish the fallback (step 1 of the spec §6.2
		// acceptance, the §6.4 rollback); the engine is only a field, the
		// render is decided by the fake tarpit above.
		Engine:  config.EngineMoli,
		Metrics: regA,
	}
	wA, err := renderworker.New(env.pool, env.q, env.store, accFailRenderer{err: tarpit}, optsA)
	if err != nil {
		t.Fatal(err)
	}
	ctxA, cancelA := context.WithCancel(ctx)
	runADone := make(chan struct{})
	go func() { defer close(runADone); _ = renderworker.Run(ctxA, wA, optsA) }()

	// The task went to q:render-fallback (the first delivery — an error,
	// the claim with attempts=2 — a publication and an XACK).
	accWaitFor(t, "the task published to q:render-fallback", 30*time.Second, func() bool {
		return streamLen(t, env.rdb, queue.StreamRenderFallback) == 1
	})
	cancelA()
	<-runADone

	t.Run("the primary worker: the row waits for the fallback without a marker", func(t *testing.T) {
		row := accRows(t, env.pool, ids["spa-shell"])[0]
		if row.rendered {
			t.Error("rendered must stay false — no render happened")
		}
		if accHash(row.contentHash) != rawHash {
			t.Error("content_hash must not change before worker B finishes the row")
		}
		var failedAt *time.Time
		if err := env.pool.QueryRow(ctx,
			"SELECT render_failed_at FROM subdomain_results WHERE domain_id = $1", ids["spa-shell"],
		).Scan(&failedAt); err != nil {
			t.Fatal(err)
		}
		if failedAt != nil {
			t.Error("render_failed_at must not be set — worker B sets the marker after exhausting the fallback (FR-10)")
		}
		// q:analyze is not published by the primary worker: worker B will
		// publish the analysis after finishing the row (FR-10).
		if n := streamLen(t, env.rdb, queue.StreamAnalyze); n != analyzeBefore {
			t.Errorf("q:analyze = %d, want %d (no growth — the publication belongs to worker B)", n, analyzeBefore)
		}
	})

	// Worker B: the same binary built differently — the q:render-fallback
	// stream, a separate group; a live Chromium finishes the row.
	browser, err := render.StartBrowser(context.Background(), render.Options{
		RemoteURL:         accChromiumWS(),
		Contexts:          1,
		Timeout:           30 * time.Second,
		RestartEveryPages: 1000,
	})
	if err != nil {
		t.Fatalf("Chromium connection: %v", err)
	}
	t.Cleanup(browser.Close)
	chromePool := render.NewPool(browser)
	t.Cleanup(chromePool.Close)
	optsB := renderworker.Options{
		Stream:          queue.StreamRenderFallback,
		Group:           "acc-render-fb-b",
		Consumer:        "acc-fb-b",
		Concurrency:     1,
		Consume:         4,
		Recover:         queue.NewReliab(env.rdb, env.q, queue.ReliabOptions{MaxAttempts: 3, MinIdle: 300 * time.Millisecond}),
		RecoverInterval: 200 * time.Millisecond,
		Fallback:        renderworker.FallbackOptions{Enabled: true}, // the recursion is forbidden by the stream (FR-10)
		Metrics:         regB,                                        // the fallback consumption is build B's counter (T-245)
	}
	wB, err := renderworker.New(env.pool, env.q, env.store, accHTTPRenderer{inner: chromePool}, optsB)
	if err != nil {
		t.Fatal(err)
	}
	ctxB, cancelB := context.WithCancel(ctx)
	runBDone := make(chan struct{})
	go func() { defer close(runBDone); _ = renderworker.Run(ctxB, wB, optsB) }()

	accWaitFor(t, "worker B rendered the fallback row", 90*time.Second, func() bool {
		rows := accRows(t, env.pool, ids["spa-shell"])
		return len(rows) == 1 && rows[0].rendered
	})
	// Wait for the delivery acknowledgment (an XACK after the write and the
	// publication): the balance block below reads the group's PEL and the
	// stream depth — canceling worker B's ctx in the "row commit → XACK"
	// window would leave the row in the PEL and flake the check (the T-245
	// review).
	accWaitFor(t, "worker B acknowledged the fallback delivery (XACK)", 30*time.Second, func() bool {
		return pendingCount(t, env.rdb, queue.StreamRenderFallback, "acc-render-fb-b") == 0
	})
	cancelB()
	<-runBDone

	t.Run("worker B: the DOM stored, q:analyze published", func(t *testing.T) {
		after := accRows(t, env.pool, ids["spa-shell"])[0]
		if !after.rendered {
			t.Fatal("rendered not set — the fallback finish not recorded")
		}
		if accHash(after.contentHash) == rawHash {
			t.Error("content_hash unchanged — raw HTML stored instead of the DOM")
		}
		dom, err := env.store.Get(ctx, accHash(after.contentHash))
		if err != nil {
			t.Fatalf("DOM read from the store: %v", err)
		}
		if !strings.Contains(string(dom), "SPA-контент после рендера") {
			t.Errorf("the DOM lacks the JS-drawn content: %.120q", dom)
		}
		if n := streamLen(t, env.rdb, queue.StreamAnalyze); n != analyzeBefore+1 {
			t.Errorf("q:analyze = %d, want %d (+1 — the finish publication, Fresh)", n, analyzeBefore+1)
		}
		if n := streamLen(t, env.rdb, queue.StreamRenderFallback); n != 1 {
			t.Errorf("q:render-fallback = %d, want 1 (a recursive fallback forbidden, FR-10)", n)
		}
	})

	// The fallback balance check (T-245, step 4 of the spec §6.2
	// acceptance, FR-11): what worker A published equals what worker B
	// consumed plus the queue remainder. The instrumentation is the same
	// instruments as in production: published/consumed — the builds'
	// counters, the remainder — the crawler_queue_depth stream depth
	// (BackpressureGate.Len: min(lag, XLEN) + the PEL over the groups);
	// its PEL part is a direct XPENDING of build B's group. The scenario
	// is finished to the end — the remainder is 0, no divergences.
	t.Run("balance: published = consumed + the q:render-fallback remainder", func(t *testing.T) {
		published := accCounterSum(t, regA, "crawler_render_fallback_published_total")
		if published != 1 {
			t.Fatalf("published = %v, want 1 (one tarpit publication)", published)
		}
		consumed := accCounterSum(t, regB, "crawler_render_fallback_consumed_total")
		if consumed != 1 {
			t.Fatalf("consumed = %v, want 1 (one finish by worker B)", consumed)
		}
		if got := accCounterSum(t, regB, "crawler_render_fallback_consumed_total", "result", "rendered"); got != 1 {
			t.Errorf("consumed{rendered} = %v, want 1 (the finish outcome — success)", got)
		}
		depth, err := gate.Len(ctx, queue.StreamRenderFallback)
		if err != nil {
			t.Fatalf("q:render-fallback depth (crawler_queue_depth): %v", err)
		}
		if pel := pendingCount(t, env.rdb, queue.StreamRenderFallback, "acc-render-fb-b"); pel != 0 {
			t.Errorf("worker B's group PEL remainder = %d, want 0 (the delivery acknowledged)", pel)
		}
		if diff := published - consumed - float64(depth); diff != 0 {
			t.Errorf("the fallback balance: published(%v) != consumed(%v) + remainder(%d), divergence %v", published, consumed, depth, diff)
		}
		// Build A consumed nothing: its registry has no consumed series.
		if got := accCounterSum(t, regA, "crawler_render_fallback_consumed_total"); got != 0 {
			t.Errorf("the primary build's consumed = %v, want 0 (only worker B counts)", got)
		}
	})
}

// ---------------------------------------------------------------- item 4

// TestAccept04_ReanalyzeNoRefetch — §11.4 item 4: "…a rule change +
// reanalyze updates the results without a refetch". The working samples
// are analyzed with the first set → the rules change (a probe firing on
// one of the samples is added) → ReanalyzeOnce publishes q:analyze → the
// worker with the second set overwrites the findings: the target sample
// gains the probe hit and a new score (the rules were really reapplied,
// not the rules_version re-stamped), the others keep their verdicts. The
// traces of a refetch are checked by their absence: the HTTP episodes do
// not run, the result rows do not change (checked_at, content_hash), the
// store does not grow.
func TestAccept04_ReanalyzeNoRefetch(t *testing.T) {
	skipUnlessIntegration(t)
	env := newAccEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	labels := []string{"laravel-11-vite", "symfony-negative"}
	ids := accPipeline(ctx, t, env, accZone, labels)
	zoneID := accZoneID(t, env.pool, accZone)
	accAnalyze(ctx, t, env, env.analyzer, len(labels))

	v1 := env.rules.Version
	if v1 == 0 {
		t.Fatal("the working set's version must not be zero")
	}

	// The "before" snapshot: the result rows and the store.
	type rowSnap struct {
		checked time.Time
		hash    []byte
		version int32
	}
	before := map[int64][]rowSnap{}
	for _, l := range labels {
		rows, err := env.pool.Query(ctx, `
			SELECT checked_at, content_hash, rules_version FROM subdomain_results
			WHERE domain_id = $1 ORDER BY subdomain_id, variant`, ids[l])
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var s rowSnap
			if err := rows.Scan(&s.checked, &s.hash, &s.version); err != nil {
				t.Fatal(err)
			}
			before[ids[l]] = append(before[ids[l]], s)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
	var contentsBefore int
	if err := env.pool.QueryRow(ctx, "SELECT count(*) FROM contents").Scan(&contentsBefore); err != nil {
		t.Fatal(err)
	}
	dnsBefore := streamLen(t, env.rdb, queue.StreamDNS)
	httpBefore := streamLen(t, env.rdb, queue.StreamHTTP)
	renderBefore := streamLen(t, env.rdb, queue.StreamRender)

	// The rules changed: a set copy with a probe firing on symfony-negative
	// — the canonical hash (Version) is different; the target must gain a
	// new signal and a higher score.
	loader2, err := rules.NewReloader(accModifiedRulesDir(t), nil)
	if err != nil {
		t.Fatalf("the modified rule set: %v", err)
	}
	v2 := loader2.Set().Version
	if v2 == v1 {
		t.Fatal("the set version unchanged — reanalyze will not find stale rows")
	}
	an2, err := analyzer.New(env.pool, env.q, env.store, loader2, analyzer.Options{
		MinScore: rules.DefaultMinScore,
	})
	if err != nil {
		t.Fatal(err)
	}

	res, err := analyzer.ReanalyzeOnce(ctx, env.pool, env.q, v2, nil, analyzer.ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("ReanalyzeOnce: %v", err)
	}
	// The selection — the stale rows (rules_version IS DISTINCT FROM v2).
	if res.Selected != len(labels) || res.Published != len(labels) {
		t.Fatalf("reanalyze: selected=%d published=%d, want %d/%d", res.Selected, res.Published, len(labels), len(labels))
	}

	// The second set's worker re-consumes q:analyze.
	accAnalyze(ctx, t, env, an2, len(labels))

	// The findings are overwritten with the new version; the rules were
	// really reapplied: the target sample's probe hit appeared and the
	// score was recomputed (a noisy-OR with the probe weight), the others
	// keep their verdicts. A stub that "only updated the rules_version
	// stamp, reusing signals/score" fails here.
	for _, l := range labels {
		fs := accFindings(t, env.pool, zoneID, ids[l])
		sample := accSampleByID(t, l)
		// The findings row is one per (domain, subdomain, variant): the hits
		// are in the signals jsonb, there is no separate row per rule
		// (T-071, §6.1).
		if len(fs) != 1 {
			t.Errorf("%s: findings after the reanalysis = %d rows, want 1", l, len(fs))
			continue
		}
		fired := accFiredRules(t, fs[0].signals)
		wantScore := *sample.Expect.Score
		wantFired := len(sample.Expect.Rules)
		if l == accProbeSample {
			wantFired++
			wantScore = 1 - (1-*sample.Expect.Score)*(1-accProbeWeight)
			if !fired[accProbeRule] {
				t.Errorf("%s: the probe %s missing from signals — the rules not reapplied (a rules_version re-stamp?)", l, accProbeRule)
			}
		} else if fired[accProbeRule] {
			t.Errorf("%s: the probe %s fired outside the target sample %s", l, accProbeRule, accProbeSample)
		}
		if got := len(fired); got != wantFired {
			t.Errorf("%s: hits in signals = %d (%v), want %d", l, got, fired, wantFired)
		}
		for _, f := range fs {
			if f.rulesVersion != v2 {
				t.Errorf("%s: the hit's rules_version = %d, want %d (the new set)", l, f.rulesVersion, v2)
			}
			if math.Abs(float64(f.score)-wantScore) > accScoreTol {
				t.Errorf("%s: score after the reanalysis = %.4f, want ≈%.3f", l, f.score, wantScore)
			}
		}
	}

	// No refetch: the rows did not change at all, except the stamp.
	for _, l := range labels {
		rows, err := env.pool.Query(ctx, `
			SELECT checked_at, content_hash, rules_version FROM subdomain_results
			WHERE domain_id = $1 ORDER BY subdomain_id, variant`, ids[l])
		if err != nil {
			t.Fatal(err)
		}
		i := 0
		for rows.Next() {
			var s rowSnap
			if err := rows.Scan(&s.checked, &s.hash, &s.version); err != nil {
				t.Fatal(err)
			}
			if i >= len(before[ids[l]]) {
				t.Fatalf("%s: more rows after the reanalysis than before", l)
			}
			want := before[ids[l]][i]
			if !s.checked.Equal(want.checked) {
				t.Errorf("%s: checked_at changed (%v → %v) — a recheck?", l, want.checked, s.checked)
			}
			if string(s.hash) != string(want.hash) {
				t.Errorf("%s: content_hash changed — the content refetched", l)
			}
			if s.version != v2 {
				t.Errorf("%s: the row's rules_version = %d, want %d", l, s.version, v2)
			}
			i++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if i != len(before[ids[l]]) {
			t.Errorf("%s: %d rows, want %d", l, i, len(before[ids[l]]))
		}
	}
	if n := streamLen(t, env.rdb, queue.StreamDNS); n != dnsBefore {
		t.Errorf("q:dns grew: %d → %d — the DNS stage ran", dnsBefore, n)
	}
	if n := streamLen(t, env.rdb, queue.StreamHTTP); n != httpBefore {
		t.Errorf("q:http grew: %d → %d — the download repeated", httpBefore, n)
	}
	if n := streamLen(t, env.rdb, queue.StreamRender); n != renderBefore {
		t.Errorf("q:render changed: %d → %d", renderBefore, n)
	}
	var contentsAfter int
	if err := env.pool.QueryRow(ctx, "SELECT count(*) FROM contents").Scan(&contentsAfter); err != nil {
		t.Fatal(err)
	}
	if contentsAfter != contentsBefore {
		t.Errorf("the store grew: %d → %d blocks — the content refetched", contentsBefore, contentsAfter)
	}
}

// ---------------------------------------------------------------- item 5

// TestAccept05_LaravelTablesQueries — §11.4 item 5: "The laravel_domains /
// laravel_findings table makes it possible to get: the list of Laravel
// domains, the subdomains, the version (a range), the signals". SQL
// selections from the tables after the pipeline: a confident domain, a
// weak negative (findings without an aggregate), a clean negative (no
// findings at all).
func TestAccept05_LaravelTablesQueries(t *testing.T) {
	skipUnlessIntegration(t)
	env := newAccEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	labels := []string{"laravel-8-mix", "rails-negative", "django-negative"}
	ids := accPipeline(ctx, t, env, accZone, labels)
	zoneID := accZoneID(t, env.pool, accZone)
	accAnalyze(ctx, t, env, env.analyzer, len(labels))

	t.Run("the list of Laravel domains", func(t *testing.T) {
		rows, err := env.pool.Query(ctx, `
			SELECT d.label FROM laravel_domains AS ld
			JOIN domains AS d ON d.zone_id = ld.zone_id AND d.id = ld.domain_id
			WHERE d.zone_id = $1 ORDER BY d.label`, zoneID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var got []string
		for rows.Next() {
			var label string
			if err := rows.Scan(&label); err != nil {
				t.Fatal(err)
			}
			got = append(got, label)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		// The §13.1 threshold: a confident sample gives a row, weak ones do not.
		if len(got) != 1 || got[0] != "laravel-8-mix" {
			t.Errorf("Laravel domains = %v, want [laravel-8-mix]", got)
		}
	})

	t.Run("subdomains with hits (found_on_mask ↔ the reference table)", func(t *testing.T) {
		// Decoding found_on_mask with the bits of the subdomains reference
		// table — the subdomains Laravel was found on (spec 5.8).
		var labelsOut []string
		rows, err := env.pool.Query(ctx, `
			SELECT s.label FROM laravel_domains AS ld
			JOIN subdomains AS s ON (ld.found_on_mask & (1 << s.id)) <> 0
			WHERE ld.zone_id = $1 AND ld.domain_id = $2
			ORDER BY s.id`, zoneID, ids["laravel-8-mix"])
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var l string
			if err := rows.Scan(&l); err != nil {
				t.Fatal(err)
			}
			labelsOut = append(labelsOut, l)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if len(labelsOut) != 1 || labelsOut[0] != "" {
			t.Errorf("subdomains with hits = %q, want an empty label (the apex, bit 0)", labelsOut)
		}
	})

	t.Run("the version (a range) and the confident domain's hits", func(t *testing.T) {
		agg, ok := accLaravelDomain(t, env.pool, zoneID, ids["laravel-8-mix"])
		if !ok {
			t.Fatal("laravel_domains has no row for the confident domain")
		}
		// The sample's range: Mix — up to 9.x (meta.yaml, the bounds are rule lines).
		if got := accStr(agg.verMin); got != "" {
			t.Errorf("ver_min = %q, want NULL (no lower bound)", got)
		}
		if got := accStr(agg.verMax); got != "9.x" {
			t.Errorf("ver_max = %q, want 9.x", got)
		}
		fs := accFindings(t, env.pool, zoneID, ids["laravel-8-mix"])
		if len(fs) != 1 {
			t.Fatalf("findings = %d rows, want 1 (the apex, variant 0)", len(fs))
		}
		fired := accFiredRules(t, fs[0].signals)
		want := []string{
			"cookie_xsrf_token", "cookie_laravel_session", "cookie_session_suffix",
			"cookie_encrypted_payload", "meta_csrf_token", "mix_assets",
		}
		for _, r := range want {
			if !fired[r] {
				t.Errorf("the rule %s did not fire (fired %v)", r, fired)
			}
		}
		if len(fired) != len(want) {
			t.Errorf("%d fired rules, want %d (%v)", len(fired), len(want), fired)
		}
		if !fs[0].isLaravel {
			t.Error("is_laravel of a confident hit = false, want true (the 0.6 threshold)")
		}
	})

	t.Run("weak hits — findings only", func(t *testing.T) {
		// rails: csrf-meta + *_session below the threshold — all hits in
		// laravel_findings (§13.1), no laravel_domains row.
		fs := accFindings(t, env.pool, zoneID, ids["rails-negative"])
		if len(fs) != 1 {
			t.Fatalf("rails findings = %d rows, want 1", len(fs))
		}
		if fs[0].isLaravel {
			t.Error("a weak hit marked is_laravel = true")
		}
		fired := accFiredRules(t, fs[0].signals)
		if !fired["meta_csrf_token"] || !fired["cookie_session_suffix"] {
			t.Errorf("rails weak signs = %v, want csrf-meta and *_session", fired)
		}
		if _, ok := accLaravelDomain(t, env.pool, zoneID, ids["rails-negative"]); ok {
			t.Error("a weak domain made it into laravel_domains — the 0.6 threshold violated")
		}

		// django: not a single hit — neither findings nor an aggregate.
		if fs := accFindings(t, env.pool, zoneID, ids["django-negative"]); len(fs) != 0 {
			t.Errorf("the clean negative's findings = %d rows, want 0", len(fs))
		}
		if _, ok := accLaravelDomain(t, env.pool, zoneID, ids["django-negative"]); ok {
			t.Error("a clean negative made it into laravel_domains")
		}
	})
}

// ---------------------------------------------------------------- item 6

// TestAccept06_ZoneProgressAutomatic — §11.4 item 6: "Zone progress
// (total / checked) is displayed and updated automatically; the metrics
// are available in Prometheus". The zone counters are recomputed by
// monitor (ZoneStatsOnce/ZoneStatsLoop — the same code as the service);
// the display is the zones.domains_total/domains_checked cache that
// crawlerctl status/zones reads; the metrics — the /metrics exposure of
// the service's registry (what the dev bench's Prometheus scrapes).
func TestAccept06_ZoneProgressAutomatic(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The data: a zone with 4 live domains (2 done, 1 pending, 1 queued)
	// and 1 deleted; an empty zone — its counters are zeroed, not frozen.
	zoneA := accInsertZone(t, pool, "acc6.test", "")
	zoneB := accInsertZone(t, pool, "acc6-empty.test", "")
	done := accInsertDomains(t, pool, zoneA, []string{"d1", "d2"})
	accInsertDomains(t, pool, zoneA, []string{"p1"})
	accInsertDomains(t, pool, zoneA, []string{"q1"})
	gone := accInsertDomains(t, pool, zoneA, []string{"g1"})
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`UPDATE domains SET check_state = %d WHERE id = ANY($1)`, int16(model.CheckStateDone)), done,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`UPDATE domains SET check_state = %d, queued_at = now() WHERE zone_id = $1 AND label = 'q1'`,
		int16(model.CheckStateQueued)), zoneA,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`UPDATE domains SET status = %d, check_state = %d WHERE id = $1`,
		int16(model.StatusDeleted), int16(model.CheckStateDone)), gone[0],
	); err != nil {
		t.Fatal(err)
	}

	// Zone A's Laravel finds (T-120): one laravel_domains row — the
	// domains_laravel cache of the same pass; the rows already passed the
	// analyzer threshold.
	if _, err := pool.Exec(ctx,
		`INSERT INTO laravel_domains (zone_id, domain_id, score, found_on_mask, rules_version, updated_at)
		 VALUES ($1, $2, 0.9, 1, 1, now())`, zoneA, done[0],
	); err != nil {
		t.Fatal(err)
	}

	res, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}
	if res.Zones != 2 {
		t.Errorf("zones recomputed: %d, want 2", res.Zones)
	}
	accZoneCounters := func(zone int16) (total, checked, laravel int32, updated bool) {
		t.Helper()
		var u *time.Time
		if err := pool.QueryRow(ctx,
			"SELECT domains_total, domains_checked, domains_laravel, updated_at FROM zones WHERE id = $1", zone,
		).Scan(&total, &checked, &laravel, &u); err != nil {
			t.Fatal(err)
		}
		return total, checked, laravel, u != nil
	}
	total, checked, laravel, updated := accZoneCounters(zoneA)
	if total != 4 || checked != 2 {
		t.Errorf("zone A: total/checked = %d/%d, want 4/2 (deleted outside the denominator, queued not checked)", total, checked)
	}
	if laravel != 1 {
		t.Errorf("zone A: domains_laravel = %d, want 1 (the same pass, T-120)", laravel)
	}
	if !updated {
		t.Error("updated_at not set — the \"progress stall\" alert (T-080) goes blind")
	}
	if total, checked, laravel, _ := accZoneCounters(zoneB); total != 0 || checked != 0 || laravel != 0 {
		t.Errorf("empty zone: total/checked/laravel = %d/%d/%d, want 0/0/0", total, checked, laravel)
	}

	// The display: the cache converges with the direct aggregate (the
	// crawlerctl status query over the live domains — "total/checked" from
	// the same data).
	var liveTotal, liveChecked int
	if err := pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT count(*), count(*) FILTER (WHERE check_state = %d)
		FROM domains WHERE zone_id = $1 AND status <> %d`,
		int16(model.CheckStateDone), int16(model.StatusDeleted)), zoneA,
	).Scan(&liveTotal, &liveChecked); err != nil {
		t.Fatal(err)
	}
	if int32(liveTotal) != total || int32(liveChecked) != checked {
		t.Errorf("the cache %d/%d diverges from the aggregate %d/%d", total, checked, liveTotal, liveChecked)
	}

	// Updates automatically: the monitor service's loop with a short
	// interval picks up the state change itself (the first pass — right
	// away, then on the tick; the same ZoneStatsLoop as in cmd/monitor).
	loopCtx, stop := context.WithCancel(ctx)
	loopDone := make(chan error, 1)
	go func() {
		loopDone <- monitor.ZoneStatsLoop(loopCtx, pool, nil, monitor.ZoneStatsOptions{Interval: 100 * time.Millisecond})
	}()
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`UPDATE domains SET check_state = %d WHERE zone_id = $1 AND label = 'p1'`,
		int16(model.CheckStateDone)), zoneA,
	); err != nil {
		t.Fatal(err)
	}
	accWaitFor(t, "the automatic checked 2 → 3 update", 5*time.Second, func() bool {
		_, checked, _, _ := accZoneCounters(zoneA)
		return checked == 3
	})
	stop()
	if err := <-loopDone; err != nil {
		t.Errorf("ZoneStatsLoop exited with an error: %v", err)
	}

	// The metrics: the real exposure of the running monitor service — what
	// the dev bench's Prometheus scrapes (docker DNS monitor:9100, as in
	// deploy/docker/prometheus/prometheus.yml). An HTTP request to the
	// service, not a local registry: a forgotten route, a wrong port or a
	// silent handler fail here due to the production code. If the
	// application service is not up (make test-integration starts only the
	// infrastructure and the testbed) — an honest skip, not the tautology
	// of "own registry → own handler".
	url := accMonitorMetricsURL()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Skipf("the monitor service unavailable (%s: %v) — bring up the app profile (make up): the real /metrics exposure needs a running service", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("the monitor service's /metrics: code %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(string(body), `crawler_service_up{service="monitor"} 1`) {
		t.Errorf("the monitor exposure without crawler_service_up{service=\"monitor\"} 1:\n%s", string(body))
	}
}

// ---------------------------------------------------------------- item 8

// TestAccept08_RecheckInterval — §11.4 item 8: "A domain is rechecked no
// more often than the given interval (6 months by default); the value is
// changed by config". Domains of different ages against a live DB: the
// done → pending change takes only the mature ones, a repeat pass is
// empty (no more often), shrinking the interval by config picks up more
// recent ones.
func TestAccept08_RecheckInterval(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	zone := accInsertZone(t, pool, "acc8.test", "")
	accInsertDomains(t, pool, zone, []string{"old", "fresh", "never", "pending", "gone"})
	set := func(label string, q string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, q, args...); err != nil {
			t.Fatalf("domain %s: %v", label, err)
		}
	}
	alive, done, del := int16(model.StatusAlive), int16(model.CheckStateDone), int16(model.StatusDeleted)
	set("old", fmt.Sprintf(`UPDATE domains SET status = %d, check_state = %d, last_checked_at = DATE '2025-04-01' WHERE zone_id = $1 AND label = 'old'`, alive, done), zone)
	set("fresh", fmt.Sprintf(`UPDATE domains SET status = %d, check_state = %d, last_checked_at = DATE '2026-10-01' WHERE zone_id = $1 AND label = 'fresh'`, alive, done), zone)
	set("never", fmt.Sprintf(`UPDATE domains SET status = %d, check_state = %d WHERE zone_id = $1 AND label = 'never'`, alive, done), zone)
	set("pending", fmt.Sprintf(`UPDATE domains SET status = %d, check_state = %d, last_checked_at = DATE '2025-04-01' WHERE zone_id = $1 AND label = 'pending'`, alive, int16(model.CheckStatePending)), zone)
	set("gone", fmt.Sprintf(`UPDATE domains SET status = %d, check_state = %d, last_checked_at = DATE '2025-04-01' WHERE zone_id = $1 AND label = 'gone'`, del, done), zone)

	state := func(label string) (int16, int16) {
		t.Helper()
		var status, check int16
		if err := pool.QueryRow(ctx,
			"SELECT status, check_state FROM domains WHERE zone_id = $1 AND label = $2", zone, label,
		).Scan(&status, &check); err != nil {
			t.Fatal(err)
		}
		return status, check
	}

	// The default — 6 months (§3.3): the config and the scheduler constant
	// are one value.
	if got := config.Default().Scheduler.RecheckInterval; got != config.Duration(scheduler.DefaultRecheckInterval) {
		t.Errorf("the config default = %s, want %s (6 months)", got, scheduler.DefaultRecheckInterval)
	}
	// The value is changed by config: the environment variable overrides.
	envCfg, err := config.LoadEnv(func(key string) string {
		if key == "CRAWLER_SCHEDULER_RECHECK_INTERVAL" {
			return "170h"
		}
		return os.Getenv(key) // the rest of the environment (incl. APP_PG_DSN) — as is
	})
	if err != nil {
		t.Fatalf("config with an override: %v", err)
	}
	if got := envCfg.Scheduler.RecheckInterval; got != config.Duration(170*time.Hour) {
		t.Errorf("the environment override = %s, want 170h", got)
	}

	// A fixed "now": the domains' age does not depend on the run date.
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	res, err := scheduler.RecheckOnce(ctx, pool, scheduler.RecheckOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("RecheckOnce: %v", err)
	}
	if res.Domains != 1 {
		t.Fatalf("domains moved: %d, want 1 (only the mature old)", res.Domains)
	}
	if _, check := state("old"); check != int16(model.CheckStatePending) {
		t.Errorf("old: check_state = %d, want pending", check)
	}
	// The result status and foreign states are untouched.
	if s, _ := state("old"); s != int16(model.StatusAlive) {
		t.Errorf("old: the result status changed (%d) — a recheck only touches the cycle", s)
	}
	for _, label := range []string{"fresh", "never", "pending", "gone"} {
		if _, check := state(label); check == int16(model.CheckStatePending) && label != "pending" {
			t.Errorf("%s: moved to pending outside the interval", label)
		}
	}

	// No more often than the interval: a repeat pass finds no work.
	res, err = scheduler.RecheckOnce(ctx, pool, scheduler.RecheckOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if res.Domains != 0 {
		t.Errorf("a repeat pass moved %d more — the interval not respected", res.Domains)
	}

	// The interval changes by config: 7 days (shorter than fresh's age of
	// ~17 days, but longer than the age of "checked today") — fresh is
	// mature too, never (never checked) and pending/deleted still are not.
	// Maturity — age > interval (§11.4 item 8 "no more often"); a longer
	// interval takes only the older ones.
	res, err = scheduler.RecheckOnce(ctx, pool, scheduler.RecheckOptions{
		Interval: 7 * 24 * time.Hour,
		Now:      func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Domains != 1 {
		t.Fatalf("after the interval change %d moved, want 1 (fresh)", res.Domains)
	}
	if _, check := state("fresh"); check != int16(model.CheckStatePending) {
		t.Errorf("fresh: check_state = %d, want pending", check)
	}
	for _, label := range []string{"never", "gone"} {
		if _, check := state(label); check != int16(model.CheckStateDone) {
			t.Errorf("%s: check_state = %d, want done (no age/status)", label, check)
		}
	}
}

// ---------------------------------------------------------------- item 9

// TestAccept09_RestartNoStuckDomains — §11.4 item 9: "Stopping/restarting
// any service does not lead to data loss or "stuck" domains (within the
// lease time)". The edge not covered by the neighbors (e2e — a PostgreSQL
// drop, resilience — a Redis loss and a worker death in the middle of a
// batch): a stopped service leaves the domains queued with a lease; a live
// lease is not taken away, an expired one — LeaseOnce returns to pending,
// the dispatcher re-selects and re-publishes the batch; no data is lost.
func TestAccept09_RestartNoStuckDomains(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	rdb := resilienceRedisEnv(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	labels := []string{"a1", "a2", "a3"}
	ids := insertPendingDomains(t, pool, "acc9.test", labels)
	batch, err := scheduler.DispatchBatch(ctx, pool, scheduler.DispatchQuery{Limit: len(labels)})
	if err != nil {
		t.Fatalf("dispatch batch: %v", err)
	}
	if _, err := scheduler.EnqueueBatch(ctx, pool, q, batch); err != nil {
		t.Fatalf("enqueue batch: %v", err)
	}
	if n := countDomains(t, pool, stateWhere(model.CheckStateQueued)); n != len(labels) {
		t.Fatalf("queued: %d, want %d", n, len(labels))
	}

	// A live lease is not given back: "the service works" — the domains
	// stay in work (Now without a shift, the lease was just issued). A nil
	// cursor — a one-off pass from the zero cursor (T-200).
	res, err := scheduler.LeaseOnce(ctx, pool, nil, scheduler.LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}
	if res.Domains != 0 {
		t.Fatalf("the return with a live lease: %d, want 0", res.Domains)
	}

	// A service stop: the worker died, the domains hung in queued. Shifting
	// "now" beyond the lease term — the lease pass's view after downtime.
	shifted := func() time.Time { return time.Now().Add(2 * scheduler.DefaultLeaseTimeout) }
	res, err = scheduler.LeaseOnce(ctx, pool, nil, scheduler.LeaseOptions{Now: shifted})
	if err != nil {
		t.Fatal(err)
	}
	if res.Domains != int64(len(labels)) {
		t.Fatalf("the expired returned: %d, want %d", res.Domains, len(labels))
	}
	if n := countDomains(t, pool, stateWhere(model.CheckStatePending)); n != len(labels) {
		t.Fatalf("pending after the return: %d, want %d (not stuck in queued)", n, len(labels))
	}

	// The dispatcher re-selects and re-publishes: the domains are in work
	// again, none lost (the id set preserved).
	batch2, err := scheduler.DispatchBatch(ctx, pool, scheduler.DispatchQuery{Limit: len(labels)})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch2) != len(labels) {
		t.Fatalf("re-dispatched: %d, want %d", len(batch2), len(labels))
	}
	got := idSet(func() []int64 {
		out := make([]int64, 0, len(batch2))
		for _, d := range batch2 {
			out = append(out, d.DomainID)
		}
		return out
	}())
	want := idSet(ids)
	if len(got) != len(want) {
		t.Fatalf("the domain set changed: %v vs %v — data loss", got, want)
	}
	// The composition, not just the size: re-selecting foreign ones or
	// duplicates at the same size is also data loss.
	for id := range want {
		if !got[id] {
			t.Fatalf("domain %d lost on republishing: %v vs %v", id, got, want)
		}
	}
	if _, err := scheduler.EnqueueBatch(ctx, pool, q, batch2); err != nil {
		t.Fatal(err)
	}
	if n := streamLen(t, rdb, queue.StreamDNS); n != 2 {
		t.Errorf("q:dns batches: %d, want 2 (the first + the republished)", n)
	}

	// A deleted domain outside the cycle: neither the lease pass nor the dispatcher touch it.
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`UPDATE domains SET status = %d WHERE id = $1`, int16(model.StatusDeleted)), ids[0]); err != nil {
		t.Fatal(err)
	}
	res, err = scheduler.LeaseOnce(ctx, pool, nil, scheduler.LeaseOptions{Now: shifted})
	if err != nil {
		t.Fatal(err)
	}
	// The §13.1 guard: a deleted one does not return to the cycle — only live ones.
	if res.Domains != int64(len(labels)-1) {
		t.Fatalf("the lease after the second hang: %d, want %d (deleted outside the cycle)", res.Domains, len(labels)-1)
	}
	batch3, err := scheduler.DispatchBatch(ctx, pool, scheduler.DispatchQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch3) != len(labels)-1 {
		t.Errorf("re-dispatched after the deletion: %d, want %d", len(batch3), len(labels)-1)
	}
	for _, d := range batch3 {
		if d.DomainID == ids[0] {
			t.Error("a deleted domain dispatched (§13.1)")
		}
	}
}

// ---------------------------------------------------------------- item 10

// TestAccept10_DomainSearchByName — §11.4 item 10: "A search by name
// (crawlerctl domain) for a domain in any writing form (unicode/punycode)
// returns the status, the per-subdomain results and the Laravel signals
// (if any); for an unknown domain a clear message is printed". The level
// of the SQL function domain_info (the single source of truth of the CLI
// and psql, T-082) + the form normalization of internal/idn — what
// crawlerctl does before the query.
func TestAccept10_DomainSearchByName(t *testing.T) {
	skipUnlessIntegration(t)
	env := newAccEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Two domains: an IDN in the IDN zone (the zone's unicode display name)
	// and a Laravel sample with signals.
	idnZone := accInsertZone(t, env.pool, accIDNZone, accIDNZoneUni)
	accPipelineZone(ctx, t, env, idnZone, accIDNZone, []string{accIDNLabel})
	accPipeline(ctx, t, env, accZone, []string{"laravel-8-mix"})
	accAnalyze(ctx, t, env, env.analyzer, 2)

	info := func(name string) map[string]any {
		t.Helper()
		var raw []byte
		if err := env.pool.QueryRow(ctx, "SELECT domain_info($1)", name).Scan(&raw); err != nil {
			t.Fatalf("domain_info(%q): %v", name, err)
		}
		if raw == nil {
			return nil
		}
		return dinfoDecode(t, raw)
	}

	t.Run("the IDN domain in any writing form", func(t *testing.T) {
		// The input forms: unicode (case, a dot), punycode — the
		// internal/idn normalization before the DB call, as crawlerctl
		// domain (T-081).
		forms := []string{
			accIDNLabelU + "." + accIDNZoneUni + ".",     // сайт.Проверка.TEST.
			"  " + accIDNLabel + "." + accIDNZone + "  ", // punycode with edge spaces
		}
		var canonical map[string]any
		for _, form := range forms {
			n, err := idn.Normalize(form)
			if err != nil {
				t.Fatalf("normalize %q: %v", form, err)
			}
			if n.ASCII != accIDNLabel+"."+accIDNZone {
				t.Fatalf("the normalized form %q → %q, want %s", form, n.ASCII, accIDNLabel+"."+accIDNZone)
			}
			got := info(n.ASCII)
			if got == nil {
				t.Fatalf("domain_info(%q from the form %q) = NULL", n.ASCII, form)
			}
			if canonical == nil {
				canonical = got
				continue
			}
			if fmt.Sprint(got["domain"]) != fmt.Sprint(canonical["domain"]) {
				t.Errorf("the writing forms gave different answers: %v vs %v", got["domain"], canonical["domain"])
			}
		}

		// The status, the per-subdomain results, the zone with a unicode name.
		dom := dinfoMap(t, canonical["domain"], "domain")
		if got := dinfoStr(t, dom["status_name"], "domain.status_name"); got != "alive" {
			t.Errorf("status = %q, want alive", got)
		}
		if got := dinfoStr(t, dom["name"], "domain.name"); got != accIDNLabel+"."+accIDNZone {
			t.Errorf("the name = %q, want the punycode form", got)
		}
		if alive := dinfoSlice(t, canonical["dns_alive"], "dns_alive"); len(alive) != 1 {
			t.Errorf("dns_alive = %v, want one alive subdomain (the apex)", alive)
		}
		results := dinfoSlice(t, canonical["results"], "results")
		if len(results) != 1 {
			t.Fatalf("results = %d entries, want 1", len(results))
		}
		rec := dinfoMap(t, results[0], "results[0]")
		if got := dinfoStr(t, rec["final_url"], "results[0].final_url"); got != "https://"+accIDNLabel+"."+accIDNZone+"/" {
			t.Errorf("the final URL = %q", got)
		}
		zone := dinfoMap(t, canonical["zone"], "zone")
		if got := dinfoStr(t, zone["name_unicode"], "zone.name_unicode"); got != accIDNZoneUni {
			t.Errorf("the zone's unicode name = %q, want %q (display, spec 5.2 item 7)", got, accIDNZoneUni)
		}
		if canonical["laravel"] != nil {
			t.Errorf("laravel of a non-Laravel domain = %v, want null", canonical["laravel"])
		}
	})

	t.Run("Laravel signs in the response", func(t *testing.T) {
		got := info("laravel-8-mix." + accZone)
		if got == nil {
			t.Fatal("domain_info(laravel-8-mix) = NULL")
		}
		lar := dinfoMap(t, got["laravel"], "laravel")
		if got := dinfoStr(t, lar["ver_max"], "laravel.ver_max"); got != "9.x" {
			t.Errorf("laravel.ver_max = %q, want 9.x", got)
		}
		findings := dinfoSlice(t, lar["findings"], "laravel.findings")
		if len(findings) != 1 {
			t.Fatalf("findings = %d, want 1", len(findings))
		}
		first := dinfoMap(t, findings[0], "findings[0]")
		if v, ok := first["is_laravel"].(bool); !ok || !v {
			t.Errorf("a confident hit: is_laravel = %v (present=%v), want true", first["is_laravel"], ok)
		}
	})

	t.Run("an unknown domain — a clear answer", func(t *testing.T) {
		// No domain, the zone exists: NULL — the CLI distinguishes "no
		// domain in the zone" (a second light query to zones, T-081) from
		// "the zone is missing". The input — as crawlerctl's: a normalized
		// punycode form.
		n, err := idn.Normalize("Нет-Такого." + accIDNZoneUni)
		if err != nil {
			t.Fatalf("normalize the unknown name: %v", err)
		}
		if got := info(n.ASCII); got != nil {
			t.Errorf("domain_info of a non-existent domain = %v, want NULL", got)
		}
		var zoneExists bool
		if err := env.pool.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM zones WHERE name = $1)", accIDNZone,
		).Scan(&zoneExists); err != nil {
			t.Fatal(err)
		}
		if !zoneExists {
			t.Error("the zone disappeared — \"no domain\" indistinguishable from \"no zone\"")
		}
		// An unknown zone: NULL too, without an error (the input is valid, no data).
		if got := info("site.unknown-zone.test"); got != nil {
			t.Errorf("domain_info in an unknown zone = %v, want NULL", got)
		}
	})
}

// ---------------------------------------------------------------- item 11

// accSampleExpect — the sample's exact expectations are mandatory (the
// T-075 corpus contract): their absence is a clear error, not a
// dereference panic in the cases.
func accSampleExpect(t *testing.T, s rules.Sample) rules.SampleExpect {
	t.Helper()
	if s.Expect.IsLaravel == nil || s.Expect.Score == nil ||
		s.Expect.CalcVerMin == nil || s.Expect.CalcVerMax == nil {
		t.Fatalf("samples/%s/meta.yaml: the is_laravel/score/calc_ver_min/calc_ver_max expectations are required", s.ID)
	}
	return s.Expect
}

// accSampleByID — a customer corpus sample by directory name.
func accSampleByID(t *testing.T, id string) rules.Sample {
	t.Helper()
	if _, err := os.Stat(accSamplesDir); err != nil {
		t.Fatalf("the samples corpus unavailable: %v", err)
	}
	samples, err := rules.LoadSamples(accSamplesDir)
	if err != nil {
		t.Fatalf("the corpus does not load: %v", err)
	}
	for _, s := range samples {
		if s.ID == id {
			accSampleExpect(t, s)
			return s
		}
	}
	t.Fatalf("the sample %s missing from the corpus", id)
	return rules.Sample{}
}

// accSamples — the whole samples corpus (the testbed serves them as
// <id>.crawl.test domains, the T-090/T-073 generator).
func accSamples(t *testing.T) []rules.Sample {
	t.Helper()
	if _, err := os.Stat(accSamplesDir); err != nil {
		t.Fatalf("the samples corpus unavailable: %v", err)
	}
	samples, err := rules.LoadSamples(accSamplesDir)
	if err != nil {
		t.Fatalf("the corpus does not load: %v", err)
	}
	if len(samples) < 13 {
		t.Fatalf("the corpus shrank: %d samples, want at least 13", len(samples))
	}
	for _, s := range samples {
		accSampleExpect(t, s)
	}
	return samples
}

// TestAccept11_CustomerSamplesPipeline — §11.4 item 11: "The analyzer on
// real Laravel samples provided by the customer gives the expected hits
// and version ranges; all hits go into laravel_findings, only those with
// a score not below the threshold go into laravel_domains". The whole
// corpus (the unit engine runs it in T-075; here — the pipeline): the
// samples file through DNS/HTTP/analyze on a live testbed, the
// expectations — each sample's meta.yaml; in laravel_findings — all hits
// including weak ones of negatives, in laravel_domains — only confident
// ones (the 0.6 threshold, §13.1).
func TestAccept11_CustomerSamplesPipeline(t *testing.T) {
	skipUnlessIntegration(t)
	env := newAccEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	samples := accSamples(t)
	labels := make([]string, 0, len(samples))
	for _, s := range samples {
		labels = append(labels, s.ID)
	}
	ids := accPipeline(ctx, t, env, accZone, labels)
	zoneID := accZoneID(t, env.pool, accZone)
	accAnalyze(ctx, t, env, env.analyzer, len(labels))

	var positives, negatives, weakNegatives int
	for _, s := range samples {
		sample, id := s, ids[s.ID]
		name := sample.ID + "." + accZone

		// The domain is checked: alive/done (§7) — the samples answer over
		// both protocols.
		st := httpDomainState(t, env.pool, id)
		if st.status != model.StatusAlive || st.checkState != int16(model.CheckStateDone) {
			t.Errorf("%s: status/check_state = %s/%d, want alive/done", name, st.status, st.checkState)
		}

		// laravel_findings: ALL hits (§13.1) — the composition, score,
		// the computed version range, the set version.
		fs := accFindings(t, env.pool, zoneID, id)
		switch {
		case len(sample.Expect.Rules) == 0:
			if len(fs) != 0 {
				t.Errorf("%s: a clean negative, findings = %d rows", name, len(fs))
			}
		case len(fs) != 1:
			t.Errorf("%s: findings = %d rows, want 1 (the apex, variant 0)", name, len(fs))
		default:
			f := fs[0]
			if f.sub != 0 || f.variant != int16(model.VariantMain) {
				t.Errorf("%s: the hit's subdomain/variant = %d/%d, want 0/0", name, f.sub, f.variant)
			}
			if f.rulesVersion != env.rules.Version {
				t.Errorf("%s: rules_version = %d, want %d (the set version)", name, f.rulesVersion, env.rules.Version)
			}
			if math.Abs(float64(f.score)-*sample.Expect.Score) > accScoreTol {
				t.Errorf("%s: score = %.4f, want ≈%.3f", name, f.score, *sample.Expect.Score)
			}
			if got, want := accStr(f.verMin), accStr(sample.Expect.CalcVerMin); got != want {
				t.Errorf("%s: ver_min = %q, want %q", name, got, want)
			}
			if got, want := accStr(f.verMax), accStr(sample.Expect.CalcVerMax); got != want {
				t.Errorf("%s: ver_max = %q, want %q", name, got, want)
			}
			if f.isLaravel != *sample.Expect.IsLaravel {
				t.Errorf("%s: is_laravel = %v, want %v (the 0.6 threshold)", name, f.isLaravel, *sample.Expect.IsLaravel)
			}
			fired := accFiredRules(t, f.signals)
			if len(fired) != len(sample.Expect.Rules) {
				t.Errorf("%s: %d fired rules, want %d (%v)", name, len(fired), len(sample.Expect.Rules), fired)
			}
			for _, r := range sample.Expect.Rules {
				if !fired[r] {
					t.Errorf("%s: the rule %s did not fire (fired %v)", name, r, fired)
				}
			}
		}

		// laravel_domains: only confident ones (score ≥ the threshold); the
		// aggregate of a domain with a single row matches the hit,
		// found_on_mask — the subdomain bit (§5.8). The counters below are
		// the corpus composition per meta.yaml (the expectations); the
		// divergences from the DB are already flagged by the errors above.
		agg, ok := accLaravelDomain(t, env.pool, zoneID, id)
		if !*sample.Expect.IsLaravel {
			if ok {
				t.Errorf("%s: a negative sample in laravel_domains (score %.3f)", name, agg.score)
			}
			negatives++
			if len(sample.Expect.Rules) > 0 {
				weakNegatives++ // the weak hits stayed in findings
			}
			continue
		}
		positives++
		if !ok {
			t.Errorf("%s: a confident sample without a laravel_domains row", name)
			continue
		}
		if math.Abs(float64(agg.score)-*sample.Expect.Score) > accScoreTol {
			t.Errorf("%s: the aggregate score = %.4f, want ≈%.3f", name, agg.score, *sample.Expect.Score)
		}
		if got, want := accStr(agg.verMin), accStr(sample.Expect.CalcVerMin); got != want {
			t.Errorf("%s: the aggregate ver_min = %q, want %q", name, got, want)
		}
		if got, want := accStr(agg.verMax), accStr(sample.Expect.CalcVerMax); got != want {
			t.Errorf("%s: the aggregate ver_max = %q, want %q", name, got, want)
		}
		if agg.mask != 0b1 {
			t.Errorf("%s: found_on_mask = %b, want bit 0 (the apex)", name, agg.mask)
		}
		if agg.rulesV != env.rules.Version {
			t.Errorf("%s: the aggregate rules_version = %d, want %d", name, agg.rulesV, env.rules.Version)
		}
	}

	// The corpus integrity for acceptance: positives and negatives with
	// "weak" hits (the false-positive control) must be present (§13.3, the
	// T-075 corpus).
	if positives < 8 {
		t.Errorf("%d positive samples, want at least 8 (Laravel major versions)", positives)
	}
	if negatives < 4 || weakNegatives < 2 {
		t.Errorf("%d negatives (%d weak), want at least 4/2", negatives, weakNegatives)
	}
}

// An end-to-end integration test of the pipeline (T-091; spec 11.2 "End-to-end
// run: a file in incoming → DB rows → results → laravel_*", acceptance
// criterion 11.4 item 5; resilience to a PostgreSQL restart — spec 11.2
// "Resilience", the essence of criterion 11.4 item 9 for the DB).
//
// The level is library calls of the components of all stages against the live
// stack (PostgreSQL, Redis, unbound → coredns, testweb), without starting
// service processes: every stage runs the same code as the cmd/* builds —
// file → ingest.Pipeline → scheduler (DispatchBatch/EnqueueBatch,
// q:dns) → DNS episodes (dnsworker.ProcessDomain, q:http) → HTTP episodes
// (httpworker.ProcessDomain + ContentSaver + Recorder → subdomain_results,
// q:analyze) → analyzer.ProcessResult → laravel_findings/laravel_domains.
// The former HTTP-stage e2e minimum (TestHTTPWorker_E2E, T-060) is not
// duplicated: here it is a link of the full chain; the analysis expectations
// are checked against the testdata/laravel-samples samples (meta.yaml, the
// same cross-check as in the T-075 corpus): the testbed serves the samples
// with the same headers/cookies/index.
//
// A PostgreSQL restart in the middle of the pipeline: the DB container
// belongs to the shared dev stack — restarting the instance from a test is
// impossible (it would hit parallel runs and running services). The
// emulation matches §11.2 in essence: from the clients' point of view a
// restart is the loss of all connections with the data surviving.
// pg_terminate_backend by the application_name label (the test pool's DSN)
// closes the sockets of the test's connections, pgxpool replaces them with
// new ones on the next use — the pipeline continues from the same place and
// the totals converge. Permissions: a role may terminate its own backends
// (verified on the dev stack); the connections of other services (unlabeled)
// are not touched. Redis loss and a worker stopping in the middle of a batch
// — the territory of resilience_test.go (T-032).
//
// Isolation — as with the neighbors: an isolated schema with migrations
// (withTestSchema), Redis DB 15 (resilienceRedisEnv) with q:* cleanup
// before/after, incoming and the content directory — t.TempDir.
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"crawler/internal/analyzer"
	"crawler/internal/content"
	"crawler/internal/dnsclient"
	"crawler/internal/dnsworker"
	"crawler/internal/httpfetch"
	"crawler/internal/httpworker"
	"crawler/internal/ingest"
	"crawler/internal/migrate"
	"crawler/internal/model"
	"crawler/internal/queue"
	"crawler/internal/rules"
	"crawler/internal/scheduler"
)

// e2eAppName marks the test pool's connections in pg_stat_activity — the
// target of the disconnect in the PostgreSQL restart emulation (see the
// file comment).
const e2eAppName = "it-e2e-t091"

// The stream consumer groups are unique to the test: Redis DB 15 is cleaned
// up before/after, but the names do not clash with neighboring tests and
// repeated runs in one environment.
const (
	e2eDNSGroup       = "it-e2e-dns"
	e2eHTTPGroup      = "it-e2e-http"
	e2eAnalyzeGroup   = "it-e2e-an"
	e2eZone           = "crawl.test"
	e2eScoreTolerance = 0.001 // float4 in the DB (like the T-075 corpus tolerance)
)

// e2eSchema — an isolated schema with migrations (like migratedSchema), but
// the pool is labeled application_name: the test's connections are
// addressable in pg_stat_activity for the PostgreSQL restart emulation.
func e2eSchema(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn, cleanup := withTestSchema(t)
	t.Cleanup(cleanup)
	dsn += "&application_name=" + e2eAppName
	if err := migrate.Up(context.Background(), dsn); err != nil {
		t.Fatalf("применение миграций к изолированной схеме: %v", err)
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("подключение к тестовой схеме: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// e2eEnv — the pipeline of all stages on live PostgreSQL/Redis/testbed: the
// same components and wiring as in the cmd/* builds (the fetcher — as in
// httpWorkerSetup, T-060; the analyzer — a rules Reloader, as in
// cmd/analyzer).
type e2eEnv struct {
	pool     *pgxpool.Pool
	rdb      *redis.Client
	q        *queue.RedisQueue
	resolve  dnsclient.Resolver
	store    *content.PackStore
	saver    *httpworker.ContentSaver
	recorder *httpworker.Recorder
	hworker  *httpworker.Worker
	analyzer *analyzer.Worker
	rules    *rules.Set
}

func newE2EEnv(t *testing.T) *e2eEnv {
	t.Helper()
	pool := e2eSchema(t)
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

	// The stage transport — as in the cmd/http-worker build: redirect hosts
	// are resolved by the same resolver as the DNS stage (spec 5.5 item 2).
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

	// The rule set is the working rules/. Priority: APP_RULES_DIR (when the
	// directory exists — the service containers), otherwise the path from
	// the test directory to the repository root (tools mounts the rules at
	// /rules, and the go-common-inherited
	// APP_RULES_DIR=/etc/crawler/rules is not mounted in tools).
	rulesDir := os.Getenv("APP_RULES_DIR")
	if dir := rulesDir; dir != "" {
		if _, err := os.Stat(dir); err != nil {
			rulesDir = ""
		}
	}
	if rulesDir == "" {
		rulesDir = "../../rules"
	}
	loader, err := rules.NewReloader(rulesDir, nil)
	if err != nil {
		t.Fatalf("набор правил %s: %v", rulesDir, err)
	}
	an, err := analyzer.New(pool, q, store, loader, analyzer.Options{
		MinScore: rules.DefaultMinScore,
	})
	if err != nil {
		t.Fatal(err)
	}

	return &e2eEnv{
		pool: pool, rdb: rdb, q: q, resolve: resolve,
		store: store, saver: saver, recorder: recorder,
		hworker: hworker, analyzer: an, rules: loader.Set(),
	}
}

// e2eSample — the expectations of the testdata/laravel-samples/<label>
// sample (meta.yaml; the testbed serves the samples as <label>.crawl.test
// domains): score — the noisy-OR of the fired rule weights, verMin/verMax —
// the exact strings of the computed range ("" — no bound, NULL in the DB),
// rules — the full set of hits, title — the page content marker.
type e2eSample struct {
	label     string
	title     string
	isLaravel bool
	score     float64
	verMin    string
	verMax    string
	rules     []string
}

// e2eSamples — the samples connected by the testbed generator in master:
// two positive Laravel and two negative ones (a weak symfony hit and a
// clean django) — the negatives pin the is_laravel threshold (§13.1).
var e2eSamples = []e2eSample{
	{
		label: "laravel-11-vite", title: "Laravel Vite App",
		isLaravel: true, score: 0.894, verMin: "9.19", verMax: "",
		rules: []string{
			"cookie_xsrf_token", "cookie_laravel_session", "cookie_session_suffix",
			"cookie_encrypted_payload", "meta_csrf_token", "vite_assets", "inertia",
		},
	},
	{
		label: "laravel-8-mix", title: "Laravel",
		isLaravel: true, score: 0.859, verMin: "", verMax: "9.x",
		rules: []string{
			"cookie_xsrf_token", "cookie_laravel_session", "cookie_session_suffix",
			"cookie_encrypted_payload", "meta_csrf_token", "mix_assets",
		},
	},
	{
		label: "symfony-negative", title: "Welcome!",
		isLaravel: false, score: 0.2,
		rules: []string{"meta_csrf_token"},
	},
	{
		label: "django-negative", title: "Welcome to Django",
		isLaravel: false, score: 0,
		rules: nil,
	},
}

// e2eBreakPostgresConnections — the PostgreSQL restart emulation (spec
// 11.2): drops all test connections (the application_name label) except the
// executing one. Returns the number of dropped backends. The one-off
// "killer" connection without a label does not hit the filter itself; the
// connections of the running stack services (unlabeled) are not touched.
func e2eBreakPostgresConnections(t *testing.T) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgDSN(t))
	if err != nil {
		t.Fatalf("подключение для обрыва соединений: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var killed int
	if err := conn.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE pg_terminate_backend(pid)) FROM pg_stat_activity
		WHERE application_name = $1 AND pid <> pg_backend_pid()`, e2eAppName,
	).Scan(&killed); err != nil {
		t.Fatalf("обрыв соединений (pg_terminate_backend): %v", err)
	}
	return killed
}

// e2eWaitPoolRecovery waits for the pool to reconnect after the drop: dead
// connections are released by failed uses, new ones are created lazily
// (pgxpool) — the ping loop until the first success is "reconnection after
// the restart" from the client's point of view.
func e2eWaitPoolRecovery(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := pool.Ping(ctx)
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("пул не переподключился после обрыва соединений: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// e2eSignal — a signals jsonb row (contract T-071).
type e2eSignal struct {
	Rule string `json:"rule"`
}

// e2eHash — a content.Hash ([16]byte) from a slice read from the DB.
func e2eHash(b []byte) content.Hash {
	var h content.Hash
	copy(h[:], b)
	return h
}

// e2eStr — a string or NULL → "".
func e2eStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// TestE2E_FileToLaravel — the full pipeline "a file in incoming →
// laravel_*" (spec 11.2, criterion 11.4 item 5) with a PostgreSQL restart
// in the middle (between the DNS and HTTP stages): after the connection
// drop the pipeline finishes the episodes, and the DB totals converge with
// the sample expectations.
func TestE2E_FileToLaravel(t *testing.T) {
	skipUnlessIntegration(t)
	env := newE2EEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// ---------------------------------------------------------------- input
	// A real file in incoming (spec 5.1): the zone is created
	// automatically, the lines are domains; the pipeline is the same as in
	// cmd/ingest.
	dir := t.TempDir()
	pipe, err := ingest.NewPipeline(
		ingest.NewZoneStore(env.pool),
		ingest.NewFileStore(env.pool),
		env.pool,
		ingest.PipelineOptions{Dir: dir, BatchSize: 4, AllowedZonePaths: []string{e2eZone}},
	)
	if err != nil {
		t.Fatalf("ingest.NewPipeline: %v", err)
	}
	file := filepath.Join(dir, e2eZone, "add", "e2e.txt")
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		t.Fatal(err)
	}
	lines := make([]string, 0, len(e2eSamples)+2)
	lines = append(lines, "# сквозной прогон T-091: образцы Laravel и негативные")
	for _, s := range e2eSamples {
		lines = append(lines, s.label+"."+e2eZone)
	}
	lines = append(lines, "") // an empty line — skipped by the parser
	if err := os.WriteFile(file, []byte(fileContent(lines...)), 0o644); err != nil {
		t.Fatal(err)
	}

	// zoneID is filled by the ingest subtest (the zone is created when the
	// file is applied) — the following stages use it for queries.
	var zoneID int16

	t.Run("ingest: файл → зоны, домены, отчёт", func(t *testing.T) {
		out, err := pipe.Process(ctx, file)
		if err != nil {
			t.Fatalf("конвейер ingest: %v", err)
		}
		if out.Kind != ingest.OutcomeApplied {
			t.Fatalf("итог файла = %q, хочу applied", out.Kind)
		}
		if !existsOnDisk(t, filepath.Join(dir, "processed", e2eZone, "add", "e2e.txt")) {
			t.Error("файл не перемещён в processed/")
		}

		if n := countDomains(t, env.pool, ""); n != len(e2eSamples) {
			t.Fatalf("доменов в БД: %d, хочу %d", n, len(e2eSamples))
		}
		for _, s := range e2eSamples {
			d, ok := env.domainByLabel(t, e2eZone, s.label)
			if !ok {
				t.Fatalf("домен %s.%s не создан", s.label, e2eZone)
			}
			if d.status != int16(model.StatusNew) || d.checkState != int16(model.CheckStatePending) {
				t.Errorf("%s: status/check_state = %d/%d, хочу new/pending", s.label, d.status, d.checkState)
			}
		}

		if err := env.pool.QueryRow(ctx,
			"SELECT id FROM zones WHERE name = $1", e2eZone).Scan(&zoneID); err != nil {
			t.Fatalf("зона %s: %v", e2eZone, err)
		}
		var linesTotal, added, rejected int64
		if err := env.pool.QueryRow(ctx,
			"SELECT lines_total, added, rejected FROM ingest_files WHERE zone_id = $1", zoneID,
		).Scan(&linesTotal, &added, &rejected); err != nil {
			t.Fatalf("чтение ingest_files: %v", err)
		}
		// 6 physical lines: a comment, 4 domains, an empty one; 4 applied.
		if linesTotal != 6 || added != int64(len(e2eSamples)) || rejected != 0 {
			t.Errorf("ingest_files: lines=%d added=%d rejected=%d, хочу 6/%d/0",
				linesTotal, added, len(e2eSamples), rejected)
		}
		var staging int
		if err := env.pool.QueryRow(ctx, "SELECT count(*) FROM ingest_stage").Scan(&staging); err != nil {
			t.Fatalf("чтение ingest_stage: %v", err)
		}
		if staging != 0 {
			t.Errorf("staging не очищена: %d строк", staging)
		}
	})

	t.Run("scheduler: pending → queued + пачка q:dns", func(t *testing.T) {
		batch, err := scheduler.DispatchBatch(ctx, env.pool, scheduler.DispatchQuery{Limit: 10})
		if err != nil {
			t.Fatalf("выборка пачки: %v", err)
		}
		if len(batch) != len(e2eSamples) {
			t.Fatalf("выбрано доменов: %d, хочу %d", len(batch), len(e2eSamples))
		}
		res, err := scheduler.EnqueueBatch(ctx, env.pool, env.q, batch)
		if err != nil {
			t.Fatalf("постановка пачки: %v", err)
		}
		if res.Queued != len(e2eSamples) || res.Published != len(e2eSamples) {
			t.Fatalf("постановка: queued=%d published=%d, хочу %d/%d",
				res.Queued, res.Published, len(e2eSamples), len(e2eSamples))
		}
		if n := countDomains(t, env.pool, stateWhere(model.CheckStateQueued)); n != len(e2eSamples) {
			t.Errorf("queued после постановки: %d, хочу %d", n, len(e2eSamples))
		}
		if n := streamLen(t, env.rdb, queue.StreamDNS); n != 1 {
			t.Errorf("записей в q:dns: %d, хочу 1 (одна пачка)", n)
		}
	})

	t.Run("DNS-этап: q:dns → записи доменов и задачи q:http", func(t *testing.T) {
		deliveries := consumeTasks(t, env.q, queue.StreamDNS, e2eDNSGroup, "e2e", 10)
		if len(deliveries) != 1 {
			t.Fatalf("доставок q:dns: %d, хочу 1", len(deliveries))
		}
		// The batch — as the worker's processBatch (T-039): the subdomain
		// reference table → the freshness filter → episodes; XACK after the
		// write (Processing).
		err := queue.Processing(ctx, env.q, e2eDNSGroup, deliveries[0],
			func(c context.Context, d queue.Delivery) error {
				b, err := queue.Decode[queue.DNSBatch](d)
				if err != nil {
					return err
				}
				if len(b.Domains) != len(e2eSamples) {
					return fmt.Errorf("в пачке %d доменов, хочу %d", len(b.Domains), len(e2eSamples))
				}
				subs, err := dnsworker.LoadSubdomains(c, env.pool)
				if err != nil {
					return err
				}
				pending, err := dnsworker.PendingDomains(c, env.pool, b.Domains)
				if err != nil {
					return err
				}
				if len(pending) != len(e2eSamples) {
					return fmt.Errorf("актуальных доменов: %d, хочу %d", len(pending), len(e2eSamples))
				}
				for _, dom := range pending {
					if err := dnsworker.ProcessDomain(c, env.pool, env.q, env.resolve, dom, subs, true, nil, nil); err != nil {
						return err
					}
				}
				return nil
			})
		if err != nil {
			t.Fatalf("пачка q:dns: %v", err)
		}

		// All episodes are finished: done, the lease is reset, the mask is
		// the apex only (the samples have no www/admin/… records, §5.4
		// item 4); no terminal status — the verdict belongs to the HTTP
		// stage (§7).
		for _, s := range e2eSamples {
			var (
				status   model.Status
				mask     int64
				queuedAt *time.Time
				lastAt   *time.Time
			)
			if err := env.pool.QueryRow(ctx,
				"SELECT status, dns_mask, queued_at, last_checked_at FROM domains WHERE id = $1",
				env.domainID(t, zoneID, s.label),
			).Scan(&status, &mask, &queuedAt, &lastAt); err != nil {
				t.Fatalf("домен %s: %v", s.label, err)
			}
			if status != model.StatusNew {
				t.Errorf("%s: status = %s, хочу new (итог за HTTP-этапом)", s.label, status)
			}
			if mask != 0b1 {
				t.Errorf("%s: dns_mask = %b, хочу бит 0 (апекс)", s.label, mask)
			}
			if queuedAt != nil || lastAt == nil {
				t.Errorf("%s: queued_at=%v last_checked_at=%v", s.label, queuedAt, lastAt)
			}
		}
		if n := countDomains(t, env.pool, stateWhere(model.CheckStateDone)); n != len(e2eSamples) {
			t.Fatalf("done после DNS-этапа: %d, хочу %d", n, len(e2eSamples))
		}
		if n := streamLen(t, env.rdb, queue.StreamHTTP); n != int64(len(e2eSamples)) {
			t.Fatalf("задач в q:http: %d, хочу %d (по одной на домен)", n, len(e2eSamples))
		}
	})

	t.Run("перезапуск PostgreSQL посреди конвейера", func(t *testing.T) {
		// Guarantee the pool ≥2 live connections at the moment of the drop:
		// two parallel queries hold two physical connections and return
		// them to the pool alive.
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := env.pool.Exec(ctx, "SELECT pg_sleep(1)"); err != nil {
					t.Errorf("прогрев соединений пула: %v", err)
				}
			}()
		}
		wg.Wait()

		killed := e2eBreakPostgresConnections(t)
		if killed < 2 {
			t.Fatalf("оборвано соединений: %d, хочу ≥ 2 (два параллельных запроса перед обрывом)", killed)
		}

		// The client reconnects: dead connections are released, the pool
		// lazily creates new ones (the essence of resilience, §11.4 item 9).
		e2eWaitPoolRecovery(t, env.pool)

		// The data and the queue survived the failure: the pipeline
		// continues from the same place (PostgreSQL is the source of truth,
		// spec 3.4; Redis is untouched).
		if n := countDomains(t, env.pool, ""); n != len(e2eSamples) {
			t.Errorf("доменов после обрыва: %d, хочу %d", n, len(e2eSamples))
		}
		if n := countDomains(t, env.pool, stateWhere(model.CheckStateDone)); n != len(e2eSamples) {
			t.Errorf("done после обрыва: %d, хочу %d (результаты DNS не потеряны)", n, len(e2eSamples))
		}
		if n := streamLen(t, env.rdb, queue.StreamHTTP); n != int64(len(e2eSamples)) {
			t.Errorf("задач q:http после обрыва: %d, хочу %d", n, len(e2eSamples))
		}
	})

	t.Run("HTTP-этап после сбоя: q:http → subdomain_results + q:analyze", func(t *testing.T) {
		tasks := consumeTasks(t, env.q, queue.StreamHTTP, e2eHTTPGroup, "e2e", 10)
		if len(tasks) != len(e2eSamples) {
			t.Fatalf("задач q:http: %d, хочу %d", len(tasks), len(e2eSamples))
		}
		// The episode — as the worker's processDelivery (T-053):
		// ProcessDomain → ContentSaver (T-056) → Recorder (T-058) → XACK.
		for _, d := range tasks {
			err := queue.Processing(ctx, env.q, e2eHTTPGroup, d,
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
						return fmt.Errorf("домен %s: результат не записан (охрана записи, T-058)", task.Name)
					}
					return nil
				})
			if err != nil {
				t.Fatalf("HTTP-эпизод: %v", err)
			}
		}

		for _, s := range e2eSamples {
			id := env.domainID(t, zoneID, s.label)
			name := s.label + "." + e2eZone

			// The domain is alive/done (§7), the lease is finished.
			st := httpDomainState(t, env.pool, id)
			if st.status != model.StatusAlive {
				t.Errorf("%s: status = %s, хочу alive", name, st.status)
			}
			if st.checkState != int16(model.CheckStateDone) || st.queuedAt != nil || st.tempAttempts != 0 {
				t.Errorf("%s: check_state=%d queued_at=%v temp_attempts=%d",
					name, st.checkState, st.queuedAt, st.tempAttempts)
			}

			// The samples respond identically over http and https → a single
			// variant 0 https row (spec 5.5 item 5), the content is stored.
			rows := httpRows(t, env.pool, id)
			if len(rows) != 1 {
				t.Fatalf("%s: строк subdomain_results = %d, хочу 1", name, len(rows))
			}
			r := rows[0]
			if r.subdomainID != 0 || r.variant != int16(model.VariantMain) || r.scheme != int16(model.SchemeHTTPS) {
				t.Errorf("%s: строка %d/%d/%d, хочу апекс/0/https", name, r.subdomainID, r.variant, r.scheme)
			}
			if r.httpStatus != 200 || r.finalURL != "https://"+name+"/" || r.errCode != nil {
				t.Errorf("%s: статус/URL/ошибка = %d/%q/%v", name, r.httpStatus, r.finalURL, r.errCode)
			}
			if r.aliasKind != int16(model.AliasNone) || r.aliasSubdomain != nil {
				t.Errorf("%s: алиас = %d/%v, хочу нет", name, r.aliasKind, r.aliasSubdomain)
			}
			if len(r.contentHash) != 16 {
				t.Fatalf("%s: content_hash = %d байт, хочу 16", name, len(r.contentHash))
			}
			body, err := env.store.Get(ctx, e2eHash(r.contentHash))
			if err != nil {
				t.Fatalf("%s: чтение контента из хранилища: %v", name, err)
			}
			if !strings.Contains(string(body), s.title) {
				t.Errorf("%s: контент без маркера %q: %.80q", name, s.title, body)
			}
		}

		// Publishing to q:analyze — one Result per subdomain×variant row
		// (T-058); non-SPA samples do not create render tasks (T-066).
		if n := streamLen(t, env.rdb, queue.StreamAnalyze); n != int64(len(e2eSamples)) {
			t.Errorf("записей в q:analyze: %d, хочу %d", n, len(e2eSamples))
		}
		if n := streamLen(t, env.rdb, queue.StreamRender); n != 0 {
			t.Errorf("записей в q:render: %d, хочу 0 (образцы без SPA-оболочек)", n)
		}
	})

	t.Run("analyzer: q:analyze → laravel_findings/laravel_domains", func(t *testing.T) {
		msgs := consumeTasks(t, env.q, queue.StreamAnalyze, e2eAnalyzeGroup, "e2e", 10)
		if len(msgs) != len(e2eSamples) {
			t.Fatalf("доставок q:analyze: %d, хочу %d", len(msgs), len(e2eSamples))
		}
		for _, d := range msgs {
			err := queue.Processing(ctx, env.q, e2eAnalyzeGroup, d,
				func(c context.Context, del queue.Delivery) error {
					res, err := queue.Decode[queue.Result](del)
					if err != nil {
						return err
					}
					processed, err := env.analyzer.ProcessResult(c, res)
					if err != nil {
						return err
					}
					if !processed {
						return fmt.Errorf("строка %d/%d/%d/%d не найдена или перезаписана — результат HTTP-этапа не записан",
							res.ZoneID, res.DomainID, res.SubdomainID, int16(res.Variant))
					}
					return nil
				})
			if err != nil {
				t.Fatalf("анализ: %v", err)
			}
		}
	})

	t.Run("laravel_*: список, субдомены, диапазон, признаки (§11.4 п. 5)", func(t *testing.T) {
		// The list of Laravel domains — a select from laravel_domains (the
		// §13.1 threshold: only confident hits).
		rows, err := env.pool.Query(ctx, `
			SELECT d.label FROM laravel_domains AS ld
			JOIN domains AS d ON d.zone_id = ld.zone_id AND d.id = ld.domain_id
			WHERE d.zone_id = $1 ORDER BY d.label`, zoneID)
		if err != nil {
			t.Fatalf("список доменов с Laravel: %v", err)
		}
		defer rows.Close()
		var withLaravel []string
		for rows.Next() {
			var label string
			if err := rows.Scan(&label); err != nil {
				t.Fatalf("чтение списка: %v", err)
			}
			withLaravel = append(withLaravel, label)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("список доменов с Laravel: %v", err)
		}
		var want []string
		for _, s := range e2eSamples {
			if s.isLaravel {
				want = append(want, s.label)
			}
		}
		if len(withLaravel) != len(want) {
			t.Errorf("домены с Laravel = %v, хочу %v", withLaravel, want)
		} else {
			for _, label := range want {
				found := false
				for _, got := range withLaravel {
					if got == label {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("в списке доменов с Laravel нет %s (получено %v)", label, withLaravel)
				}
			}
		}

		// The appeared detection history (T-118): this circle's q:analyze
		// deliveries are fresh (the HTTP stage) — every Laravel domain got
		// exactly one appeared=true row; no lost ones.
		{
			rows, err := env.pool.Query(ctx, `
				SELECT d.label, h.appeared FROM laravel_history AS h
				JOIN domains AS d ON d.zone_id = h.zone_id AND d.id = h.domain_id
				WHERE h.zone_id = $1 ORDER BY d.label`, zoneID)
			if err != nil {
				t.Fatalf("чтение laravel_history: %v", err)
			}
			defer rows.Close()
			type histRow struct {
				label    string
				appeared bool
			}
			var hist []histRow
			for rows.Next() {
				var h histRow
				if err := rows.Scan(&h.label, &h.appeared); err != nil {
					t.Fatalf("строка laravel_history: %v", err)
				}
				hist = append(hist, h)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("чтение laravel_history: %v", err)
			}
			if len(hist) != len(want) {
				t.Fatalf("laravel_history = %d записей, хочу %d (по одной appeared на Laravel-домен)", len(hist), len(want))
			}
			for _, h := range hist {
				if !h.appeared {
					t.Errorf("laravel_history %s: appeared=false, хочу true (потерь в цикле нет)", h.label)
				}
			}
		}

		// e2eFinding — a laravel_findings row for the checks.
		type e2eFinding struct {
			sub          int16
			variant      int16
			score        float32
			verMin       *string
			verMax       *string
			isLaravel    bool
			signals      string
			rulesVersion int32
		}
		for _, s := range e2eSamples {
			id := env.domainID(t, zoneID, s.label)
			name := s.label + "." + e2eZone

			// laravel_findings: all hits regardless of the threshold
			// (§13.1) — the composition, score, range, set version.
			frows, err := env.pool.Query(ctx, `
				SELECT subdomain_id, variant, score, ver_min, ver_max, is_laravel, signals::text, rules_version
				FROM laravel_findings WHERE zone_id = $1 AND domain_id = $2`, zoneID, id)
			if err != nil {
				t.Fatalf("%s: чтение laravel_findings: %v", name, err)
			}
			var findings []e2eFinding
			for frows.Next() {
				var f e2eFinding
				if err := frows.Scan(&f.sub, &f.variant, &f.score, &f.verMin, &f.verMax,
					&f.isLaravel, &f.signals, &f.rulesVersion); err != nil {
					t.Fatalf("%s: строка laravel_findings: %v", name, err)
				}
				findings = append(findings, f)
			}
			if err := frows.Err(); err != nil {
				t.Fatalf("%s: чтение laravel_findings: %v", name, err)
			}
			frows.Close()
			if len(s.rules) == 0 {
				if len(findings) != 0 {
					t.Errorf("%s: негативный образец без срабатываний, findings = %d строк", name, len(findings))
				}
			} else {
				if len(findings) != 1 {
					t.Fatalf("%s: findings = %d строк, хочу 1 (апекс, variant 0)", name, len(findings))
				}
				f := findings[0]
				if f.sub != 0 || f.variant != int16(model.VariantMain) {
					t.Errorf("%s: срабатывание субдомена/варианта %d/%d, хочу 0/0 (апекс, variant 0)", name, f.sub, f.variant)
				}
				if f.rulesVersion != env.rules.Version {
					t.Errorf("%s: rules_version срабатывания = %d, хочу %d (версия набора)", name, f.rulesVersion, env.rules.Version)
				}
				if math.Abs(float64(f.score)-s.score) > e2eScoreTolerance {
					t.Errorf("%s: score = %.4f, хочу ≈%.3f", name, f.score, s.score)
				}
				if got := e2eStr(f.verMin); got != s.verMin {
					t.Errorf("%s: ver_min = %q, хочу %q", name, got, s.verMin)
				}
				if got := e2eStr(f.verMax); got != s.verMax {
					t.Errorf("%s: ver_max = %q, хочу %q", name, got, s.verMax)
				}
				if f.isLaravel != s.isLaravel {
					t.Errorf("%s: is_laravel = %v, хочу %v (порог 0.6)", name, f.isLaravel, s.isLaravel)
				}
				var sigs []e2eSignal
				if err := json.Unmarshal([]byte(f.signals), &sigs); err != nil {
					t.Fatalf("%s: signals jsonb: %v", name, err)
				}
				gotRules := make(map[string]bool, len(sigs))
				for _, sig := range sigs {
					gotRules[sig.Rule] = true
				}
				if len(gotRules) != len(s.rules) {
					t.Errorf("%s: срабатываний %v, хочу %v", name, gotRules, s.rules)
				}
				for _, r := range s.rules {
					if !gotRules[r] {
						t.Errorf("%s: срабатывание %s отсутствует (признаки = %v)", name, r, gotRules)
					}
				}
			}

			// laravel_domains — only confident ones (the §13.1 threshold):
			// for a domain with a single row the aggregate matches the hit;
			// found_on_mask — the subdomain bit (§5.8). Negative samples
			// (including clean django) have no row at all.
			var (
				aggScore float32
				aggMin   *string
				aggMax   *string
				aggMask  int64
				aggVer   int32
			)
			err = env.pool.QueryRow(ctx, `
				SELECT score, ver_min, ver_max, found_on_mask, rules_version
				FROM laravel_domains WHERE zone_id = $1 AND domain_id = $2`, zoneID, id,
			).Scan(&aggScore, &aggMin, &aggMax, &aggMask, &aggVer)
			if !s.isLaravel {
				if !errors.Is(err, pgx.ErrNoRows) {
					t.Errorf("%s: негативный образец: чтение laravel_domains = %v, хочу отсутствие строки (ErrNoRows)", name, err)
				}
				continue
			}
			if err != nil {
				t.Fatalf("%s: чтение laravel_domains: %v", name, err)
			}
			if math.Abs(float64(aggScore)-s.score) > e2eScoreTolerance {
				t.Errorf("%s: агрегат score = %.4f, хочу ≈%.3f", name, aggScore, s.score)
			}
			if got := e2eStr(aggMin); got != s.verMin {
				t.Errorf("%s: агрегат ver_min = %q, хочу %q", name, got, s.verMin)
			}
			if got := e2eStr(aggMax); got != s.verMax {
				t.Errorf("%s: агрегат ver_max = %q, хочу %q", name, got, s.verMax)
			}
			if aggMask != 0b1 {
				t.Errorf("%s: found_on_mask = %b, хочу бит 0 (апекс)", name, aggMask)
			}
			if aggVer != env.rules.Version {
				t.Errorf("%s: rules_version = %d, хочу %d (версия набора)", name, aggVer, env.rules.Version)
			}
		}

		// The re-analysis stamp: all rows are analyzed by the current set
		// (the T-071/T-074 contract).
		var untagged int
		if err := env.pool.QueryRow(ctx,
			"SELECT count(*) FROM subdomain_results WHERE rules_version IS DISTINCT FROM $1",
			env.rules.Version).Scan(&untagged); err != nil {
			t.Fatalf("подсчёт нештампованных строк: %v", err)
		}
		if untagged != 0 {
			t.Errorf("строк без rules_version текущего набора: %d, хочу 0", untagged)
		}
	})
}

// domainByLabel — the observed domain columns by the zone's punycode name
// and label (a wrapper over the query, the ingestEnv.domain style).
func (e *e2eEnv) domainByLabel(t *testing.T, zone, label string) (domainState, bool) {
	t.Helper()
	var d domainState
	err := e.pool.QueryRow(context.Background(), `
		SELECT d.label, d.status, d.check_state, d.flags, d.queued_at
		FROM domains AS d JOIN zones AS z ON z.id = d.zone_id
		WHERE z.name = $1 AND d.label = $2`, zone, label,
	).Scan(&d.label, &d.status, &d.checkState, &d.flags, &d.queuedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return d, false
	}
	if err != nil {
		t.Fatalf("чтение домена %s.%s: %v", label, zone, err)
	}
	return d, true
}

// domainID — the domain id by label in the test zone (called from subtests —
// t is passed as a parameter, Fatalf addresses the calling subtest).
func (e *e2eEnv) domainID(t *testing.T, zoneID int16, label string) int64 {
	t.Helper()
	var id int64
	if err := e.pool.QueryRow(context.Background(),
		"SELECT id FROM domains WHERE zone_id = $1 AND label = $2", zoneID, label,
	).Scan(&id); err != nil {
		t.Fatalf("домен %s: %v", label, err)
	}
	return id
}

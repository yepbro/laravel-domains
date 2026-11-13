// Integration tests of the HTTP stage on the test bench (T-060; spec 11.2,
// §8, 5.5): stage 5 components against the live testweb (Caddy) and
// mockserver stack ("nasty" responses, scenario virtual hosts of
// scenarios.yaml).
//
// The level is pipeline components, not the raw protocol:
// TestHTTP_TestwebScenarios and TestHTTP_MockserverEvil (env_test.go) have
// already pinned the fixtures and endpoints by net/http exchanges; here the
// same scenarios are run through the httpfetch transport (limits and
// anti-tarpit T-049, redirect chains T-048, TLS attributes T-050, capped
// decompression T-051) and the httpworker worker (ProcessDomain +
// ContentSaver T-056 + Recorder T-058 — the error classification of §8,
// variants/aliases T-054/T-055, the SPA verdict T-057).
//
// The end-to-end minimum (as T-040 for DNS): q:http tasks for the same-site
// and bare→www scenarios through live PostgreSQL/Redis: subdomain_results
// rows with variants and aliases, publishing to q:analyze, contents refcount
// on redelivery. Isolation — the migratedSchema schema and Redis DB 15
// (resilienceRedisEnv) with q:* cleanup before/after.
//
// Test time is seconds: transport timeouts are overridden with short values
// (the spec 5.5 limit table is configurable), bench answers are
// milliseconds; the slow branches (tarpit ~5 s, black hole ~3 s) are
// deliberate classification expectations, not background delays.
package integration

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/httpfetch"
	"crawler/internal/httpworker"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// httpBlackholeIP — the bench's black hole address (closed-ports): the TCP
// dial ends with EHOSTUNREACH in ~3 s (no ARP reply) — a deterministic
// network error, the ErrNetwork class → no_http.
const httpBlackholeIP = "172.28.100.250"

// httpTarpitSpeed/httpTarpitCap — the anti-tarpit parameters for
// TestHTTPFetch_Tarpit: a speed of 8192 B/s against the mockserver's
// 512 B/s output and a 4 KiB cap — the watchdog fires in ~1 s (the
// remain/speed deadline = 500 ms < the 1 s chunk interval), the total
// timeout (20 s) does not get there: it is the T-049 mechanism that is
// tested.
const (
	httpTarpitSpeed = int64(8192)
	httpTarpitCap   = int64(4096)
)

// httpFetchURL parses a scenario URL (an error is fatal, the names are fixed).
func httpFetchURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("URL %s: %v", raw, err)
	}
	return u
}

// httpTestFetcher — the stage transport with InsecureSkipVerify (spec 5.5
// item 3) and an honest UA (§9) over the test parameters: mutate edits the
// Options before creation (timeouts, limits, resolver). A nil mutation —
// the defaults of the spec 5.5 limit table.
func httpTestFetcher(t *testing.T, mutate func(*httpfetch.Options)) *httpfetch.Fetcher {
	t.Helper()
	opts := httpfetch.Options{
		UserAgent:       config.DefaultUserAgent,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // testbed, spec 5.5 item 3
		Limits:          &httpfetch.LimitOptions{},
	}
	if mutate != nil {
		mutate(&opts)
	}
	f := httpfetch.NewFetcher(opts)
	t.Cleanup(f.CloseIdleConnections)
	return f
}

// ---------------------------------------------------------------- transport

// TestHTTPFetch_Tarpit — the T-049 anti-tarpit (spec 5.5 "min. speed after
// the first byte", §8): mockserver serves the body at 512 B/s — below the
// minimum; the watchdog closes the connection, the read returns an error
// with the timeout classification (temp_error at the consumer, not "no
// site").
func TestHTTPFetch_Tarpit(t *testing.T) {
	skipUnlessIntegration(t)
	f := httpTestFetcher(t, func(o *httpfetch.Options) {
		o.MaxBodyBytes = httpTarpitCap
		o.Limits.MaxBodyBytes = httpTarpitCap
		o.Limits.MinBodySpeed = httpTarpitSpeed
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	res, err := f.Fetch(ctx, httpfetch.FetchRequest{
		URL:   httpFetchURL(t, "http://tarpit.crawl.test/"),
		Addrs: []netip.Addr{netip.MustParseAddr(mockserverIP)},
	})
	if err != nil {
		t.Fatalf("Fetch (the tarpit's headers arrive at once): %v", err)
	}
	if res.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}

	start := time.Now()
	body, readErr := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if readErr == nil {
		t.Fatal("reading the infinite slow body must break by the speed watchdog")
	}
	if !strings.Contains(readErr.Error(), "tarpit") {
		t.Errorf("the error is not anti-tarpit: %v", readErr)
	}
	if !httpfetch.IsTimeout(readErr) {
		t.Errorf("the error without a time classification (temp_error at the worker): %v", readErr)
	}
	if n := len(body); n == 0 || int64(n) > httpTarpitCap {
		t.Errorf("read %d bytes — outside the range (0; %d]", n, httpTarpitCap)
	}
	if el := time.Since(start); el > 10*time.Second {
		t.Errorf("the trigger took %v — the overall timeout worked, not the speed watchdog", el)
	}
}

// TestHTTPFetch_HugeBody — the body cap of T-049 (spec 5.5 "max readable
// body size | 2 MB", §8): mockserver declares Content-Length 1 GiB and
// really pours beyond the limit — the body is cut exactly at 2 MiB, the
// Truncated flag is set, the read has no network error (partial data, not
// a failure).
func TestHTTPFetch_HugeBody(t *testing.T) {
	skipUnlessIntegration(t)
	f := httpTestFetcher(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	res, err := f.Fetch(ctx, httpfetch.FetchRequest{
		URL:   httpFetchURL(t, "http://huge-body.crawl.test/"),
		Addrs: []netip.Addr{netip.MustParseAddr(mockserverIP)},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer func() { _ = res.Body.Close() }()
	if got := res.Header.Get("Content-Length"); got != "1073741824" {
		t.Fatalf("Content-Length = %q, want 1073741824 (the scenario declares a gigabyte)", got)
	}

	body, readErr := io.ReadAll(res.Body)
	if readErr != nil {
		t.Fatalf("the cap truncation — not a network error: %v", readErr)
	}
	if n := len(body); n != 2<<20 {
		t.Errorf("read %d bytes, want exactly %d (the cap)", n, 2<<20)
	}
	if res.BodyLimiter == nil || !res.BodyLimiter.Truncated() {
		t.Error("the Truncated sign not set (the partiality decision belongs to T-056/T-058)")
	}
}

// TestHTTPFetch_ZipBomb — the decompression cap of T-051 (spec 5.5
// "decompression — with a size limit", §8): an infinite gzip stream of
// zeros decompresses no further than MaxBodyBytes —
// ErrDecompressTooLarge. There is no br scenario: mockserver serves only
// gzip, testweb (Caddy) does not encode responses.
func TestHTTPFetch_ZipBomb(t *testing.T) {
	skipUnlessIntegration(t)
	f := httpTestFetcher(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	res, err := f.Fetch(ctx, httpfetch.FetchRequest{
		URL:   httpFetchURL(t, "http://zip-bomb.crawl.test/"),
		Addrs: []netip.Addr{netip.MustParseAddr(mockserverIP)},
	})
	if err != nil {
		t.Fatalf("Fetch (the headers arrive, the error — in the body unpacking): %v", err)
	}
	_, readErr := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if !errors.Is(readErr, httpfetch.ErrDecompressTooLarge) {
		t.Fatalf("the read error = %v, want ErrDecompressTooLarge", readErr)
	}
}

// TestHTTPChain_RedirectLoops — the T-048 chains (spec 5.5 item 1, §8):
// an infinite redirect is stopped by the max_redirects limit, a ring — by
// loop detection; both causes share the single ErrTooManyRedirects
// classification, NOT temp_error (a permanent site property, spec 6.2).
func TestHTTPChain_RedirectLoops(t *testing.T) {
	skipUnlessIntegration(t)
	f := httpTestFetcher(t, nil)
	addrs := []netip.Addr{netip.MustParseAddr(mockserverIP)}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	t.Run("an infinite redirect — the limit", func(t *testing.T) {
		cr, err := httpfetch.FetchChain(ctx, f, httpfetch.FetchRequest{
			URL:   httpFetchURL(t, "http://infinite-redirect.crawl.test/"),
			Addrs: addrs,
		})
		if !errors.Is(err, httpfetch.ErrTooManyRedirects) {
			t.Fatalf("the error = %v, want ErrTooManyRedirects", err)
		}
		if !strings.Contains(err.Error(), "limit") {
			t.Errorf("the reason is not the limit: %v", err)
		}
		if httpfetch.IsTimeout(err) {
			t.Error("the redirect limit is not a time class (not temp_error, spec 6.2)")
		}
		if cr == nil || cr.Result != nil {
			t.Fatal("no final page: the Result must be nil")
		}
		if want := httpfetch.DefaultMaxRedirects + 1; len(cr.Chain) != want {
			t.Errorf("the chain = %d steps, want %d (the start + 8 hops)", len(cr.Chain), want)
		}
	})

	t.Run("a cyclic redirect — the cycle detection", func(t *testing.T) {
		cr, err := httpfetch.FetchChain(ctx, f, httpfetch.FetchRequest{
			URL:   httpFetchURL(t, "http://cycle-redirect.crawl.test/"),
			Addrs: addrs,
		})
		if !errors.Is(err, httpfetch.ErrTooManyRedirects) {
			t.Fatalf("the error = %v, want ErrTooManyRedirects (a cycle — the same classification)", err)
		}
		if !strings.Contains(err.Error(), "cycle") {
			t.Errorf("the reason is not a cycle: %v", err)
		}
		if cr == nil || cr.Result != nil || len(cr.Chain) != 3 {
			t.Errorf("the outcome = %+v (chain %d), want a nil Result and the \"/\"→/b→/a chain (the start — the scenario's main page)", cr, len(cr.Chain))
		}
	})
}

// TestHTTPFetch_BadTLS — bad certificates (T-050; spec 5.5 item 3, §8):
// the connect goes through (InsecureSkipVerify — the content is needed),
// the certificate attributes are preserved, standalone verification gives
// Valid=false with typed causes. The scenario domains (tls-*.crawl.test,
// SNI) and the debug bad-tls names from env_test.go carry the same
// property.
func TestHTTPFetch_BadTLS(t *testing.T) {
	skipUnlessIntegration(t)
	f := httpTestFetcher(t, nil)
	addrs := []netip.Addr{netip.MustParseAddr(mockserverIP)}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cases := []struct {
		host     string
		problems []httpfetch.TLSProblem
	}{
		{"tls-self-signed.crawl.test", []httpfetch.TLSProblem{httpfetch.TLSProblemSelfSigned}},
		{"tls-expired.crawl.test", []httpfetch.TLSProblem{httpfetch.TLSProblemExpired, httpfetch.TLSProblemSelfSigned}},
		{"tls-wrong-san.crawl.test", []httpfetch.TLSProblem{httpfetch.TLSProblemHostnameMismatch, httpfetch.TLSProblemSelfSigned}},
		{"self-signed.bad-tls.crawl.test", []httpfetch.TLSProblem{httpfetch.TLSProblemSelfSigned}},
		{"expired.bad-tls.crawl.test", []httpfetch.TLSProblem{httpfetch.TLSProblemExpired, httpfetch.TLSProblemSelfSigned}},
		{"wrong-san.bad-tls.crawl.test", []httpfetch.TLSProblem{httpfetch.TLSProblemHostnameMismatch, httpfetch.TLSProblemSelfSigned}},
	}
	for _, tc := range cases {
		t.Run(tc.host, func(t *testing.T) {
			res, err := f.Fetch(ctx, httpfetch.FetchRequest{
				URL:   httpFetchURL(t, "https://"+tc.host+"/"),
				Addrs: addrs,
			})
			if err != nil {
				t.Fatalf("the connect with InsecureSkipVerify must pass: %v", err)
			}
			defer func() { _ = res.Body.Close() }()
			if res.StatusCode != 200 {
				t.Fatalf("status = %d, want 200 (an invalid certificate does not hinder the fetch, §8)", res.StatusCode)
			}
			info := res.TLSInfo
			if info == nil {
				t.Fatal("TLSInfo not parsed (the issuer/expiry/SAN must be preserved)")
			}
			if info.Valid {
				t.Error("Valid = true for an invalid certificate")
			}
			if len(info.Problems) != len(tc.problems) {
				t.Fatalf("the problems = %v, want %v", info.Problems, tc.problems)
			}
			for i, p := range tc.problems {
				if info.Problems[i] != p {
					t.Errorf("problem[%d] = %s, want %s (the order: expiry, host, self-signed)", i, info.Problems[i], p)
				}
			}
			if !info.SelfSigned {
				t.Error("SelfSigned = false — the mockserver certificates are homemade")
			}
		})
	}

	t.Run("http — TLSInfo nil, not an error", func(t *testing.T) {
		res, err := f.Fetch(ctx, httpfetch.FetchRequest{
			URL:   httpFetchURL(t, "http://cookies.crawl.test/"),
			Addrs: addrs,
		})
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		defer func() { _ = res.Body.Close() }()
		if res.TLSInfo != nil {
			t.Error("TLSInfo of an http answer must be nil (§8)")
		}
	})
}

// ---------------------------------------------------------------- pipeline

// httpWorkerEnv — the worker pipeline on live PostgreSQL/Redis: an isolated
// schema, a queue (publishing to q:analyze), a PackStore in a temp directory
// and an OnResult chain as in the cmd/http-worker build (T-056 → T-058).
// The fetcher — the stage transport (adapted by the test: timeouts,
// resolution).
type httpWorkerEnv struct {
	pool     *pgxpool.Pool
	rdb      *redis.Client
	q        *queue.RedisQueue
	store    *content.PackStore
	saver    *httpworker.ContentSaver
	recorder *httpworker.Recorder
	worker   *httpworker.Worker
}

// httpWorkerSetup assembles the env: mutate edits the transport parameters.
func httpWorkerSetup(t *testing.T, mutate func(*httpfetch.Options)) *httpWorkerEnv {
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

	// Redirect hosts are resolved by the same resolver as the DNS stage
	// (spec 5.5 item 2, the httpworker.DNSLookup adapter + a cache, as in
	// the cmd/http-worker build).
	resolve := dnsEnv(t)
	fetcher := httpTestFetcher(t, func(o *httpfetch.Options) {
		o.Resolver = httpfetch.NewCachingResolver(
			httpworker.NewDNSLookup(resolve, false), httpfetch.CacheOptions{})
		if mutate != nil {
			mutate(o)
		}
	})

	// MaxRedirects — a mandatory explicit limit: 0 in Options means "do not
	// follow" (T-053), not the default.
	w, err := httpworker.New(httpworker.ChainFetch(fetcher), httpworker.Options{
		MaxRedirects: httpfetch.DefaultMaxRedirects,
		Store:        store,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &httpWorkerEnv{
		pool: pool, rdb: rdb, q: q, store: store,
		saver: saver, recorder: recorder, worker: w,
	}
}

// process — a full domain episode, as in the worker's processDelivery:
// ProcessDomain → content saving (T-056) → result recording and
// publishing to q:analyze (T-058). Returns the result with the alias
// marking and ContentSaver normalization already applied.
func (e *httpWorkerEnv) process(ctx context.Context, t *testing.T, task queue.HTTPTask) *httpworker.DomainResult {
	t.Helper()
	res, err := e.worker.ProcessDomain(ctx, task)
	if err != nil {
		t.Fatalf("ProcessDomain %s: %v", task.Name, err)
	}
	if err := e.saver.Save(ctx, res); err != nil {
		t.Fatalf("ContentSaver %s: %v", task.Name, err)
	}
	if _, err := e.recorder.RecordAndPublish(ctx, res); err != nil {
		t.Fatalf("RecordAndPublish %s: %v", task.Name, err)
	}
	return res
}

// httpTask — a q:http task for a domain with "label → address" subdomains.
// The subdomain order is deterministic (by ascending id), as in the DNS
// stage tasks (T-039).
func httpTask(zoneID int16, domainID int64, name string, subs map[int16]string) queue.HTTPTask {
	task := queue.HTTPTask{ZoneID: zoneID, DomainID: domainID, Name: name}
	for _, id := range []int16{0, 1} {
		label, ok := subs[id]
		if !ok {
			continue
		}
		task.Subdomains = append(task.Subdomains, queue.HTTPSubdomain{
			SubdomainID: id, Label: label, Addrs: []string{httpScenarioAddr(name, label)},
		})
	}
	return task
}

// httpScenarioAddr — the bench address for a scenario subdomain: testweb
// for the Caddy fixtures, mockserver for the "nasty" responses, the black
// hole for closed-ports (the address routing is the same as in the CoreDNS
// zones).
func httpScenarioAddr(name, label string) string {
	host := name
	if label != "" {
		host = label + "." + name
	}
	switch {
	case strings.HasPrefix(host, "closed-ports."):
		return httpBlackholeIP
	case strings.HasPrefix(host, "spa-shell."), strings.HasPrefix(host, "same-site."),
		strings.HasPrefix(host, "www.same-site."), strings.HasPrefix(host, "bare-to-www."),
		strings.HasPrefix(host, "www.bare-to-www."):
		return testwebIP
	default:
		return mockserverIP
	}
}

// httpSubdomainRow — a subdomain_results row for the checks.
type httpSubdomainRow struct {
	subdomainID    int16
	variant        int16
	scheme         int16
	httpStatus     int16
	finalURL       string
	aliasKind      int16
	aliasSubdomain *int16
	contentHash    []byte
	errCode        *int16
}

// httpRows reads a domain's subdomain_results rows sorted by subdomain_id
// (the insertion order is unspecified).
func httpRows(t *testing.T, pool *pgxpool.Pool, domainID int64) []httpSubdomainRow {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT subdomain_id, variant, scheme, http_status, final_url, alias_kind,
		        alias_subdomain_id, content_hash, error
		 FROM subdomain_results WHERE domain_id = $1 ORDER BY subdomain_id`, domainID)
	if err != nil {
		t.Fatalf("read subdomain_results: %v", err)
	}
	defer rows.Close()
	var out []httpSubdomainRow
	for rows.Next() {
		var r httpSubdomainRow
		if err := rows.Scan(&r.subdomainID, &r.variant, &r.scheme, &r.httpStatus, &r.finalURL,
			&r.aliasKind, &r.aliasSubdomain, &r.contentHash, &r.errCode); err != nil {
			t.Fatalf("a subdomain_results row: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("subdomain_results iteration: %v", err)
	}
	return out
}

// httpDomainRow — the domain state after an episode.
type httpDomainRow struct {
	status        model.Status
	checkState    int16
	queuedAt      *time.Time
	lastCheckedAt *time.Time
	tempAttempts  int16
}

func httpDomainState(t *testing.T, pool *pgxpool.Pool, domainID int64) httpDomainRow {
	t.Helper()
	var r httpDomainRow
	err := pool.QueryRow(context.Background(),
		`SELECT status, check_state, queued_at, last_checked_at, temp_attempts
		 FROM domains WHERE id = $1`, domainID,
	).Scan(&r.status, &r.checkState, &r.queuedAt, &r.lastCheckedAt, &r.tempAttempts)
	if err != nil {
		t.Fatalf("reading domains: %v", err)
	}
	return r
}

// TestHTTPWorker_E2E — the end-to-end minimum of the HTTP stage (spec 5.5
// items 5–6, 9, §7; as T-040 for DNS): q:http tasks for the same-site
// (http→https to one site — a single variant 0 row) and bare→www (a
// redirect to a subdomain — alias_kind 2, the content is stored once)
// scenarios through live PostgreSQL/Redis: subdomain_results rows,
// publishing to q:analyze, the contents refcount on redelivery
// (at-least-once, spec 3.4).
func TestHTTPWorker_E2E(t *testing.T) {
	skipUnlessIntegration(t)
	env := httpWorkerSetup(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	labels := []string{"same-site", "bare-to-www"}
	ids := insertPendingDomains(t, env.pool, "crawl.test", labels)
	var zoneID int16
	if err := env.pool.QueryRow(ctx,
		"SELECT id FROM zones WHERE name = $1", "crawl.test").Scan(&zoneID); err != nil {
		t.Fatalf("zone crawl.test: %v", err)
	}
	// The bare-to-www page hash for the contents checks.
	var pageHash *content.Hash

	t.Run("same-site: one site — the only variant 0 record", func(t *testing.T) {
		task := httpTask(zoneID, ids[0], "same-site.crawl.test", map[int16]string{0: "", 1: "www"})
		res := env.process(ctx, t, task)
		if !res.HasRecords() {
			t.Fatal("the same-site domain must be alive (records exist)")
		}

		apex := res.Subdomains[0]
		if len(apex.Records) != 1 {
			t.Fatalf("apex records: %d, want 1 (the protocols converge on one site)", len(apex.Records))
		}
		rec := apex.Records[0]
		if rec.Variant != model.VariantMain || rec.Scheme != model.SchemeHTTPS {
			t.Errorf("variant/scheme = %d/%d, want 0/https (https canonical, spec 5.5 item 5)", rec.Variant, rec.Scheme)
		}
		if rec.FinalURL != "https://same-site.crawl.test/" {
			t.Errorf("the final URL = %q, want https://same-site.crawl.test/", rec.FinalURL)
		}
		if rec.AliasKind != model.AliasNone || rec.AliasSubdomainID != nil {
			t.Errorf("the apex alias = %d/%v, want none", rec.AliasKind, rec.AliasSubdomainID)
		}
		if rec.HTTPStatus != 200 || rec.ErrClass != httpworker.ErrNone {
			t.Errorf("status/error = %d/%s, want 200/none", rec.HTTPStatus, rec.ErrClass)
		}
		if !rec.StoreContent || rec.ContentHash == nil {
			t.Error("the original's content not stored (StoreContent/ContentHash)")
		}

		// www.same-site is not configured in Caddy: https is rejected by
		// SNI, http answers with an empty page — a single row with the
		// scheme of its own check ("only one protocol works", spec 5.5
		// item 5).
		www := res.Subdomains[1]
		if len(www.Records) != 1 || www.Records[0].Scheme != model.SchemeHTTP {
			t.Fatalf("www records = %d, want 1 over http", len(www.Records))
		}
		if www.HTTPS.Available {
			t.Error("https www.same-site must fail (the host not configured in Caddy)")
		}
	})

	t.Run("bare→www: an alias and the content once", func(t *testing.T) {
		task := httpTask(zoneID, ids[1], "bare-to-www.crawl.test", map[int16]string{0: "", 1: "www"})
		res := env.process(ctx, t, task)

		apex := res.Subdomains[0]
		if len(apex.Records) != 1 {
			t.Fatalf("apex records: %d, want 1", len(apex.Records))
		}
		arec := apex.Records[0]
		if arec.AliasKind != model.AliasRedirect {
			t.Errorf("the apex's alias_kind = %d, want 2 (a redirect to the original subdomain)", arec.AliasKind)
		}
		if arec.AliasSubdomainID == nil || *arec.AliasSubdomainID != 1 {
			t.Errorf("alias_subdomain_id = %v, want 1 (www)", arec.AliasSubdomainID)
		}
		if arec.StoreContent || arec.ContentHash != nil {
			t.Error("the alias does not store its own content (the hash normalized to NULL)")
		}
		if arec.FinalURL != "https://www.bare-to-www.crawl.test/" {
			t.Errorf("the apex's final URL = %q, want https://www.bare-to-www.crawl.test/", arec.FinalURL)
		}

		www := res.Subdomains[1]
		if len(www.Records) != 1 {
			t.Fatalf("www records: %d, want 1", len(www.Records))
		}
		wrec := www.Records[0]
		if wrec.AliasKind != model.AliasNone || wrec.AliasSubdomainID != nil {
			t.Errorf("the www alias = %d/%v, want none (the original record \"at home\")", wrec.AliasKind, wrec.AliasSubdomainID)
		}
		if !wrec.StoreContent || wrec.ContentHash == nil {
			t.Fatal("the original's content not stored")
		}
		h := *wrec.ContentHash
		pageHash = &h

		// The page is read from the store (a roundtrip, spec 11.4 item 3).
		body, err := env.store.Get(ctx, h)
		if err != nil {
			t.Fatalf("Get of the content: %v", err)
		}
		if !strings.Contains(string(body), "bare-to-www.crawl.test") {
			t.Errorf("the content does not look like the fixture page: %.80q", body)
		}
	})

	t.Run("subdomain_results rows", func(t *testing.T) {
		rows := httpRows(t, env.pool, ids[1])
		if len(rows) != 2 {
			t.Fatalf("bare-to-www rows: %d, want 2 (the apex and www, variant 0)", len(rows))
		}
		apex, www := rows[0], rows[1]
		if apex.subdomainID != 0 || www.subdomainID != 1 {
			t.Fatalf("row order: subdomain_id = %d, %d, want 0, 1", apex.subdomainID, www.subdomainID)
		}
		for _, r := range rows {
			if r.variant != int16(model.VariantMain) || r.scheme != int16(model.SchemeHTTPS) {
				t.Errorf("row %d: variant/scheme = %d/%d, want 0/https", r.subdomainID, r.variant, r.scheme)
			}
			if r.httpStatus != 200 || r.finalURL != "https://www.bare-to-www.crawl.test/" {
				t.Errorf("row %d: status/URL = %d/%q", r.subdomainID, r.httpStatus, r.finalURL)
			}
			if r.errCode != nil {
				t.Errorf("row %d: error = %d, want NULL", r.subdomainID, *r.errCode)
			}
		}
		if apex.aliasKind != int16(model.AliasRedirect) || apex.aliasSubdomain == nil || *apex.aliasSubdomain != 1 {
			t.Errorf("apex: alias = %d/%v, want 2/1", apex.aliasKind, apex.aliasSubdomain)
		}
		if apex.contentHash != nil {
			t.Error("apex: content_hash must be NULL (the content belongs to the original)")
		}
		if www.aliasKind != int16(model.AliasNone) || www.aliasSubdomain != nil {
			t.Errorf("www: alias = %d/%v, want none", www.aliasKind, www.aliasSubdomain)
		}
		if len(www.contentHash) != 16 {
			t.Errorf("www: content_hash = %d bytes, want 16", len(www.contentHash))
		}
	})

	t.Run("domains: alive/done, the lease finished", func(t *testing.T) {
		for i, label := range labels {
			r := httpDomainState(t, env.pool, ids[i])
			if r.status != model.StatusAlive {
				t.Errorf("%s: status = %s, want alive", label, r.status)
			}
			if r.checkState != int16(model.CheckStateDone) {
				t.Errorf("%s: check_state = %d, want done (§7)", label, r.checkState)
			}
			if r.queuedAt != nil || r.lastCheckedAt == nil || r.tempAttempts != 0 {
				t.Errorf("%s: queued_at=%v last_checked_at=%v temp_attempts=%d", label, r.queuedAt, r.lastCheckedAt, r.tempAttempts)
			}
		}
	})

	t.Run("publishing q:analyze", func(t *testing.T) {
		if n := streamLen(t, env.rdb, queue.StreamAnalyze); n != 4 {
			t.Fatalf("entries in q:analyze = %d, want 4 (one per subdomain×variant write)", n)
		}
		got := map[string]bool{}
		for _, d := range consumeTasks(t, env.q, queue.StreamAnalyze, "it-http-e2e", "an", 10) {
			res, err := queue.Decode[queue.Result](d)
			if err != nil {
				t.Fatalf("decoding Result: %v", err)
			}
			got[fmt.Sprintf("%d/%d/%d", res.DomainID, res.SubdomainID, res.Variant)] = true
		}
		want := map[string]bool{}
		for _, id := range ids {
			want[fmt.Sprintf("%d/0/0", id)] = true
			want[fmt.Sprintf("%d/1/0", id)] = true
		}
		if len(got) != len(want) {
			t.Fatalf("the q:analyze keys = %v, want %v", got, want)
		}
		for k := range want {
			if !got[k] {
				t.Errorf("no %s key in q:analyze (domain/subdomain/variant)", k)
			}
		}
	})

	t.Run("a redelivery: no refcount leak", func(t *testing.T) {
		if pageHash == nil {
			t.Fatal("the page's hash not pinned (the bare→www check did not pass)")
		}
		before, ok := contentRowByHash(t, env.pool, *pageHash)
		if !ok {
			t.Fatal("the page's content not stored in contents before the repeat")
		}
		if before.refc != 1 {
			t.Fatalf("the refcount before the repeat = %d, want 1", before.refc)
		}

		task := httpTask(zoneID, ids[1], "bare-to-www.crawl.test", map[int16]string{0: "", 1: "www"})
		env.process(ctx, t, task)

		after, ok := contentRowByHash(t, env.pool, *pageHash)
		if !ok {
			t.Fatal("the content disappeared after the redelivery")
		}
		if after.refc != 1 {
			t.Errorf("the refcount after the repeat = %d, want 1 (a repeat Put +1 and the old row's DELETE −1)", after.refc)
		}
		if n := len(httpRows(t, env.pool, ids[1])); n != 2 {
			t.Errorf("rows after the repeat = %d, want 2 (an overwrite, not growth)", n)
		}
		if n := streamLen(t, env.rdb, queue.StreamAnalyze); n != 6 {
			t.Errorf("entries in q:analyze = %d, want 6 (the duplicates limited by delivery)", n)
		}
	})
}

// TestHTTPWorker_EvilScenarios — "nasty" responses through the worker
// pipeline (spec §8, 5.5): the check error classification and the final
// domain statuses per §7 — tarpit → temp_error (the timeout class), black
// hole → no_http, a giant body → truncation without storing content
// (T-056), a zip bomb and an infinite redirect → ErrBody/ErrRedirects
// codes without the temp class, an SPA shell → the render verdict (T-057),
// bad certificates → a load + TLSInfo with causes (T-050).
func TestHTTPWorker_EvilScenarios(t *testing.T) {
	skipUnlessIntegration(t)
	// The pipeline-level tarpit must be classified by the T-049 anti-tarpit
	// (limitBodySpeedError — the timeout class → temp_error): the body cap
	// and the min. speed are squeezed so that the watchdog fires long
	// before the total timeout (in production the same ratio gives a cap of
	// 2 MiB against speeds below 2 KiB/s). The other scenarios are not
	// affected by the squeezed cap: their responses are shorter than 4 KiB
	// or fail on their own limits (decompression, redirects).
	env := httpWorkerSetup(t, func(o *httpfetch.Options) {
		o.TotalTimeout = 5 * time.Second
		o.MaxBodyBytes = httpTarpitCap
		o.Limits.MaxBodyBytes = httpTarpitCap
		o.Limits.MinBodySpeed = httpTarpitSpeed
	})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	labels := []string{
		"tarpit", "closed-ports", "huge-body", "zip-bomb",
		"infinite-redirect", "spa-shell", "tls-self-signed", "tls-expired", "tls-wrong-san",
	}
	ids := insertPendingDomains(t, env.pool, "crawl.test", labels)
	var zoneID int16
	if err := env.pool.QueryRow(ctx,
		"SELECT id FROM zones WHERE name = $1", "crawl.test").Scan(&zoneID); err != nil {
		t.Fatalf("zone crawl.test: %v", err)
	}
	byLabel := map[string]int64{}
	for i, l := range labels {
		byLabel[l] = ids[i]
	}
	// All scenarios without www: one subdomain (the apex) — that is how
	// they are declared in scenarios.yaml.
	run := func(label string) *httpworker.DomainResult {
		t.Helper()
		name := label + ".crawl.test"
		return env.process(ctx, t, httpTask(zoneID, byLabel[label], name, map[int16]string{0: ""}))
	}

	t.Run("the tarpit: the time classification → temp_error", func(t *testing.T) {
		res := run("tarpit")
		for _, c := range []struct {
			name string
			chk  httpworker.SchemeCheck
		}{{"https", res.Subdomains[0].HTTPS}, {"http", res.Subdomains[0].HTTP}} {
			if c.chk.ErrClass != httpworker.ErrTimeout {
				t.Errorf("%s: class = %s, want timeout (temp_error, spec 6.2)", c.name, c.chk.ErrClass)
			}
			if c.chk.Available {
				t.Errorf("%s: Available for the cut-off tarpit", c.name)
			}
		}
		if res.HasRecords() || !res.AnyTimeout() {
			t.Fatal("no records and there are timeouts — the temp_error status input (§7)")
		}
		if r := httpDomainState(t, env.pool, byLabel["tarpit"]); r.status != model.StatusTempError {
			t.Errorf("the tarpit's status = %s, want temp_error (not no_http)", r.status)
		}
	})

	t.Run("closed ports: no answer → no_http", func(t *testing.T) {
		res := run("closed-ports")
		if res.HasRecords() {
			t.Fatal("the web silent — there must be no records")
		}
		if res.AnyTimeout() {
			t.Error("a connection refusal is not a time class (not temp_error)")
		}
		if r := httpDomainState(t, env.pool, byLabel["closed-ports"]); r.status != model.StatusNoHTTP {
			t.Errorf("status = %s, want no_http", r.status)
		}
	})

	t.Run("a giant body: truncation without content", func(t *testing.T) {
		res := run("huge-body")
		recs := res.Subdomains[0].Records
		if len(recs) != 1 {
			t.Fatalf("records: %d, want 1 (the status received — the protocol available)", len(recs))
		}
		rec := recs[0]
		if rec.HTTPStatus != 200 || !rec.Truncated || rec.ErrClass != httpworker.ErrBody {
			t.Errorf("status/truncation/class = %d/%v/%s, want 200/truncated/body (§8)", rec.HTTPStatus, rec.Truncated, rec.ErrClass)
		}
		if rec.ContentHash != nil || !rec.ContentNotStored {
			t.Error("the truncated body not hashed and not stored (a hash of truncated bytes would lie at dedup, T-056)")
		}
		rows := httpRows(t, env.pool, byLabel["huge-body"])
		if len(rows) != 1 || rows[0].errCode == nil || *rows[0].errCode != int16(httpworker.ErrBody) {
			t.Fatalf("the DB rows = %+v, want one with error=body", rows)
		}
		if rows[0].contentHash != nil {
			t.Error("the truncated record's content_hash must be NULL")
		}
		if r := httpDomainState(t, env.pool, byLabel["huge-body"]); r.status != model.StatusAlive {
			t.Errorf("status = %s, want alive (the page answered)", r.status)
		}
	})

	t.Run("a zip bomb: ErrBody without records", func(t *testing.T) {
		res := run("zip-bomb")
		for _, chk := range []httpworker.SchemeCheck{res.Subdomains[0].HTTPS, res.Subdomains[0].HTTP} {
			if chk.ErrClass != httpworker.ErrBody || chk.Available {
				t.Errorf("class/Available = %s/%v, want body/false (the unpacking cap, T-051)", chk.ErrClass, chk.Available)
			}
		}
		if res.HasRecords() {
			t.Error("no page — there must be no records")
		}
		if r := httpDomainState(t, env.pool, byLabel["zip-bomb"]); r.status != model.StatusNoHTTP {
			t.Errorf("status = %s, want no_http (the limit is not a temp class)", r.status)
		}
	})

	t.Run("an infinite redirect: ErrRedirects, not temp", func(t *testing.T) {
		res := run("infinite-redirect")
		chk := res.Subdomains[0].HTTPS
		if chk.ErrClass != httpworker.ErrRedirects {
			t.Fatalf("class = %s, want redirects", chk.ErrClass)
		}
		if chk.ErrClass.IsTemp() {
			t.Error("the redirect limit is a permanent property of the site (not temp_error, T-048)")
		}
		if want := httpfetch.DefaultMaxRedirects + 1; len(chk.Redirects) != want {
			t.Errorf("the chain in the result = %d steps, want %d", len(chk.Redirects), want)
		}
		if r := httpDomainState(t, env.pool, byLabel["infinite-redirect"]); r.status != model.StatusNoHTTP {
			t.Errorf("status = %s, want no_http", r.status)
		}
	})

	t.Run("the SPA shell: the render verdict and content", func(t *testing.T) {
		res := run("spa-shell")
		recs := res.Subdomains[0].Records
		if len(recs) != 1 {
			t.Fatalf("records: %d, want 1", len(recs))
		}
		rec := recs[0]
		if rec.SPA == nil || !rec.SPA.SPA {
			t.Fatal("the SPA verdict not set (T-057: the q:render publication — the render stage)")
		}
		reasons := map[httpfetch.SPAReason]bool{}
		for _, r := range rec.SPA.Reasons {
			reasons[r] = true
		}
		for _, want := range []httpfetch.SPAReason{
			httpfetch.SPAReasonLowText, httpfetch.SPAReasonRootContainer, httpfetch.SPAReasonNoScriptJS,
		} {
			if !reasons[want] {
				t.Errorf("the sign %s did not fire (the fixture #root + noscript)", want)
			}
		}
		if rec.Charset != "utf-8" {
			t.Errorf("the charset = %q, want utf-8", rec.Charset)
		}
		if !rec.StoreContent || rec.ContentHash == nil {
			t.Error("the SPA shell's content must be stored (the render will replace it later, T-067)")
		}
		rows := httpRows(t, env.pool, byLabel["spa-shell"])
		if len(rows) != 1 || rows[0].contentHash == nil {
			t.Fatalf("the DB rows = %+v, want one with content", rows)
		}
	})

	tlsCases := []struct {
		label    string
		problems []httpfetch.TLSProblem
	}{
		{"tls-self-signed", []httpfetch.TLSProblem{httpfetch.TLSProblemSelfSigned}},
		{"tls-expired", []httpfetch.TLSProblem{httpfetch.TLSProblemExpired, httpfetch.TLSProblemSelfSigned}},
		{"tls-wrong-san", []httpfetch.TLSProblem{httpfetch.TLSProblemHostnameMismatch, httpfetch.TLSProblemSelfSigned}},
	}
	for _, tc := range tlsCases {
		t.Run(tc.label+": the fetch + TLSInfo in the record", func(t *testing.T) {
			res := run(tc.label)
			chk := res.Subdomains[0].HTTPS
			if !chk.Available || chk.TLS == nil {
				t.Fatalf("https unavailable or TLS not parsed (an invalid certificate does not hinder, §8)")
			}
			if chk.TLS.Valid || len(chk.TLS.Problems) != len(tc.problems) {
				t.Fatalf("the certificate check = valid:%v problems:%v, want false %v",
					chk.TLS.Valid, chk.TLS.Problems, tc.problems)
			}
			rows := httpRows(t, env.pool, byLabel[tc.label])
			if len(rows) != 1 {
				t.Fatalf("rows: %d, want 1 (the protocols lead to one site)", len(rows))
			}
			var tlsJSON string
			if err := env.pool.QueryRow(ctx,
				"SELECT tls::text FROM subdomain_results WHERE domain_id = $1", byLabel[tc.label],
			).Scan(&tlsJSON); err != nil {
				t.Fatalf("reading the tls jsonb: %v", err)
			}
			for _, p := range tc.problems {
				if !strings.Contains(tlsJSON, `"`+p.String()+`"`) {
					t.Errorf("the tls jsonb without the %s problem: %s", p, tlsJSON)
				}
			}
		})
	}
}

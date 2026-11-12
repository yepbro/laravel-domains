// Package httpworker — the HTTP stage of domain checking (T-053; spec 5.5 items 1, 6,
// §4.2, §7). A q:http task — a domain in full (the T-024 format): the worker checks
// every DNS-alive subdomain over both protocols (https and http; redirects,
// limits, TLS and encodings — the httpfetch.FetchChain chain, T-048), in parallel
// inside the task (the Options.PerDomain goroutine pool), with a shared "final
// URL → result" cache (spec 5.5 item 6: do not download the same page twice —
// the bare→www alias after a redirect to the same URL is not downloaded by a
// second load). The domain result is assembled in full: the records after the
// variant logic (DecideVariants, T-054), the aliases (DetectAliases, T-055) and
// the SPA heuristic (DetectSPA, T-057) — and returned to the caller.
//
// Writing the result into the DB (subdomain_results, the domain status) and the
// q:analyze publication — Recorder (record.go, T-058), the service build puts it into
// Options.OnResult right after ContentSaver (T-056); the hook is called in the Run
// loop before the XACK acknowledgment — the "write into the DB → publication → XACK"
// order (spec 4.2, §5.5 item 9) is assembled without editing the loop. The q:render publication over
// the SPA records and the render_pending flag on the render queue's overflow —
// renderq.go (T-066), called from the Recorder.
//
// The page bodies (UTF-8, T-051) are not held in memory (T-147): the body
// is read with a cap, the hash is computed over it (the variants criterion
// of T-054 and the aliases T-055 — the comparison is always by hashes) and the SPA verdict
// (T-057), then it is immediately stored into the ContentStore (Options.Store,
// a Put by hash) and discarded — the task slot does not hold bodies longer than
// the receipt + the Put (the slot memory: only the metadata and PerDomain
// buffers of the bodies being read). The references added by early Puts are reconciled with
// the result records in ContentSaver (store.go): a page that did not become
// anyone's subdomain_results row (a folded variant, a losing
// alias class, a repeated download of the same content), releases
// the reference by Delete; one that did — moves into the record's row (T-056).
//
// The slot memory is observable (T-163): the busy slots, the body bytes in processing
// and the body size distribution are exported into /metrics (the metrics
// crawler_http_tasks_inflight / crawler_http_body_bytes_inflight /
// crawler_http_body_bytes) — the sampler computes the "unit memory / slot" for
// the task_concurrency 256 return criterion; the soft heap cap
// http.mem_limit (cmd/http-worker) keeps anon below the unit's MemoryHigh
// instead of the kernel's reclaim storms.
//
// Dead-host cutoff (T-164): apex and www that did not pass the connect phase
// (connection refused or a connect timeout over both protocols)
// finish the task without checking the other subdomains — the dead hosts
// of mass zones stop burning slots with the timeout tail (the
// docs/load-measurements.md §12 measurement: a mean of 11.5 s at a transport
// of 0.54 s/request). The config — http.dead_host_cutoff.
package httpworker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miekg/dns"

	"crawler/internal/content"
	"crawler/internal/dnsclient"
	"crawler/internal/httpfetch"
	"crawler/internal/metrics"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// FetchFunc — the load seam (the dnsclient.exchangeFunc style): one check
// (subdomain × protocol) — one pass of the T-048 redirect chain.
// The production implementation — ChainFetch; the unit tests substitute a scenario without
// the network.
type FetchFunc func(ctx context.Context, req httpfetch.FetchRequest, opts httpfetch.ChainOptions) (*httpfetch.ChainResult, error)

// ChainFetch builds a FetchFunc over the stage's transport (*httpfetch.Fetcher):
// an ordinary httpfetch.FetchChainOpts call. A separate wrapper, not the direct
// use of FetchChainOpts by the worker — the substitution seam in the tests.
func ChainFetch(f *httpfetch.Fetcher) FetchFunc {
	return func(ctx context.Context, req httpfetch.FetchRequest, opts httpfetch.ChainOptions) (*httpfetch.ChainResult, error) {
		return httpfetch.FetchChainOpts(ctx, f, req, opts)
	}
}

// --- the check error classification (spec §8 "an error code is recorded") --------

// ErrorClass — the class of a subdomain check error over a protocol. The codes are stable:
// T-058 puts the class into subdomain_results.error. A class does not drop the domain —
// the error stays in the subdomain's result, the episode's aggregator
// decides the final status (§7, T-058).
type ErrorClass int16

const (
	// ErrNone — a check without an error (a truncated body is noted separately
	// by the Truncated flag, but is also encoded as ErrBody — see below).
	ErrNone ErrorClass = 0
	// ErrTimeout — a timeout of one of the layers (connect/TLS/headers/overall,
	// the anti-tarpit): httpfetch.IsTimeout. A temporary class (spec 6.2:
	// temp_error — "SERVFAIL, timeouts"), a check repeat makes sense.
	ErrTimeout ErrorClass = 1
	// ErrRedirects — the chain interrupted by the max_redirects limit or a loop
	// detection (httpfetch.ErrTooManyRedirects): a permanent site property,
	// not a temp_error (T-048).
	ErrRedirects ErrorClass = 2
	// ErrTLS — a TLS handshake failure due to the server's certificate/protocol
	// (httpfetch.IsTLSCertError): an incorrect certificate does not
	// hinder the load (InsecureSkipVerify, spec 5.5 item 3), this is exactly a refusal
	// to establish the connection.
	ErrTLS ErrorClass = 3
	// ErrBody — the response body: an unsupported/broken Content-Encoding,
	// a zip bomb (httpfetch.ErrDecompressTooLarge) or a truncation by the
	// max_body_bytes cap (the data is partial, the status is received — Available is
	// true at that, spec 8 "huge responses are interrupted by the limits").
	ErrBody ErrorClass = 4
	// ErrNetwork — the other transport errors: a connection refusal, a disconnect,
	// an error resolving the redirect's host. The ctx cancellation in
	// flight also lands here (such a check's result is discarded anyway — see
	// ProcessDomain).
	ErrNetwork ErrorClass = 5
	// ErrCutoff — the check was not performed: the task was finished by the
	// dead-host cutoff (T-164) — apex and www fell at the connect phase
	// (a refusal or a connect timeout over both protocols), the other
	// subdomains were not checked. The class is set only to the synthetic
	// checks of the skipped subdomains (workerSkipSubdomain): it is not
	// a network error and not a check outcome — the class does not
	// reach subdomain_results (a skipped subdomain creates no records, §8), it gives no
	// temp_error (IsTemp false — the episode's repeat is decided by the apex/www timeouts,
	// not by the cutoff).
	ErrCutoff ErrorClass = 6
)

// String returns the class name for the logs.
func (c ErrorClass) String() string {
	switch c {
	case ErrNone:
		return "none"
	case ErrTimeout:
		return "timeout"
	case ErrRedirects:
		return "redirects"
	case ErrTLS:
		return "tls"
	case ErrBody:
		return "body"
	case ErrNetwork:
		return "network"
	case ErrCutoff:
		return "cutoff"
	default:
		return fmt.Sprintf("ErrorClass(%d)", int16(c))
	}
}

// IsTemp reports that the error class is temporary and the check repeat may
// succeed (spec 6.2: temp_error — timeouts). The other classes —
// properties of the site or its environment; a repeat in the near horizon is
// pointless (the regular recheck — T-030).
func (c ErrorClass) IsTemp() bool { return c == ErrTimeout }

// workerClassifyError reduces a load error to a class (the check order
// matters: TooManyRedirects is not a net.Error, the anti-tarpit timeout —
// is one).
func workerClassifyError(err error) ErrorClass {
	switch {
	case err == nil:
		return ErrNone
	case errors.Is(err, httpfetch.ErrTooManyRedirects):
		return ErrRedirects
	case errors.Is(err, httpfetch.ErrDecompressTooLarge):
		return ErrBody
	case httpfetch.IsTimeout(err):
		return ErrTimeout
	case httpfetch.IsTLSCertError(err):
		return ErrTLS
	default:
		return ErrNetwork
	}
}

// --- the "final URL → result" cache (spec 5.5 item 6) --------------------------

// workerCacheKey — the final URLs' cache key: scheme + host (lower
// case — the punycode is guaranteed by the T-048 chain) + path (empty — the root
// "/"). Consistent with the variants criterion of T-054 (NewVariantInput): the query and
// fragment are NOT included — the counter parameters (?i=1, ?i=2, …) do not break
// the deduplication of one page, and the real content difference at one path
// is caught by the hash. The scheme is included: http and https of one address are different downloads,
// substituting the http check's result with an https page would fake
// the availability of a protocol that was actually not checked. An explicitly specified
// non-standard port is included for the same reason: the host:8443 page is
// another service's answer, a cache hit would substitute the default
// port's check result with the unchecked port's content; the schemes'
// default ports (80/443) are normalized away — an explicit :443 at https is the same address.
func workerCacheKey(u *url.URL) string {
	if u == nil {
		return ""
	}
	path := u.Path
	if path == "" {
		path = "/"
	}
	host := strings.ToLower(u.Hostname())
	if p := u.Port(); p != "" && p != workerSchemeDefaultPort(u.Scheme) {
		host += ":" + p
	}
	return strings.ToLower(u.Scheme) + "://" + host + path
}

// workerSchemeDefaultPort — the scheme's default port (the key normalization:
// an explicit :443 at https — the same address as without the port); the other schemes do
// not occur in the pipeline — there is no default.
func workerSchemeDefaultPort(scheme string) string {
	if strings.ToLower(scheme) == "https" {
		return "443"
	}
	return "80"
}

// workerPage — a downloaded page (the chain's outcome) in the cache: the hash and the SPA verdict
// are computed at the read, the body is stored by the early Put (T-147) and does not
// live in memory — the cache hits share the hash and the page's signs, the bytes lie in
// the store (ContentStore.Get by hash). The error class/text (the ErrBody
// of a truncated page) is carried to the cache hit: the same content
// is classified identically regardless of whether the check downloaded it
// or took it from the cache.
type workerPage struct {
	final      *url.URL
	status     int
	header     http.Header
	cookies    []*http.Cookie
	tls        *httpfetch.TLSInfo
	hash       *content.Hash
	stored     bool // the early Put passed: the store reference waits for the record's row (T-147)
	charset    string
	truncated  bool
	responseMS int
	spa        *httpfetch.SPAVerdict
	errClass   ErrorClass
	errText    string
}

// workerPut — an entry of the task's early Put registry (T-147): the stored
// page's hash and the fact of writing new bytes (for the dedup share, spec 11.4
// item 3). The registry is assembled in the task's cache (workerURLCache) as the
// downloads go and passed into DomainResult: ContentSaver.Save reconciles it
// with the records — the references that did not become rows are removed by Delete.
type workerPut struct {
	hash    content.Hash
	created bool
}

// workerURLCache — the domain task's shared cache (spec 5.5 item 6 "inside the
// worker for the domain task"): the final URLs of all the completed checks and the
// early Put registry (T-147). It lives one task: the aliases are a property of a
// pair of subdomains of one domain, between the domains the final URLs do not intersect
// (foreign parkings do not count — reusing a foreign body
// would substitute the TLS/headers of a foreign connection). Concurrency-safe:
// the subdomain checks run in parallel.
type workerURLCache struct {
	mu    sync.Mutex
	pages map[string]*workerPage
	puts  []workerPut // the task's early Puts in the download order (T-147)
}

func workerNewURLCache() *workerURLCache {
	return &workerURLCache{pages: make(map[string]*workerPage, 8)}
}

// get returns the page by the key or nil.
func (c *workerURLCache) get(key string) *workerPage {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pages[key]
}

// put remembers the page by the final URL key.
func (c *workerURLCache) put(key string, p *workerPage) {
	if key == "" || p == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pages[key] = p
}

// addPut appends a successful early Put into the task's registry (T-147).
func (c *workerURLCache) addPut(h content.Hash, created bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.puts = append(c.puts, workerPut{hash: h, created: created})
}

// takePuts returns the accumulated early Put registry (called once
// at the task's end — ProcessDomain moves it into DomainResult).
func (c *workerURLCache) takePuts() []workerPut {
	c.mu.Lock()
	defer c.mu.Unlock()
	puts := c.puts
	c.puts = nil
	return puts
}

// --- the domain result -------------------------------------------------------

// SchemeCheck — the outcome of a subdomain check over one protocol (spec 5.5 items 1,
// 7). An error does not drop the domain: the availability — Available, the result condition —
// ErrClass (§8); the body is not held in memory — the hash,
// the encoding and the SPA verdict remain from it.
type SchemeCheck struct {
	// Scheme — the check's protocol (of the starting request), not the final URL.
	Scheme model.Scheme
	// Available — the check reached the final answer: the server answered with any
	// status, including 4xx/5xx ("the protocol works", the T-054 criterion).
	Available bool
	// Cached — the outcome is taken from the final URLs' cache without the network (spec 5.5 item 6):
	// this check's starting URL matched another task check's final
	// URL (the bare→www classic). The network was not touched, but the primary
	// source's response time (up to its headers) is carried into ResponseMS —
	// the alias class's original record carries the page's real response time;
	// the response time metrics still do not observe the cache hits.
	// The cache-hit state — the matrix rows S4–S6 of the FSM (T-266,
	// schemestate.go).
	Cached bool
	// FinalURL — the final URL (the page at the chain's end, spec 5.5 item 4);
	// nil at a load error. At a cache hit (Cached) the field carries
	// the check's STARTING URL, not the primary source's final URL:
	// the final URL with a query (?i=1 — the cache key does not include the query,
	// workerCacheKey) is written by its query-less form, Redirects —
	// a synthetic single step. A deliberate distortion, not changed
	// (T-266, TZ-fsm spec §3 FR-12; the host/path — the T-054/T-055 criteria —
	// match exactly).
	FinalURL *url.URL
	// Status — the final answer's status code.
	Status int
	// Header — the final answer's headers (a distillate of the exchange: only
	// the final page, the chain's intermediate answers are represented
	// by Redirects). T-058 serializes into subdomain_results.headers.
	Header http.Header
	// Cookies — the final answer's Set-Cookie (spec 5.5 item 7 "cookies").
	Cookies []*http.Cookie
	// TLS — the parse of the final connection's certificate (T-050); nil for http
	// and the http checks' cache hits (not an error, §8).
	TLS *httpfetch.TLSInfo
	// ResponseMS — the time up to the final answer's headers, ms (the
	// last chain Fetch's Elapsed, spec 5.5 item 7 "the response time"). A cache
	// hit carries the primary source's measurement (its Elapsed):
	// 0 — only at load errors (there were no headers).
	ResponseMS int
	// Redirects — the redirect chain (T-048): the URL and the status of every step,
	// the last element — the final page. At an error — the performed
	// part; at a cache hit — a synthetic single step.
	Redirects []httpfetch.RedirectStep
	// ContentHash — the hash of the UTF-8-normalized body (content.HashOf,
	// spec 5.7): the parkings' deduplication and the variants/aliases criterion
	// (T-054/T-055). nil — the body was not received (an error); an empty body's hash is
	// a valid value. The storing itself — T-056.
	ContentHash *content.Hash
	// Truncated — the body is truncated by the max_body_bytes cap (T-049): the data
	// is partial, not a network error; the hash is computed over the truncated bytes.
	Truncated bool
	// Charset — the canonical name of the response's encoding (ToUTF8, T-051).
	Charset string
	// ContentStored — the page body is stored by an early Put into the ContentStore
	// (T-147): the check's hash is a live store reference, a candidate for the
	// record's content_hash. false — a truncated body (not stored
	// at all), a disk overflow or the max_store_bytes limit (the content
	// write pause, spec 8: the domain result is not dropped). The body
	// itself is not held in memory (T-147): the bytes are in the store, Get by
	// hash; an empty body is valid stored content (T-138).
	ContentStored bool
	// SPA — the heuristic's verdict over the body (DetectSPA, T-057); nil at a load
	// error. The q:render publication over it is done by the T-066 render pipeline
	// (renderq.go) in the result write chain — on the queue's
	// overflow the domain receives the render_pending flag and is reworked later.
	SPA *httpfetch.SPAVerdict
	// ErrClass — the error class (ErrNone — no error; a body truncation — ErrBody
	// at Available=true). ErrText — the original text for the logs and parsing.
	ErrClass ErrorClass
	ErrText  string
	// DeadDial — the check fell at the starting host's connect phase:
	// connection refused or a connect timeout before the chain's first answer
	// (httpfetch.IsConnectFailure; the performed part of the redirects is empty —
	// a failure on the first request to its own host, not on a redirect hop,
	// T-164). The dead-host cutoff signature: there is no connection to the host at
	// the TCP level — not a failure after establishing (TLS, headers, body — there
	// the connection already existed). Never at an available check.
	DeadDial bool
	// storeErr — an early Put failure that is not the content write pause
	// (T-147): no "stored" nor "skipped" can be matched to the page —
	// the episode honestly fails (ProcessDomain returns an error, a repeated
	// delivery, spec 3.4). The field is internal: filled by checkScheme,
	// read by ProcessDomain; the cache hits do not inherit it (the error
	// belongs to its own write attempt).
	storeErr error
}

// Record — the stored subdomain-result record after the variant logic and
// the aliases: the subdomain_results columns (spec 5.5 item 7, §6.1). The record's data —
// from the source check (Scheme; variant 0 at folded protocols carries the
// https check's data, T-054); T-058 writes into the DB.
type Record struct {
	// Variant — 0 main, 1 http (T-054).
	Variant model.Variant
	// Scheme — the record's source check's protocol.
	Scheme model.Scheme
	// SubdomainID, Label — the record's identity (for the consumer's reference).
	SubdomainID int16
	Label       string
	// FinalURL — the record's final URL (as a string, as in subdomain_results.
	// final_url); Host/Path — its normalized representation for
	// comparisons (the T-054/T-055 criteria).
	FinalURL string
	Host     string
	Path     string
	// HTTPStatus, Header, Cookies, TLS, ResponseMS, Redirects — the
	// source check's data (see SchemeCheck).
	HTTPStatus int
	Header     http.Header
	Cookies    []*http.Cookie
	TLS        *httpfetch.TLSInfo
	ResponseMS int
	Redirects  []httpfetch.RedirectStep
	// ContentHash, Truncated, Charset, SPA — the source check's content.
	ContentHash *content.Hash
	Truncated   bool
	Charset     string
	SPA         *httpfetch.SPAVerdict
	// ContentStored — the source check's body is stored by an early Put
	// (T-147): at the originals with a hash without truncation the Put's
	// reference becomes the subdomain_results row's reference (ContentSaver,
	// T-056). false at an original — the content is not stored (a disk
	// overflow, the max_store_bytes limit): the record lives with content_hash = nil
	// and ContentNotStored. The body is not stored in the record (T-147) — the bytes
	// are available by the store over the hash; an empty body is valid content
	// (T-138).
	ContentStored bool
	// ContentNotStored — the body was received (the hash computed), but the content is NOT
	// stored: a disk overflow (ErrStoreFull, spec 8), the
	// max_store_bytes limit (spec 5.5) or a body truncation by the max_body_bytes cap.
	// The record lives with content_hash = NULL: content_hash is a reference into
	// contents (§6.1), without a block in the store there is no row; a repeated
	// domain check (recheck, T-030) will download the page and store it.
	// Not set for aliases: their content — in the original record (T-055).
	ContentNotStored bool
	// AliasKind, AliasSubdomainID, StoreContent — the DetectAliases
	// decision (T-055): the kind of the link with the original subdomain and the sign "the
	// content is stored once at the original" (the storing itself — T-056).
	AliasKind        model.AliasKind
	AliasSubdomainID *int16
	StoreContent     bool
	// ErrClass — the source check's error class (ErrBody at a truncated
	// body with a live status; usually ErrNone for available records).
	ErrClass ErrorClass
}

// SubdomainResult — a task subdomain's outcome: both protocol checks and
// the records the variant logic created (T-054).
type SubdomainResult struct {
	SubdomainID int16
	Label       string
	// Hostname — the subdomain's full name (label + "." + domain), punycode.
	Hostname string
	// Addrs, CNAME — the DNS stage's data from the task (spec 5.4 item 4), carried
	// into the result (spec 5.5 item 7 "everything is stored: the addresses, the CNAME").
	Addrs []netip.Addr
	CNAME string
	// HTTPS, HTTP — the checks over the protocols (the field order — the
	// launch order in the pool).
	HTTPS SchemeCheck
	HTTP  SchemeCheck
	// Records — the records after the variant logic and the aliases; empty — the
	// protocols did not answer (a contribution into "no one answered", §7).
	Records []Record
}

// DomainResult — the full outcome of the domain's HTTP episode (spec 5.5, §7): the generalized
// structure for the T-058 write — the domain status (alive/no_http/temp_error by
// §6.2/§7), the subdomain_results rows, the q:render enqueueing.
type DomainResult struct {
	ZoneID   int16
	DomainID int64
	Name     string
	// Subdomains — the q:http task's order.
	Subdomains []SubdomainResult
	// Cutoff — the task was finished by the dead-host cutoff (T-164): apex and www
	// fell at the connect phase over both protocols, the other subdomains were not
	// checked (their synthetic checks — the ErrCutoff class; no
	// records, §8). Observability: the dead_host_cutoff bit in domains.flags
	// (the T-058 result write), the
	// crawler_http_dead_host_cutoff_* counters (metrics.go) and the worker's log —
	// the false cutoffs' control sample is built over them (the T-164 criterion:
	// a share < 1%, a repeated full check of the cutoff domains).
	Cutoff bool
	// puts — the task's early Put registry (T-147, not for the consumer):
	// filled by ProcessDomain, ContentSaver.Save reconciles it with
	// the records — the references that did not become rows are removed by Delete.
	puts []workerPut
}

// RecordCount — the number of result records (subdomain × variant).
func (r *DomainResult) RecordCount() int {
	n := 0
	for i := range r.Subdomains {
		n += len(r.Subdomains[i].Records)
	}
	return n
}

// HasRecords — whether at least one subdomain answered over at least one protocol
// (§7: "no one answered → status=no_http"; T-058 will assign the status).
func (r *DomainResult) HasRecords() bool { return r.RecordCount() > 0 }

// AnyTimeout — whether there is a timeout class among the checks (ErrTimeout): the
// data for the T-058 decision between no_http (the web is silent constantly) and temp_error
// ("SERVFAIL, timeouts", spec 6.2) — for a domain that "went silent" entirely
// with timeouts, the episode's repeat is meaningful.
func (r *DomainResult) AnyTimeout() bool {
	for i := range r.Subdomains {
		if r.Subdomains[i].HTTPS.ErrClass == ErrTimeout || r.Subdomains[i].HTTP.ErrClass == ErrTimeout {
			return true
		}
	}
	return false
}

// --- the worker ------------------------------------------------------------------

// The worker's defaults.
const (
	// DefaultGroup — the q:http consumer group (spec 4.2: consumer groups
	// scale the workers horizontally).
	DefaultGroup = "http-worker"

	// DefaultConcurrency — concurrent q:http tasks (domains) per
	// instance in the Run loop. A modest number against http.concurrency=2000:
	// the global connection and RPS cap is held by the transport (T-049,
	// Options.Limits — the GlobalConns semaphore), the tasks merely fill it;
	// every task holds the domain result in memory — the headers, the hashes and
	// the metadata (the page bodies are not held: the early Put T-147),
	// the bodies being read are bounded by PerDomain × max_body_bytes — a typical
	// task is a few MiB. The load scaling — by instances (spec 4.1
	// "scales by the number of goroutines and processes"), the cap
	// is raised by Options.Concurrency.
	//
	// The number — in sync with the config default http.task_concurrency
	// (cmd/http-worker; the TestWorkerOptionsDefaults test catches a divergence
	// — a silently diverged default would change the production instance's behavior
	// against the documentation). 192 (T-157; the original 256 — the T-092 measurements
	// §3/§11): a day on 256 slots — two unit fatals
	// (load-measurements.md §12: an OOM kill at 05:48, reaching the
	// 900.5 MiB = MemoryMax peak at 09:17; MemoryHigh 850M — 120,095
	// reclaim events) at ≈3.3 MiB anon per slot (the bodies were held until
	// the task's end — until the early Put T-147); 192 are stable (the wave 21
	// soak: 860–893 MiB, oom=0, NRestarts=0), the rate ≈17–19 domains/s.
	// T-163 closed the profile's remainder (the slot measurement metrics + the soft
	// heap cap http.mem_limit against the MemoryHigh reclaim storms);
	// the return to 256 — by the sampler measurement after the deploy (the criterion
	// ≤1.5 MiB/slot and a day without MemoryHigh throttling), not by code.
	DefaultConcurrency = 192

	// DefaultConsume — q:http records per one read: the size of the
	// XREADGROUP chunk and of the sliding consumption's prefetch buffer (T-139) —
	// the read runs ahead of the processing by no more than a couple of chunks (the current one
	// in the reader's hand + the buffer), the next read happens upon freeing
	// the task slots (the dispatcher takes a delivery from the buffer only when
	// there is a slot), the read window does not bound the parallelism.
	DefaultConsume = 16

	// DefaultPerDomain — parallel checks inside a domain task
	// (subdomain × protocol). Subdomains ≤ 63 (§5.10), twice as many
	// checks — but a moderate pool: one domain's subdomains almost always
	// live on the same IPs (one server, CNAME hostings), and
	// the real parallelism of connecting to one IP is already bounded
	// by the transport's per-IP semaphore (3–4, spec 5.5); 8 = the doubled per-IP
	// cap covers the domains with several addresses (apex/www
	// separately, CNAME targets) and two checks of one subdomain, without granting
	// one domain an unfair share of the global budget.
	DefaultPerDomain = 8

	// DefaultErrorWait — the pause after Redis errors (the T-039 style):
	// idle rounds do not warm the logs, the unacknowledged deliveries will be returned by
	// XAUTOCLAIM (T-025).
	DefaultErrorWait = 30 * time.Second
)

// workerFailWaitBase — the starting pause of the work loop's
// fuse (Run): deliveries left in a row without an XACK in the number of a whole read
// chunk (Consume, T-139) at a systematic processing failure —
// Options.OnResult/the DB — suspend the q:http reading with a doubling
// of the pause up to DefaultErrorWait. Without the pause the loop would keep drawing
// tasks: every delivery burned an attempt (XAUTOCLAIM after
// DefaultClaimMinIdle, T-025) and carried it into q:dead. A variable, not
// a constant: the unit tests shrink the pause without waiting seconds.
var workerFailWaitBase = time.Second

// workerFailBackoff — the pause after a streak of endured fuse
// pauses in a row (the series grows until the reader sees no
// success — the failure counter is reset by an acknowledged delivery):
// a doubling from workerFailWaitBase, the DefaultErrorWait cap (the
// shift is bounded — a Duration overflow).
func workerFailBackoff(streak int) time.Duration {
	d := workerFailWaitBase << min(streak-1, 5)
	if d > DefaultErrorWait {
		d = DefaultErrorWait
	}
	return d
}

// Options — the worker's parameters; the zero values are replaced by the defaults
// (withDefaults, the dnsworker.Options style). The production build (cmd/http-worker)
// takes Concurrency/Consume/PerDomain from the http.task_* configuration
// (internal/config, T-139); here the same defaults — for the isolated
// builds and tests.
type Options struct {
	// Concurrency — concurrent q:http tasks in the Run loop; 0 —
	// DefaultConcurrency. Config: http.task_concurrency.
	Concurrency int
	// Consume — q:http records per one read (the XREADGROUP chunk and the
	// sliding consumption's prefetch buffer, T-139); 0 — DefaultConsume.
	// Config: http.task_consume.
	Consume int
	// Group — the q:http consumer group; "" — DefaultGroup.
	Group string
	// Consumer — the group's consumer name; "" — "http-worker-<pid>".
	Consumer string
	// PerDomain — parallel checks (subdomain × protocol) inside
	// the domain task; 0 — DefaultPerDomain. Config: http.task_per_domain.
	PerDomain int
	// DeadHostCutoff — the dead-host cutoff (T-164): apex and www that did not pass
	// the connect phase (connection refused or a connect timeout — both
	// protocols) finish the task without checking the other subdomains:
	// the dead hosts of mass zones stop burning slots with the timeout tail
	// (the §12 measurement: a mean of 11.5 s at a transport of 0.54 s/request, 60% of tasks in
	// the 10–30 s bucket). The skipped subdomains' records — by §8 ("no
	// answer": no records), their checks — the synthetic ErrCutoff class.
	// false — the former behavior: all the task's subdomains are checked.
	// Config: http.dead_host_cutoff.
	DeadHostCutoff bool
	// Store — the content store for the early body storing (T-147):
	// every downloaded page goes into a Put by hash immediately on receipt
	// and is not held in the slot's memory (the alias deduplication goes by
	// the hashes, T-055). Production — *content.PackStore (the same as
	// ContentSaver's); nil — an error: without the early Put the records'
	// bodies would accumulate in memory until the task's end (≈3.34 MiB/slot, the
	// T-092 §11 measurement) — a silent rollback of the optimization is worse than an explicit start refusal.
	Store content.ContentStore
	// MaxRedirects — the redirect chain's limit (http.max_redirects,
	// T-048): 0 — do not follow at all; negative values — the default
	// of the spec 5.5 limits table (httpfetch.DefaultMaxRedirects).
	MaxRedirects int
	// Recover — the stuck deliveries' return layer (Reliab, T-025): Run
	// raises a background RecoverLoop (XAUTOCLAIM → the same processing →
	// XACK) with the same Group/Consumer; nil — no recovery
	// (isolated builds and tests without Redis specifics).
	Recover *queue.Reliab
	// RecoverBatch — the RecoverLoop's claim chunk per pass; 0 — the
	// queue's default (queue.DefaultRecoverBatch). The production build passes
	// redis.recover_batch (T-153): the stuck deliveries' return rate — a queue-configured
	// value, not the stage's consumption window.
	RecoverBatch int
	// RecoverInterval — the RecoverLoop's stuck records polling period;
	// 0 — the queue's default (queue.DefaultRecoverInterval). Config —
	// redis.recover_interval (T-153).
	RecoverInterval time.Duration
	// Log — the logger; nil — the records are suppressed (slog.DiscardHandler).
	Log *slog.Logger
	// Metrics — the service's Prometheus registry (T-077, spec 5.9): from it New
	// builds the stage's instruments (NewMetrics); nil — without metrics (isolated
	// builds and tests).
	Metrics *metrics.Registry
	// OnResult — the ready domain result's handler, called in
	// the Run loop AFTER the processing and BEFORE the XACK. The service build (cmd/http-worker)
	// puts the chain here: ContentSaver (T-056) → Recorder — the write
	// of subdomain_results, the domain status and the q:analyze publication (T-058).
	// nil — the result is assembled and discarded (isolated tests).
	// The handler's error — a delivery without an XACK, it will return with a repeated
	// delivery (at-least-once, spec 3.4).
	OnResult func(ctx context.Context, res *DomainResult) error
}

func (o Options) withDefaults() Options {
	if o.Concurrency <= 0 {
		o.Concurrency = DefaultConcurrency
	}
	if o.Consume <= 0 {
		o.Consume = DefaultConsume
	}
	if o.Group == "" {
		o.Group = DefaultGroup
	}
	if o.Consumer == "" {
		o.Consumer = fmt.Sprintf("http-worker-%d", os.Getpid())
	}
	if o.PerDomain <= 0 {
		o.PerDomain = DefaultPerDomain
	}
	if o.MaxRedirects < 0 {
		o.MaxRedirects = httpfetch.DefaultMaxRedirects
	}
	return o
}

func (o Options) log() *slog.Logger {
	if o.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return o.Log
}

// Worker — the HTTP worker: processes the q:http tasks (domains) through the
// FetchFunc load seam. Concurrency-safe: the state is immutable after New.
type Worker struct {
	fetch FetchFunc
	opt   Options
	met   *Metrics // the stage's instruments (T-077); nil — no-op
}

// New creates the worker over the fetch load seam (production — ChainFetch).
func New(fetch FetchFunc, opts Options) (*Worker, error) {
	if fetch == nil {
		return nil, errors.New("httpworker: шов загрузки не задан (ChainFetch)")
	}
	if opts.Store == nil {
		return nil, errors.New("httpworker: хранилище контента не задано (раннее сохранение тел, T-147)")
	}
	return &Worker{fetch: fetch, opt: opts.withDefaults(), met: NewMetrics(opts.Metrics)}, nil
}

// workerSub — a parsed task subdomain: the addresses are parsed once.
type workerSub struct {
	id    int16
	label string
	host  string
	addrs []netip.Addr
	cname string
}

// workerPrepareTask validates the task (queue.Decode already checked it in
// the loop — here the guard of direct ProcessDomain calls) and parses the addresses.
func workerPrepareTask(task queue.HTTPTask) ([]workerSub, error) {
	if strings.TrimSpace(task.Name) == "" {
		return nil, errors.New("httpworker: задача без имени домена")
	}
	if len(task.Subdomains) == 0 {
		return nil, errors.New("httpworker: задача без субдоменов (пустая q:http не публикуется, T-024)")
	}
	seen := make(map[int16]struct{}, len(task.Subdomains))
	subs := make([]workerSub, 0, len(task.Subdomains))
	for i, s := range task.Subdomains {
		if err := model.ValidateSubdomainID(int(s.SubdomainID)); err != nil {
			return nil, fmt.Errorf("httpworker: субдомен №%d: %w", i+1, err)
		}
		if _, dup := seen[s.SubdomainID]; dup {
			return nil, fmt.Errorf("httpworker: субдомен №%d: дубликат subdomain_id %d", i+1, s.SubdomainID)
		}
		seen[s.SubdomainID] = struct{}{}
		if strings.ContainsAny(s.Label, ". ") {
			return nil, fmt.Errorf("httpworker: субдомен №%d: метка %q — одна DNS-метка", i+1, s.Label)
		}
		host := task.Name
		if s.Label != "" {
			host = s.Label + "." + task.Name
		}
		addrs := make([]netip.Addr, 0, len(s.Addrs))
		for _, a := range s.Addrs {
			addr, err := netip.ParseAddr(a)
			if err != nil {
				return nil, fmt.Errorf("httpworker: субдомен №%d: адрес %q: %w", i+1, a, err)
			}
			addrs = append(addrs, addr)
		}
		if len(addrs) == 0 {
			return nil, fmt.Errorf("httpworker: субдомен №%d: нет адресов", i+1)
		}
		subs = append(subs, workerSub{id: s.SubdomainID, label: s.Label, host: host, addrs: addrs, cname: s.CNAME})
	}
	return subs, nil
}

// ProcessDomain processes the q:http task in full (spec 5.5 items 1, 6, §7):
// the subdomain checks over both protocols in parallel (the PerDomain pool) with
// a shared final URL cache → the variant logic (T-054) → the aliases (T-055).
// With the dead-host cutoff on (Options.DeadHostCutoff, T-164) the checks
// go in two waves: apex and www — the first wave, a dead connect phase
// of both over both protocols (DeadDial) finishes the task without checking
// the other subdomains (the records — by §8, observability — Cutoff/ErrCutoff).
// The individual checks' errors stay in their results and do not drop the domain
// (§8); a return error — only an unfit task or a ctx cancellation (the episode is not
// finished, the q:http delivery is not acknowledged and will repeat, spec 3.4).
func (w *Worker) ProcessDomain(ctx context.Context, task queue.HTTPTask) (*DomainResult, error) {
	subs, err := workerPrepareTask(task)
	if err != nil {
		return nil, err
	}

	res := &DomainResult{
		ZoneID:     task.ZoneID,
		DomainID:   task.DomainID,
		Name:       task.Name,
		Subdomains: make([]SubdomainResult, len(subs)),
	}
	cache := workerNewURLCache()

	// The goroutine pool: the PerDomain semaphore is acquired before the launch (the
	// goroutine queue does not grow, the dnsworker.processBatch style); the writes into different
	// SubdomainResult fields — different memory cells, no race.
	sem := make(chan struct{}, w.opt.PerDomain)
	var wg sync.WaitGroup
	// launch — launches the task subdomain's checks (https and http) in the pool;
	// the single launch point — works both in one wave (cutoff
	// off) and in two (the apex/www head → the tail, T-164).
	launch := func(i int, sub workerSub) {
		res.Subdomains[i] = SubdomainResult{
			SubdomainID: sub.id,
			Label:       sub.label,
			Hostname:    sub.host,
			Addrs:       sub.addrs,
			CNAME:       sub.cname,
		}
		// https and http of one subdomain — independent checks; every check's
		// Scheme is fixed into its result by checkScheme itself.
		wg.Add(2)
		sem <- struct{}{}
		go func(dst *SchemeCheck, sub workerSub) {
			defer wg.Done()
			defer func() { <-sem }()
			*dst = *w.checkScheme(ctx, sub.host, sub.addrs, model.SchemeHTTPS, cache)
		}(&res.Subdomains[i].HTTPS, sub)
		sem <- struct{}{}
		go func(dst *SchemeCheck, sub workerSub) {
			defer wg.Done()
			defer func() { <-sem }()
			*dst = *w.checkScheme(ctx, sub.host, sub.addrs, model.SchemeHTTP, cache)
		}(&res.Subdomains[i].HTTP, sub)
	}

	if w.opt.DeadHostCutoff {
		// Two waves (T-164): apex and www fly first — their outcome decides
		// whether to check the others. A dead connect phase of both over both
		// protocols — the dead host's signature: the other subdomains are not
		// checked, the task finishes with the available result.
		head, rest := workerCutoffSplit(subs)
		for _, i := range head {
			launch(i, subs[i])
		}
		wg.Wait()
		if workerDeadHostHead(res, head) && len(rest) > 0 {
			res.Cutoff = true
			for _, i := range rest {
				workerSkipSubdomain(&res.Subdomains[i], subs[i])
			}
			w.opt.log().Info("dead-host cutoff: apex и www не прошли connect-фазу — субдомены не проверяются",
				"domain", task.Name, "skipped_subdomains", len(rest), "skipped_checks", 2*len(rest))
		} else {
			for _, i := range rest {
				launch(i, subs[i])
			}
			wg.Wait()
		}
	} else {
		for i := range subs {
			launch(i, subs[i])
		}
		wg.Wait()
	}

	// A cancellation/deadline — the episode is not finished: a result with the failed
	// checks would look like "the web is silent" and give a false no_http;
	// an honest failure will return the task with a repeated delivery (spec 3.4).
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("httpworker: домен %s: отмена: %w", task.Name, err)
	}

	// An early Put failure that is not the content write pause (a
	// DB/FS error, a cancellation) drops the task (T-147): the content received no
	// reference, silence would drop it until the T-030 recheck (months); a repeated
	// delivery will replay the episode entirely (spec 3.4). The Put references of this
	// attempt, not removed by the ContentSaver reconciliation, remain — a leak in
	// the safe direction (the same class as a crash between the Put and the row's
	// commit at SwapRef, T-044).
	for i := range res.Subdomains {
		for _, c := range []*SchemeCheck{&res.Subdomains[i].HTTPS, &res.Subdomains[i].HTTP} {
			if c.storeErr != nil {
				return nil, fmt.Errorf("httpworker: домен %s: сохранение контента: %w", task.Name, c.storeErr)
			}
		}
	}

	// The variant logic (T-054) per every subdomain; the aliases (T-055) — over
	// all the domain's records at once (the equivalence classes glue
	// subdomains together, §5.5 item 6). The records' pointers are taken from the already assembled
	// result (an append into a local slice with a reallocation would tear them away from
	// SubdomainResult.Records).
	var (
		aliasIn  []httpfetch.AliasInput
		aliasRec []*Record
	)
	for i := range res.Subdomains {
		sub := &res.Subdomains[i]
		httpsIn := httpfetch.NewVariantInput(model.SchemeHTTPS, sub.HTTPS.Available, sub.HTTPS.FinalURL, sub.HTTPS.ContentHash)
		httpIn := httpfetch.NewVariantInput(model.SchemeHTTP, sub.HTTP.Available, sub.HTTP.FinalURL, sub.HTTP.ContentHash)
		variants, err := httpfetch.DecideVariants(httpsIn, httpIn)
		if err != nil {
			return nil, fmt.Errorf("httpworker: домен %s: субдомен %s: %w", task.Name, sub.Hostname, err)
		}
		recs := make([]Record, 0, len(variants))
		for _, vr := range variants {
			src := &sub.HTTPS
			if vr.Scheme == model.SchemeHTTP {
				src = &sub.HTTP
			}
			recs = append(recs, Record{
				Variant:       vr.Variant,
				Scheme:        vr.Scheme,
				SubdomainID:   sub.SubdomainID,
				Label:         sub.Label,
				FinalURL:      src.FinalURL.String(),
				Host:          src.FinalURL.Hostname(),
				Path:          src.FinalURL.Path,
				HTTPStatus:    src.Status,
				Header:        src.Header,
				Cookies:       src.Cookies,
				TLS:           src.TLS,
				ResponseMS:    src.ResponseMS,
				Redirects:     src.Redirects,
				ContentHash:   src.ContentHash,
				Truncated:     src.Truncated,
				Charset:       src.Charset,
				SPA:           src.SPA,
				ContentStored: src.ContentStored,
				ErrClass:      src.ErrClass,
				AliasKind:     model.AliasNone,
			})
		}
		sub.Records = recs
		for j := range sub.Records {
			r := &sub.Records[j]
			aliasIn = append(aliasIn, httpfetch.AliasInput{
				SubdomainID: sub.SubdomainID,
				Label:       sub.Label,
				FinalHost:   r.Host,
				FinalPath:   r.Path,
				Scheme:      r.Scheme,
				Variant:     r.Variant,
				ContentHash: workerHashBytes(r.ContentHash),
			})
			aliasRec = append(aliasRec, r)
		}
	}

	// DetectAliases returns the decision parallel to the input.
	for i, ar := range httpfetch.DetectAliases(task.Name, aliasIn) {
		rec := aliasRec[i]
		rec.AliasKind = ar.AliasKind
		rec.AliasSubdomainID = ar.AliasSubdomainID
		rec.StoreContent = ar.StoreContent
	}

	// The stage boundary (T-266, TZ-fsm spec §3 FR-12): the domain result is assembled
	// in full — all the checks and records are run through the scheme check state
	// FSM's centralized validators (schemestate.go,
	// validateResult) before passing into Options.OnResult (the first link —
	// ContentSaver, T-056). An illegal field combination — a result
	// construction bug: the q:http delivery returns without an XACK
	// (spec 3.4), garbage does not reach the DB.
	if err := validateResult(res); err != nil {
		return nil, fmt.Errorf("httpworker: домен %s: %w", task.Name, err)
	}

	// The early Put registry (T-147) — into the result: ContentSaver.Save will reconcile
	// it with the records (the references that did not become rows will be removed by Delete).
	// The checks' and records' bodies are no longer in memory by this moment (T-147):
	// the early Put released them at the receipt.
	res.puts = cache.takePuts()
	return res, nil
}

// workerCutoffHeadLabel — the dead-host cutoff's head subdomain (T-164):
// the apex (the "" label) and www — the site's main names. The DNS case is insignificant —
// a case-insensitive comparison (the task's labels are already lower case, the
// check guards the direct ProcessDomain calls).
func workerCutoffHeadLabel(label string) bool {
	return label == "" || strings.EqualFold(label, "www")
}

// workerCutoffSplit divides the task's subdomains into the head (the apex and www) and
// the tail (the others) for the dead-host cutoff (T-164): the head is checked
// by the pool's first wave, the tail — by the second or not checked at all.
// The order inside the parts — the q:http task's order.
func workerCutoffSplit(subs []workerSub) (head, rest []int) {
	head = make([]int, 0, 2)
	for i := range subs {
		if workerCutoffHeadLabel(subs[i].label) {
			head = append(head, i)
		}
	}
	rest = make([]int, 0, len(subs)-len(head))
	for i := range subs {
		if !workerCutoffHeadLabel(subs[i].label) {
			rest = append(rest, i)
		}
	}
	return head, rest
}

// workerDeadHostHead — the dead host's signature (T-164): the task has
// BOTH head names (exactly two — the task contains no label duplicates) and
// every head check (https and http) finished with a connect-phase refusal
// (DeadDial): both main hosts are TCP-dead over both protocols — burning
// the task's slots on the other subdomains is pointless. Any outcome other than
// a dead dial (a live check, a TLS failure, a headers/overall timeout,
// the redirects limit) removes the signature — the domain is checked in full.
func workerDeadHostHead(res *DomainResult, head []int) bool {
	if len(head) != 2 {
		return false // no apex+www pair — the signature is not assembled
	}
	for _, i := range head {
		sub := &res.Subdomains[i]
		if !sub.HTTPS.DeadDial || !sub.HTTP.DeadDial {
			return false
		}
	}
	return true
}

// workerSkipSubdomain fills the result of a subdomain that was not checked
// because of the dead-host cutoff (T-164): the task's header (the identity,
// the DNS stage's addresses) is preserved, the checks — synthetic of the
// ErrCutoff class (observability: Episode counts the skipped checks
// by a separate counter, the text — the reason), no records — exactly like at
// a subdomain whose checks passed and were silent (§8: both protocols
// unavailable → no records; the contribution into the domain's status — the T-058
// aggregator decides over the performed apex/www checks).
func workerSkipSubdomain(dst *SubdomainResult, sub workerSub) {
	const note = "dead-host cutoff (T-164): apex и www не прошли connect-фазу — субдомен не проверялся"
	dst.SubdomainID = sub.id
	dst.Label = sub.label
	dst.Hostname = sub.host
	dst.Addrs = sub.addrs
	dst.CNAME = sub.cname
	dst.HTTPS = SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: ErrCutoff, ErrText: note}
	dst.HTTP = SchemeCheck{Scheme: model.SchemeHTTP, ErrClass: ErrCutoff, ErrText: note}
	dst.Records = nil
}

// workerHashBytes — the hash as a slice for AliasInput.ContentHash (nil — without
// content, the T-055 contract); a copy — the record in the result stays the owner.
func workerHashBytes(h *content.Hash) []byte {
	if h == nil {
		return nil
	}
	b := make([]byte, len(h))
	copy(b, h[:])
	return b
}

// workerBodyHardLimit — the worker's own guard cap of the body read
// for the case of a transport without the T-049 limits (isolated builds and
// unit tests without Options.Limits): an unlimited read would bloat the memory
// on a malicious answer (§9 — untrusted content). With the limits on
// (the production NewOptions builder always includes them) the body is already
// wrapped by the BodyLimiter with the http.max_body_bytes cap — arbitrarily
// above the default, — and the read runs under it without the worker's own cap:
// the guard must not substitute the configured limit.
const workerBodyHardLimit = httpfetch.DefaultLimitBodyBytes

// checkScheme — one check (subdomain × protocol, spec 5.5 item 1): the final
// URLs' cache → the redirect chain (the fetch seam) → the body read with a cap →
// UTF-8 (T-051) → the hash and the SPA verdict (T-057). All the errors — into the
// check's result; the ctx cancellation is visible to the caller via ctx.Err() after the pool.
func (w *Worker) checkScheme(
	ctx context.Context,
	host string,
	addrs []netip.Addr,
	scheme model.Scheme,
	cache *workerURLCache,
) *SchemeCheck {
	check := &SchemeCheck{Scheme: scheme}
	start := &url.URL{Scheme: scheme.String(), Host: host, Path: "/"}

	// The final URLs' cache (spec 5.5 item 6): this check's starting URL is already
	// downloaded by another task check (bare→www) — a second load is
	// not needed. The cache outcome is honestly marked Cached: the network was not touched.
	if page := cache.get(workerCacheKey(start)); page != nil {
		check.Available = true
		check.Cached = true
		check.FinalURL = start
		check.Status = page.status
		check.Header = page.header
		check.Cookies = page.cookies
		check.TLS = page.tls
		check.Redirects = []httpfetch.RedirectStep{{URL: start.String(), Status: page.status}}
		check.ResponseMS = page.responseMS
		check.ContentHash = page.hash
		check.Truncated = page.truncated
		check.Charset = page.charset
		check.ContentStored = page.stored
		check.SPA = page.spa
		// The page's error (a truncation — ErrBody) is carried by the cache outcome:
		// a record of the same content carries the same class as the
		// primary source's (otherwise subdomain_results.error would diverge
		// from the cache hit, the metrics would undercount the error).
		check.ErrClass = page.errClass
		check.ErrText = page.errText
		return check
	}

	chain, err := w.fetch(ctx, httpfetch.FetchRequest{URL: start, Addrs: addrs}, httpfetch.ChainOptions{MaxRedirects: w.opt.MaxRedirects})
	if err != nil {
		check.ErrClass = workerClassifyError(err)
		check.ErrText = err.Error()
		if chain != nil {
			check.Redirects = chain.Chain // the performed part (T-048)
		}
		// A dead connect of the starting host (T-164): a connect-phase refusal AND
		// not a single completed chain step — the first request to its own host
		// did not reach TCP. A dial refusal of the redirect's host (the performed
		// part is not empty) does not count as the signature: the starting host answered,
		// the hop's target fell.
		check.DeadDial = len(check.Redirects) == 0 && httpfetch.IsConnectFailure(err)
		return check
	}
	if chain == nil || chain.Result == nil {
		// A redirect limit/loop: Result nil, Chain — the performed part.
		check.ErrClass = ErrRedirects
		check.ErrText = httpfetch.ErrTooManyRedirects.Error()
		if chain != nil {
			check.Redirects = chain.Chain
		}
		return check
	}

	res := chain.Result
	check.Available = true
	check.FinalURL = res.FinalURL
	check.Status = res.StatusCode
	check.Header = res.Header
	check.Redirects = chain.Chain
	check.ResponseMS = int(res.Elapsed / time.Millisecond)
	check.TLS = res.TLSInfo
	for _, line := range res.Header.Values("Set-Cookie") {
		if c, err := http.ParseSetCookie(line); err == nil && c != nil {
			check.Cookies = append(check.Cookies, c)
		}
	}

	// The body: read entirely, closed always (the request's overall
	// timeout lives until the Close, T-047), converted to UTF-8 (T-051);
	// further only the hash (T-054/T-055/T-056) and the SPA verdict
	// (T-057) are needed. The cap — the BodyLimiter T-049, when the limits are on:
	// the read is cut by http.max_body_bytes (the sign — Truncated),
	// the anti-tarpit is there too; without the limiter — the
	// workerBodyHardLimit guard against an untrusted answer.
	var (
		body      []byte
		readErr   error
		truncated bool
	)
	body, readErr = workerReadBody(res)
	if res.BodyLimiter != nil {
		truncated = res.BodyLimiter.Truncated()
	} else {
		truncated = int64(len(body)) > workerBodyHardLimit
		if truncated {
			body = body[:workerBodyHardLimit]
		}
	}
	closeErr := res.Body.Close()
	if readErr == nil {
		readErr = closeErr
	}
	if readErr != nil {
		// The read broke after the headers (a tarpit timeout, a disconnect):
		// the protocol answered, there is no page — Available is extinguished, the partial
		// status/chain remain for parsing (§8).
		check.Available = false
		check.ErrClass = workerClassifyError(readErr)
		check.ErrText = readErr.Error()
		return check
	}
	if truncated {
		check.Truncated = true
		check.ErrClass = ErrBody
		check.ErrText = "тело обрезано потолком max_body_bytes (данные частичны)"
	}

	// The body hold window (T-163): from completing the read to the end of the
	// processing (UTF-8 → hash → SPA → the early Put) the bytes live in the slot —
	// this is the slot memory's transit component, the
	// crawler_http_body_bytes_inflight gauge sees it (the criterion's sampler measurement);
	// the read size — into the mix histogram (truncated bodies enter
	// as they are: the cap cuts them too). The size is fixed before leaving
	// the body's scope (below body = nil).
	held := int64(len(body))
	w.met.BodyHeld(held)
	defer func() { w.met.BodyHeld(-held) }()
	w.met.BodySize(len(body))

	cs := httpfetch.ToUTF8(res.Header, body)
	check.Charset = cs.Charset
	// The body's original bytes are no longer needed (T-163): when decoding
	// a non-UTF-8 encoding ToUTF8 returned a copy — the raw bytes are released
	// here, before the hash/SPA/Put; the fast path without a copy shares the slice with
	// cs.UTF8, the assignment changes nothing. The hold accounting above goes
	// by the fixed held, not by the slice.
	body = nil
	hash := content.HashOf(cs.UTF8)
	check.ContentHash = &hash
	spa := httpfetch.DetectSPA(cs.UTF8)
	check.SPA = &spa

	// The body's early storing (T-147): the bytes go into the store immediately
	// on receipt — the alias deduplication (T-055) goes by the hashes, the slot
	// does not hold bodies until the task's end. A truncated body is not
	// stored at all (the truncated bytes' hash would lie at the dedup,
	// T-049); ErrStoreFull/ErrTooLarge — the content write pause (spec 8):
	// the page is marked unstored, the domain result is not dropped;
	// the other errors — storeErr, the episode will fail in ProcessDomain.
	if !truncated {
		pr, err := w.opt.Store.Put(ctx, cs.UTF8)
		switch {
		case err == nil:
			if pr.Hash != hash {
				check.storeErr = fmt.Errorf("хранилище вернуло хэш %s, воркер вычислил %s (контракт HashOf, T-041)",
					pr.Hash, hash)
				return check
			}
			check.ContentStored = true
			cache.addPut(hash, pr.Created)
		case errors.Is(err, content.ErrStoreFull), errors.Is(err, content.ErrTooLarge):
			// The content is not stored, the result record lives with
			// content_hash = nil (ContentSaver normalizes it, spec 8).
			w.opt.log().Warn("контент страницы не сохранён — запись будет без content_hash",
				"url", res.FinalURL.String(), "err", err.Error())
		default:
			check.storeErr = err
			return check
		}
	}

	// The final URL — into the task's cache: the following checks starting at
	// it (www after the bare's redirect) do not re-download the page.
	cache.put(workerCacheKey(res.FinalURL), &workerPage{
		final:      res.FinalURL,
		status:     check.Status,
		header:     check.Header,
		cookies:    check.Cookies,
		tls:        check.TLS,
		hash:       check.ContentHash,
		stored:     check.ContentStored,
		charset:    check.Charset,
		truncated:  check.Truncated,
		responseMS: check.ResponseMS,
		spa:        check.SPA,
		errClass:   check.ErrClass,
		errText:    check.ErrText,
	})
	return check
}

// workerPreallocMax — the cap of preallocating the body read buffer by
// Content-Length (T-147): a server's lie (a gigabyte Content-Length) does not
// reserve a gigabyte — such bodies are read by the ordinary buffer growth.
const workerPreallocMax = workerBodyHardLimit

// workerReadBody reads the response body by one buffer with a preallocation by
// Content-Length (T-147): io.ReadAll grows the buffer from 512 bytes by doubling
// — at bodies of tens–hundreds of KiB that is two-three generations of copies per page
// (× PerDomain concurrent checks × Concurrency slots — the GC's transit
// garbage that entered the slot memory's peak). Content-Length is honest only
// without Content-Encoding (the T-048/spec 5.5 transport serves an already unpacked
// stream, the header describes the compressed bytes): in that case the buffer
// is allocated exactly by the declared size and filled by io.ReadFull;
// the under-delivered and over-delivered by the server, the compressed answers and a broken/absent
// header — the ordinary read with growth. Without a BodyLimiter the read is bounded by
// the worker's guard cap (workerBodyHardLimit) — the preallocation
// does not substitute it.
func workerReadBody(res *httpfetch.FetchResult) ([]byte, error) {
	rc := io.Reader(res.Body)
	if res.BodyLimiter == nil {
		rc = io.LimitReader(res.Body, workerBodyHardLimit+1)
	}
	if res.Header.Get("Content-Encoding") == "" {
		if n, err := strconv.ParseInt(res.Header.Get("Content-Length"), 10, 64); err == nil && n > 0 && n <= workerPreallocMax {
			buf := make([]byte, n)
			r, err := io.ReadFull(rc, buf)
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				return nil, err
			}
			// The tail beyond the declared size (the server lied upward)
			// is read on by growth; at an honest answer it is empty.
			extra, err := io.ReadAll(rc)
			if err != nil {
				return nil, err
			}
			return append(buf[:r], extra...), nil
		}
	}
	return io.ReadAll(rc)
}

// --- the work loop ------------------------------------------------------------

// Run — the http-worker's work loop (spec 4.2, §5.5): reads the q:http tasks
// by a consumer group in Consume chunks and processes the deliveries in parallel
// up to Concurrency domains, each — by the Processing pattern (T-025): the processing
// (ProcessDomain + Options.OnResult) → the XACK. The consumption is sliding
// (T-139): the next read happens upon freeing the task slots
// (the read runs ahead of the processing by no more than a couple of Consume chunks), the loop does
// not wait for the whole chunk's completion — a window barrier on the durations' tail
// (tarpits up to total_timeout, 20–25 s) held the stage's throughput
// at ≈2 domains/s with an idling transport (the T-092 measurement).
// It runs until the ctx cancellation and returns nil — the framework's OnRun contract
// (T-012: the started tasks finish or return to the queue by a repeated
// delivery). A read or processing error does not stop the loop:
// it is logged (Warn), the unacknowledged delivery will return by XAUTOCLAIM
// (spec 4.2); Consume deliveries in a row without a single acknowledgment —
// a systematic processing failure — endures a fuse pause
// (workerFailWaitBase): drawing new tasks while the processing lies broken
// is pointless, the delivery attempts burn. An infrastructural publication
// failure (InfraError: the Redis OOM/connection, T-152) stops
// the reading immediately and until the write recovers (Reliab.Writable): the publications
// fail anyway, and processings without a chance to write the result only
// pile up pending. The reader's modes (reading | the browser pause | the infra pause
// | the fuse pause | the rate pause; the browser and the rate in q:http
// are structurally unreachable) are formalized by the readerstate.go automaton
// (T-262, TZ-fsm spec §3 FR-8): the pass's mode is chosen by one function over
// the observations, the pause priority order — by the transition table there.
func Run(ctx context.Context, q queue.Queue, w *Worker, opts Options) error {
	if q == nil {
		return errors.New("httpworker: цикл воркера: очередь не задана")
	}
	if w == nil {
		return errors.New("httpworker: цикл воркера: воркер не задан")
	}
	o := opts.withDefaults()
	log := o.log()
	log.Info("цикл http-worker",
		"group", o.Group, "consumer", o.Consumer,
		"concurrency", o.Concurrency, "consume", o.Consume, "per_domain", o.PerDomain,
		"max_redirects", o.MaxRedirects, "on_result", o.OnResult != nil)

	wait := func(d time.Duration) {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
		case <-t.C:
		}
	}

	// The stuck deliveries' return (T-025): a background RecoverLoop on behalf of
	// the same consumer — an XAUTOCLAIM claim and the same task processing.
	// It lives until the ctx cancellation; Run waits for its exit (a started task
	// finishes or stays unacknowledged, spec 3.4).
	if o.Recover != nil {
		recDone := make(chan struct{})
		go func() {
			defer close(recDone)
			_ = queue.RecoverLoop(ctx, o.Recover, func(c context.Context, del queue.Delivery) error {
				return w.processDelivery(c, del, o)
			}, queue.RecoverOptions{
				Stream:   queue.StreamHTTP,
				Group:    o.Group,
				Consumer: o.Consumer,
				// The PEL traversal rate — the queue's, not the stage's (redis.recover_*,
				// T-153); zeros — the queue's defaults.
				Batch:    o.RecoverBatch,
				Interval: o.RecoverInterval,
				Log:      log,
			})
		}()
		defer func() { <-recDone }()
	}

	sem := make(chan struct{}, o.Concurrency)
	// The sliding consumption (T-139): the reader keeps ahead of the processing
	// no more than a couple of Consume delivery chunks (the current chunk in hand +
	// the pending buffer), the dispatcher below takes a semaphore slot for the time
	// of one delivery and immediately takes the next — a task's completion
	// frees the slot and moves the reading, nobody waits for the chunk entirely.
	// The read but not started deliveries (the buffer full, the slots taken
	// by the durations' tail) at the ctx cancellation stay without an XACK and
	// return to the queue by an XAUTOCLAIM claim — like the window's
	// not started deliveries before (at-least-once, spec 3.4).
	pending := make(chan queue.Delivery, o.Consume)
	var failed atomic.Int64 // deliveries in a row without an acknowledgment
	// infraDown — a sustained infrastructural publication error
	// (the Redis OOM/connection, T-152): the q:http reading is suspended
	// until the write recovers (Reliab.Writable) — Redis
	// rejects the results' XADD anyway, new deliveries would only pile up
	// in pending without a chance to publish. The reader modes automaton's
	// infra observation (readerstate.go, T-262): the flag is set by the dispatcher
	// below (queue.IsInfra), extinguished by the write probe or a processing success.
	var infraDown atomic.Bool
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		failStreak := 0  // endured fuse pauses in a row
		infraStreak := 0 // endured infra pauses in a row
		for {
			// The pass's observations (FR-8, readerstate.go): the sensors
			// are polled only mounted (Writable — the write
			// probe at the lit T-152 flag, one SET), the pass's mode
			// is chosen by readerDecide — the pause priority order lives
			// in the transition table, not in the if order. There is no browser
			// (Options.Alive — the renderworker probe) or rate cap
			// (Options.Rate, T-240) in q:http: the browser pause
			// and the rate pause modes enter the enum for the automaton's
			// parity (FR-8) and are structurally unreachable — browser/rate are zero values.
			obs := readerObs{
				infra:  infraDown.Load(),
				failed: failed.Load(),
				batch:  o.Consume,
			}
			if obs.infra {
				// The write probe — only at the lit flag (T-152):
				// after the write recovers the reading resumes
				// immediately. There is no Recover sensor — the flag is
				// extinguished by the resumption below (a processing success will extinguish it anyway).
				obs.writable = o.Recover == nil || o.Recover.Writable(ctx)
			}
			switch readerDecide(obs) {
			case modePauseInfra:
				// The consumption pause at a sustained InfraError of publishing
				// (T-152): while the write probe fails — the reading does not run
				// (the fuse below would count the same failures longer and
				// would read chunks between the pauses).
				infraStreak++
				pause := workerFailBackoff(infraStreak)
				log.Warn("пауза чтения q:http: публикация результатов падает об инфраструктуру Redis (OOM/соединение) — новые доставки не читаются (T-152)",
					"streak", infraStreak, "wait", pause.String())
				wait(pause)
				if ctx.Err() != nil {
					return // graceful shutdown: wait exited by the cancellation
				}
				continue
			case modePauseFail:
				// The fuse (workerFailWaitBase): deliveries left in a row
				// without an acknowledgment in the number of a whole read
				// chunk (Consume) without a single success — a systematic
				// processing failure (Options.OnResult/the DB); a pause with
				// a doubling up to DefaultErrorWait. After the pause the pass
				// reads (FR-8): the fuse throttles the rate, but does not
				// block the reading until a zero counter. An acknowledged
				// delivery resets the failure counter immediately, the pause series
				// is reset at the reader's next pass that sees the
				// counter zero.
				if ctx.Err() != nil {
					return // graceful shutdown: there was and will be no pause
				}
				failStreak++
				pause := workerFailBackoff(failStreak)
				// The standstill's observability (the T-139 review): a pause up to
				// DefaultErrorWait stops the whole stage's reading —
				// a Warn with the series and the failure count (the frequency is bounded
				// by the pause doubling, no more often than ~2/min on series).
				log.Warn("пауза предохранителя чтения q:http: доставки падают подряд",
					"failed", obs.failed, "streak", failStreak, "wait", pause.String())
				wait(pause)
				if ctx.Err() != nil {
					return // graceful shutdown: wait exited by the cancellation
				}
			}
			// The resumption after the infra pause (T-152): the flag was
			// lit in the pass's observations, and the mode is already not the infra pause:
			// the write probe passed (or there is no sensor — the processing
			// success extinguishes the flag). The flag is extinguished, the infra pause
			// series starts anew (a flag extinguished by a success does not
			// reset the series — as before T-262).
			if obs.infra && infraDown.Swap(false) {
				infraStreak = 0
				if o.Recover != nil {
					log.Info("запись в Redis восстановлена — чтение q:http возобновляется")
				}
			}
			// The fuse pause series is reset by the observation
			// of a zero failure counter (an acknowledged delivery).
			if failed.Load() == 0 {
				failStreak = 0
			}
			deliveries, err := q.Consume(ctx, queue.StreamHTTP, o.Group, o.Consumer, o.Consume)
			if err != nil {
				if ctx.Err() != nil {
					return // graceful shutdown
				}
				log.Warn("цикл http-worker: чтение q:http", "err", err)
				wait(DefaultErrorWait)
				continue
			}
			for _, d := range deliveries {
				select {
				case pending <- d:
				case <-ctx.Done():
					return // the buffer's deliveries without an XACK will be returned by XAUTOCLAIM
				}
			}
		}
	}()

	// The dispatcher: waits for a free slot (or a cancellation) and launches the delivery.
	// The semaphore slot — with an exit by cancellation: a ctx cancellation during a slot
	// wait must not launch a delivery with an already cancelled ctx (the select's
	// channel choice is non-deterministic, therefore the cancellation is checked
	// after receiving the slot too).
	var wg sync.WaitGroup
dispatch:
	for {
		var d queue.Delivery
		select {
		case <-ctx.Done():
			break dispatch // the buffer's deliveries are not started — without an XACK
		case d = <-pending:
		}
		select {
		case <-ctx.Done():
			break dispatch
		case sem <- struct{}{}:
		}
		if ctx.Err() != nil {
			<-sem
			break dispatch
		}
		wg.Add(1)
		go func(d queue.Delivery) {
			defer wg.Done()
			defer func() { <-sem }()
			// A delivery panic must not drop the process without a log and
			// a cleanup — the recover parity with renderworker (T-262,
			// TZ-fsm spec §3 FR-8): a panic → an Error log, a delivery without an XACK
			// will return with a repeated delivery (XAUTOCLAIM, T-025);
			// the in-flight neighbors and the graceful shutdown are preserved.
			// The coverage — the delivery goroutine; the RecoverLoop claim path —
			// as in the renderworker model, without a recover (the coverage
			// boundaries — readerstate.go). For the fuse a panic is
			// the same unacknowledged delivery: a panic series would burn
			// the attempts without a pause.
			defer func() {
				if r := recover(); r != nil {
					failed.Add(1)
					log.Error("паника обработки доставки",
						"entry", d.ID, "panic", r, "stack", string(debug.Stack()))
				}
			}()
			err := queue.Processing(ctx, q, o.Group, d, func(c context.Context, del queue.Delivery) error {
				return w.processDelivery(c, del, o)
			})
			if err != nil {
				failed.Add(1)
				if queue.IsInfra(err) {
					// An infrastructural publication failure (T-152):
					// the reader's infraDown branch stops the reading.
					infraDown.Store(true)
				}
				if ctx.Err() == nil {
					log.Warn("задача q:http не подтверждена — вернётся повторной доставкой",
						"entry", d.ID, "err", err)
				}
			} else {
				failed.Store(0)
				// A success means the result publication took place — the
				// Redis records are available, the pause is lifted early.
				infraDown.Store(false)
			}
		}(d)
	}
	wg.Wait()    // the started deliveries finish (graceful stop, spec 3.4)
	<-readerDone // the reader finishes by the same ctx cancellation (Consume/wait)
	return nil
}

// processDelivery processes one q:http delivery. The acknowledgment (XACK)
// is done by Processing after a nil return — as spec 4.2 requires ("the XACK
// only after writing the result into the DB"): the result write and the
// q:analyze publication go in Options.OnResult (ContentSaver T-056 → Recorder T-058,
// the cmd/http-worker build) before the return, and the "write → publication →
// XACK" order is preserved without editing the loop. A successfully finished task goes
// into the stage's instruments (T-077): the throughput, the duration,
// the final status, the check errors, the response time, the rendered=false records.
func (w *Worker) processDelivery(ctx context.Context, del queue.Delivery, o Options) error {
	task, err := queue.Decode[queue.HTTPTask](del)
	if err != nil {
		// A poison record: a repeated delivery is pointless, the queue
		// will exhaust the attempts and carry it into q:dead (T-025).
		return fmt.Errorf("httpworker: запись %s стрима %s: %w", del.ID, del.Stream, err)
	}
	// The slot is taken until the end of the processing (T-163): both entry points (the Run
	// dispatcher and the RecoverLoop claims) go through here — the occupancy
	// gauge matches the set of tasks holding the unit's memory.
	w.met.SlotEnter()
	defer w.met.SlotLeave()
	start := time.Now()
	res, err := w.ProcessDomain(ctx, task)
	if err != nil {
		return fmt.Errorf("httpworker: домен %s: %w", task.Name, err)
	}
	if o.OnResult != nil {
		if err := o.OnResult(ctx, res); err != nil {
			return fmt.Errorf("httpworker: домен %s: обработка результата: %w", task.Name, err)
		}
	}
	w.met.Episode(res, time.Since(start))
	o.log().Debug("HTTP-задача обработана",
		"domain", task.Name, "subdomains", len(res.Subdomains), "records", res.RecordCount())
	return nil
}

// --- the redirect hosts' resolution "through the same resolver" (spec 5.5 item 2) --------

// DNSLookup — the DNS stage's adapter (dnsclient.Resolver with the T-035 limits) into
// httpfetch.Resolver: the HTTP redirects' and meta refresh hosts are resolved by the
// same resolver as the DNS stage ("through the same resolver with a cache", spec 5.5
// item 2), not by the system /etc/resolv.conf. The cache — the
// httpfetch.NewCachingResolver wrapper in the service build (cmd/http-worker).
type DNSLookup struct {
	resolve dnsclient.Resolver
	ipv6    bool
}

// NewDNSLookup builds the adapter; ipv6 — dns.ipv6 (spec 5.4: AAAA is disabled
// by the config without IPv6 — the redirect-host resolution looks only for A).
func NewDNSLookup(resolve dnsclient.Resolver, ipv6 bool) *DNSLookup {
	return &DNSLookup{resolve: resolve, ipv6: ipv6}
}

// LookupNetIP resolves the host for connecting (httpfetch.Resolver): A, and
// AAAA at ipv6, the type polling is CONCURRENT (T-101: the redirect host's
// resolution sits in the T-048 chain connection's critical path —
// the max(A, AAAA) latency, not the sum); IP literals (a Location like
// http://192.0.2.1/) pass without an exchange. CNAME is unrolled by
// the recursors themselves (unbound, spec 5.4) — the answer to A also contains the target's
// records (RFC 1034 §4.3.2, T-037). A negation (NXDOMAIN) and an empty answer —
// an error: there is nothing to connect to. One type's failure or refusal does not drop
// the resolution if the other gave addresses: a transient AAAA refusal must not
// break the connection over a working A (NODATA is not here: NOERROR without
// records simply adds nothing); nothing collected at all —
// the first type-order exchange error is returned (A before AAAA —
// the choice's determinism, as at the former sequential polling).
// A ctx cancellation — common to both exchanges: Exchange gets the same ctx,
// a cancelled resolution is returned by its error.
func (d *DNSLookup) LookupNetIP(ctx context.Context, host string) ([]netip.Addr, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{ip}, nil
	}
	types := []uint16{dns.TypeA}
	if d.ipv6 {
		types = append(types, dns.TypeAAAA)
	}
	// The exchanges run in parallel with a common ctx, but are collected by the types
	// order — the outcome is stable: the A addresses, then AAAA, regardless of which
	// type answered earlier (the T-047 transport sees the same order
	// as at the sequential polling). The goroutines write into different
	// answers cells — no race; a WaitGroup, not an errgroup: the types'
	// errors do not cancel the neighbor (the HW-07 semantics), an error is not a signal.
	answers := make([]workerDNSAnswer, len(types))
	if len(types) == 1 {
		// dns.ipv6=false — one exchange, without goroutines: nothing to parallelize.
		answers[0] = d.lookupType(ctx, host, types[0])
	} else {
		var wg sync.WaitGroup
		for i, t := range types {
			wg.Add(1)
			go func(i int, t uint16) {
				defer wg.Done()
				answers[i] = d.lookupType(ctx, host, t)
			}(i, t)
		}
		wg.Wait()
	}
	var (
		out      []netip.Addr
		firstErr error // the first type-order exchange error — the outcome, if there are no addresses
	)
	for i := range answers {
		if answers[i].err != nil {
			if firstErr == nil {
				firstErr = answers[i].err
			}
			continue // the other type may still give addresses
		}
		out = append(out, answers[i].addrs...)
	}
	if len(out) == 0 {
		if firstErr != nil {
			return nil, firstErr
		}
		return nil, fmt.Errorf("httpworker: резолв %q: пустой ответ", host)
	}
	return out, nil
}

// workerDNSAnswer — one A/AAAA exchange's outcome: this type's addresses or
// an error (an exchange failure, an RCode refusal); empty addresses without an error — NOERROR
// without records (NODATA), they add nothing to the resolution.
type workerDNSAnswer struct {
	addrs []netip.Addr
	err   error
}

// lookupType — one exchange by the record type (a common ctx with the neighboring type).
func (d *DNSLookup) lookupType(ctx context.Context, host string, t uint16) workerDNSAnswer {
	res, err := d.resolve.Exchange(ctx, dnsclient.Question{Name: host, Type: t})
	switch {
	case err != nil:
		return workerDNSAnswer{err: fmt.Errorf("httpworker: резолв %q %s: %w", host, dns.TypeToString[t], err)}
	case res.RCode != dnsclient.RCodeNOERROR:
		return workerDNSAnswer{err: fmt.Errorf("httpworker: резолв %q %s: %s", host, dns.TypeToString[t], res.RCode.String())}
	}
	var addrs []netip.Addr
	for _, rr := range res.RecordsOf(t) {
		switch r := rr.(type) {
		case *dns.A:
			if addr, ok := netip.AddrFromSlice(r.A.To4()); ok {
				addrs = append(addrs, addr)
			}
		case *dns.AAAA:
			if addr, ok := netip.AddrFromSlice(r.AAAA.To16()); ok {
				addrs = append(addrs, addr.Unmap())
			}
		}
	}
	return workerDNSAnswer{addrs: addrs}
}

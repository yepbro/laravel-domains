// Package config — the unified configuration of the Domain Crawler services
// (spec 5.10 of the main spec).
//
// Value sources by priority: environment variables > YAML file > defaults.
// The file path is set by the CRAWLER_CONFIG environment variable (the
// deprecated analog is APP_CONFIG); if it is not set, config.yaml in the
// current directory is used when such a file exists; otherwise the
// configuration is assembled from defaults and the environment.
//
// Variable naming scheme: CRAWLER_<SECTION>_<FIELD>
// (CRAWLER_HTTP_TOTAL_TIMEOUT, CRAWLER_SCHEDULER_LEASE, ...). The full list
// of names, types and constraints — docs/configuration.md. An empty
// variable value counts as unset.
package config

import (
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"crawler/internal/idn"
)

// maxSubdomains — the limit on the number of subdomains: the bit mask
// dns_mask is stored in int8, a bit = subdomains.id (spec 5.10, §5.2 item 9).
const maxSubdomains = 63

// maxUserAgentLen — a reasonable length of http.user_agent (spec 5.5, §9):
// a product name with contacts fits into tens of bytes; 512 is a generous
// cap that cuts off junk instead of contacts.
const maxUserAgentLen = 512

// The bounds of the normative range of the scheduler batches (spec 5.3:
// "in batches (1000–10 000 by default)"). The unexported constants are the
// single source for validation and the pin test of the stream budget
// invariant (TestSchedulerBatchFitsStreamBudget): the q:dns cap is
// proportional to the batch, and widening the range must re-check the
// budget.
const (
	minSchedulerBatchSize = 1000
	maxSchedulerBatchSize = 10000
)

// DefaultUserAgent — the honest default User-Agent of the HTTP stage
// (spec 5.5, §9). The contacts are placeholders on a reserved domain and
// address (RFC 2606, the .example zone): they cannot represent the real
// crawler owners, so the operator must substitute their own contacts via
// the http.user_agent config — the default only keeps the service from
// being anonymous. The form is the conventional "name/version (+URL; e-mail)".
const DefaultUserAgent = "DomainCrawler/1.0 (+https://example.example/contact; admin@example.example)"

// Config — the root of the configuration; the sections follow spec 5.10.
type Config struct {
	Postgres   Postgres  `yaml:"postgres"`
	Redis      Redis     `yaml:"redis"`
	Ingest     Ingest    `yaml:"ingest"`
	Scheduler  Scheduler `yaml:"scheduler"`
	DNS        DNS       `yaml:"dns"`
	HTTP       HTTP      `yaml:"http"`
	Render     Render    `yaml:"render"`
	Content    Content   `yaml:"content"`
	Analyzer   Analyzer  `yaml:"analyzer"`
	Subdomains []string  `yaml:"subdomains"`
	Monitor    Monitor   `yaml:"monitor"`
	Web        Web       `yaml:"web"`
	Dashboard  Dashboard `yaml:"dashboard"`
	Log        Log       `yaml:"log"`
}

// Postgres — the DB connection (spec 6.3).
type Postgres struct {
	DSN      string `yaml:"dsn"`
	PoolSize int    `yaml:"pool_size"`
}

// Redis — the address and reliability parameters of the task queues (spec 4.2).
type Redis struct {
	Addr string `yaml:"addr"`
	// MaxLen — an explicit length limit of all streams on publication
	// (XADD MAXLEN ~); 0 — the per-stream limits from the queue package
	// (queue.DefaultStreamMaxLen, T-025).
	MaxLen int `yaml:"maxlen"`
	// MaxAttempts — the maximum number of task deliveries before moving to
	// q:dead (spec 4.2: configurable, 3 by default; the counter — T-025).
	MaxAttempts int `yaml:"max_attempts"`
	// ClaimMinIdle — the minimum idle time of a stuck (pending without
	// XACK) record before it is returned to work by an XAUTOCLAIM re-claim
	// (T-025).
	ClaimMinIdle Duration `yaml:"claim_min_idle"`
	// RecoverBatch — the chunk of XAUTOCLAIM re-claim per one RecoverLoop
	// pass (T-153): the rate of returning stuck records to work is
	// batch/interval per pass; it sets the self-recovery time after
	// incidents (≈ the PEL size / (batch/interval)). Together with the
	// batch, the invariant "the time of a sequential batch traversal (the
	// worst stage record × batch) < claim_min_idle" is maintained;
	// otherwise the batch tail is re-claimed by neighboring workers and
	// burns attempts — the calculation is in docs/configuration.md
	// (the redis section).
	RecoverBatch int `yaml:"recover_batch"`
	// RecoverInterval — the period of the RecoverLoop polling of stuck
	// records with an incomplete batch (T-153); a full batch is drained
	// without a pause.
	RecoverInterval Duration `yaml:"recover_interval"`
	// TrimInterval — the period of releasing the confirmed prefix of the
	// streams (T-141, spec 4.2 "Addendum 2026-10-23"): XACK does not
	// delete records from the stream, and the confirmed history would
	// accumulate to the MAXLEN cap, overflowing the Redis budget
	// (spec 3.2 — the T-093 incident).
	// The scheduler loop (queue.TrimLoop) does XTRIM MINID once per period
	// for the oldest record that must remain (the oldest pending of the
	// group; with an empty pending — last-delivered-id): only delivered
	// and confirmed is removed. The cost of a pass is three Redis commands
	// per stream.
	TrimInterval Duration `yaml:"trim_interval"`
	// ConsumerIdle — the consumer inactivity threshold of a group for
	// cleanup (T-155): after worker restarts their consumers (names with
	// pid) stay in the groups forever; the scheduler loop
	// (queue.CleanupConsumersLoop, the period is the same TrimInterval)
	// deletes with XGROUP DELCONSUMER strictly empty (pending = 0) and
	// inactive-longer-than-the-threshold consumers. The threshold protects
	// the consumers of live workers under load: deleting an empty inactive
	// one is harmless (the name is recreated by the first read with
	// records). The range is 1m..720h (30 days).
	ConsumerIdle Duration `yaml:"consumer_idle"`
}

// Ingest — the ingest of domain files (spec 5.1).
type Ingest struct {
	Dir           string `yaml:"dir"`
	BatchSize     int    `yaml:"batch_size"`
	StableSeconds int    `yaml:"stable_seconds"`
	// ScanInterval — the interval of the periodic full pass over incoming
	// as the safety net for fsnotify (T-013; an extension to the minimal
	// set of spec 5.10, hence a duration, not seconds as with
	// stable_seconds).
	ScanInterval Duration `yaml:"scan_interval"`
	// MaxErrorRatio — the threshold of the share of rejected rows of a
	// file (T-016, spec 5.1: "a file is not considered erroneous because
	// of single bad rows"): above it the file as a whole is considered
	// erroneous (failed/, T-021). The share is 0..1; an extension to the
	// minimal set of spec 5.10.
	MaxErrorRatio float64 `yaml:"max_error_ratio"`
	// ArchiveMaxUncompressed — the cap of the total uncompressed volume of
	// the members of an input archive (T-133, the zip bomb protection):
	// for zip it is checked before reading via the UncompressedSize64
	// header, for tar/gz — by a counter over the stream. An extension to
	// the minimal set of spec 5.10.
	ArchiveMaxUncompressed ByteSize `yaml:"archive_max_uncompressed"`
	// ArchiveMaxFiles — the cap of the number of entries of an input
	// archive (T-133): exceeding it puts the archive entirely into
	// failed/; for zip it is checked via the central directory (it is read
	// into memory — the entry count is the spend limit), for tar — over
	// the stream. An extension to the minimal set of spec 5.10.
	ArchiveMaxFiles int `yaml:"archive_max_files"`
	// AllowedZonePaths — the allowlist of the multi-level zone paths of
	// the ingest directories (the tld-only-zones spec §5.1, §6.1): the
	// incoming/ working directories are TLD only (<tld>/add|delete/), a
	// zone with a dot in the directory name is erroneous (the file goes to
	// failed/), except for the ones listed here (testbed test zones like
	// crawl.test, which the TLD semantics forbid from being a "path
	// zone"). The entries are normalized zone names (punycode, lower case,
	// at least one dot); normalization and dedup happen at load, the
	// shape is checked by Validate. The production value is empty.
	AllowedZonePaths []string `yaml:"allowed_zone_paths"`
}

// Scheduler — the check scheduler (spec 5.3).
type Scheduler struct {
	RecheckInterval    Duration `yaml:"recheck_interval"`
	BatchSize          int      `yaml:"batch_size"`
	QueueHighWatermark int      `yaml:"queue_high_watermark"`
	Lease              Duration `yaml:"lease"`
	// TempErrorRetryAfter — the temp_error retry window (spec 5.3, the
	// default 24 h). With a value < 24 h the maturity-bucket guard (T-202)
	// ripens the cohorts toward midnight — intraday candidates wait for
	// the drain until midnight (a degradation of ≤ a day): the selection
	// window goes by the date column last_checked_at; more —
	// docs/schema.md §crawler_counters.
	TempErrorRetryAfter Duration `yaml:"temp_error_retry_after"`
	// BacklogInterval — the period of the backlog depth collector loop
	// (scheduler.BacklogLoop, T-077): the gauge crawler_backlog_domains
	// from GROUP BY check_state over all live domains. The default is
	// 5 minutes (T-161): the original 30 s of the spec 5.9 corridor
	// (30–60 s) was designed for a small corpus — at 47 M rows (the
	// measurement §12 of docs/load-measurements.md) a pass of 17+ s every
	// 30 s together with the zone recount and the status aggregates is a
	// large part of the postgres CPU; the depth gauge of the dashboard
	// tolerates five-minute freshness. The validation corridor is
	// 30s..1h: below 30 s — the aggregate more often than spec 5.9
	// allows, above an hour — the depth stops being an observable
	// quantity. An extension to the minimal set of spec 5.10: a new field
	// inside the scheduler section.
	BacklogInterval Duration `yaml:"backlog_interval"`
	// TempErrorMaxRetries — the maximum of consecutive temp_error
	// rechecks (spec 5.3: "no more than N times in a row"); after the
	// exhaustion the domain gets a final status per the result of the last
	// check (T-031). The spec does not set the number; 3 — like the
	// delivery attempt counter of the queue (T-025).
	TempErrorMaxRetries int `yaml:"temp_error_max_retries"`
	// ZoneWeights — the zone weights of the batch selection (spec 5.3:
	// "zone weights — optional in the config", T-106): a zone name
	// (punycode, as in the zones reference table) → a weight 1..1000; a
	// zone with weight w takes a share of the batch proportional to w,
	// weight 1 for all — an equal round-robin order.
	// A nil or empty map — no weights, the default behavior.
	// cmd/scheduler resolves the names into zone_id by the reference
	// table at start (scheduler.ResolveZoneWeights): a name may be missing
	// from the reference table (ingest creates the zone with the first
	// file, spec 5.1) — a Warn, the zone is checked with weight 1;
	// inactive zones are resolved too — the weight takes effect when the
	// zone is activated (crawlerctl zones enable) without a restart. An
	// extension to the minimal set of spec 5.10: a new field inside the
	// scheduler section, no top-level section is introduced.
	ZoneWeights map[string]int `yaml:"zone_weights"`
}

// DNS — the parameters of the DNS stage (spec 5.4). The client is
// internal/dnsclient (T-034): miekg/dns, a pool of UDP sockets, a
// TCP-fallback at TC=1, retries.
type DNS struct {
	Resolvers         []string `yaml:"resolvers"`
	FallbackResolvers []string `yaml:"fallback_resolvers"`
	Concurrency       int      `yaml:"concurrency"`
	MaxQPS            int      `yaml:"max_qps"`
	IPv6              bool     `yaml:"ipv6"`
	// Timeout — the timeout of a single UDP exchange attempt. An extension
	// to the minimal set of spec 5.10 (the transport timeouts are not
	// listed there, but spec 5.4 requires retries on timeouts).
	Timeout Duration `yaml:"timeout"`
	// TCPTimeout — the TCP-fallback timeout on response truncation (TC=1):
	// the connection setup + the exchange, separate from UDP.
	TCPTimeout Duration `yaml:"tcp_timeout"`
	// Retries — the retries of a query beyond the first attempt on a
	// network timeout/error; "SERVFAIL/timeout after the retries" →
	// temp_error (spec 5.4 item 1). The minimum is 1: the spec requires
	// retries, "no retries" is not a working mode.
	Retries int `yaml:"retries"`
	// RetryDelay — the pause between the attempts.
	RetryDelay Duration `yaml:"retry_delay"`
	// UDPPoolSize — the size of the client's UDP socket pool. By default
	// it equals concurrency: a pool smaller than the planned parallelism
	// itself becomes a limiter below the configured one.
	UDPPoolSize int `yaml:"udp_pool_size"`
}

// HTTP — the parameters of the HTTP stage (spec 5.5, the limits table).
type HTTP struct {
	Concurrency int `yaml:"concurrency"`
	PerIPLimit  int `yaml:"per_ip_limit"`
	// PerIPOverrides — raised (or lowered) per-IP limits for the parker
	// prefixes (T-165, part 2b): a "CIDR → limit" map, the key is an
	// IPv4/IPv6 prefix ("195.208.0.0/16"), the value is the limit of
	// connections per one IP of this prefix instead of per_ip_limit.
	// The matching of the dialed IP is by the longest matching prefix
	// (longest-prefix, deterministically: one network cannot be listed
	// twice — validation rejects duplicates after masking; the IPv4 and
	// IPv6 prefixes do not overlap, netip compares them by family). An
	// empty map (the default) — the previous behavior: one per_ip_limit
	// for all IPs. The limit range is like per_ip_limit (1..64); the
	// actual parallelism is still capped by the global http.concurrency
	// ceiling. Purpose: in the com/xyz mixes millions of domains live on
	// parker IPs — per_ip_limit 4 with a FIFO semaphore queue stretches
	// the tasks toward 20 s (head-of-line, the finding T-092 §3); a raised
	// limit for the parker prefixes identified by the measurement unloads
	// the queue without raising the bar for the other hosts.
	// Observability for deciding "which prefixes and by how much" — the
	// histogram crawler_http_per_ip_wait_seconds (:9104, T-165 part 1).
	// An extension to the minimal set of spec 5.10.
	PerIPOverrides map[string]int `yaml:"per_ip_overrides"`
	ConnectTimeout Duration       `yaml:"connect_timeout"`
	// TLSHandshakeTimeout — the TLS handshake timeout (5 s from the
	// limits table of spec 5.5). The minimal set of spec 5.10 has no such
	// field — like the other timeouts of the table, all of them are
	// "configurable" per spec 5.5.
	TLSHandshakeTimeout Duration `yaml:"tls_handshake_timeout"`
	HeaderTimeout       Duration `yaml:"header_timeout"`
	TotalTimeout        Duration `yaml:"total_timeout"`
	MaxRedirects        int      `yaml:"max_redirects"`
	MaxBodyBytes        ByteSize `yaml:"max_body_bytes"`
	UserAgent           string   `yaml:"user_agent"`
	// MinBodySpeed — the minimum body reading speed after the first byte,
	// bytes/s (spec 5.5: "tarpit protection: a minimum speed of 2 KB/s").
	// An extension to the minimal set of spec 5.10 — the value from the
	// limits table of spec 5.5 (T-049).
	MinBodySpeed ByteSize `yaml:"min_body_speed"`
	// MaxRPS — the global request rate (spec 5.5: "limiting the outbound
	// load by the global number of connections and RPS"). The spec has no
	// number: the default is the order of the natural rate of the stage
	// (a page ~1–2 s at concurrency 2000). An extension to the minimal
	// set of spec 5.10 (T-049).
	MaxRPS int `yaml:"max_rps"`
	// BlockInternalAddrs — forbidding connections to internal addresses
	// (loopback/RFC1918/ULA/link-local/unspecified/multicast, including
	// the IPv4-in-IPv6 form) on all the paths of the SSRF circuit (T-219):
	// the DNS addresses of the starting host, the literals and resolves
	// of the hosts of the HTTP redirects, the IP-literal hosts of the
	// render tasks. SSRF protection: an untrusted site can steer the
	// crawler to 127.0.0.1, cloud metadata or a DB/Redis network address.
	// false by default (the testbed and dev benches resolve scenarios into
	// the internal 172.28.100.0/24 networks); for Internet production use
	// enabling it is mandatory. An extension to the minimal set of
	// spec 5.10.
	BlockInternalAddrs bool `yaml:"block_internal_addrs"`
	// TaskConcurrency — the concurrent q:http tasks (domains) in the
	// http-worker loop (T-139): the task parallelism of the stage, as
	// opposed to the transport Concurrency (connections/RPS — the
	// internal httpfetch, T-049). The consumption is sliding — the stage
	// ceiling ≈ tasks / the average task duration; in memory each task
	// holds the domain result (metadata; the bodies are not retained —
	// the early Put of T-147) + the buffers of the bodies being read; the
	// memory budget of the stage = TaskConcurrency × the peak memory of a
	// task. The default 192 (T-157; the former 256 — per the measurements
	// T-092, docs/load-measurements.md §3): a day on 256 slots gave two
	// unit fatals (the measurement §12: 05:48 OOM-kill, 09:17 the peak of
	// 900.5 MiB = MemoryMax; MemoryHigh=850M — 120,095 throttling events,
	// reclaim storms and swap slow the worker) at ≈3.34 MiB per slot (the
	// measurement of the code before T-147); 192 slots are stable (the
	// soak of wave 21: 860–893 MiB, oom=0, NRestarts=0), the rate ≈17–19
	// domains/s vs ≈22 at 256 — stability is more important than the
	// deferred goal of 50 domains/s. T-163 closed the rest of the profile
	// (the slot measurement metrics + the soft heap ceiling MemLimit
	// below) — the return to 256 per the sampler measurement after the
	// deploy; 256 × TaskPerDomain 8 = 2048 ≈ the transport Concurrency
	// 2000 — the task window fills the connection budget without
	// exceeding it. An extension to the minimal set of spec 5.10
	// (spec §13.2: default values are refined by measurements).
	TaskConcurrency int `yaml:"task_concurrency"`
	// TaskConsume — the q:http records per one read (T-139): the size of
	// the XREADGROUP batch and the prefetch buffer of the sliding
	// consumption — the read runs ahead of processing by no more than a
	// couple of batches (the current one in the reader's hand + the
	// buffer), the next read happens when task slots free up; the read
	// window does not limit the parallelism (16 ≈ 3 reads/s at the actual
	// 47.6 domains/s, §11). The same number is the threshold of the loop
	// fuse: that many consecutive deliveries without a single
	// confirmation suspends the reading with a doubled pause (small values
	// make the fuse more sensitive — change deliberately).
	TaskConsume int `yaml:"task_consume"`
	// TaskPerDomain — the parallel checks (a subdomain × a protocol)
	// inside a domain task (the ProcessDomain goroutine pool): the
	// subdomains of one domain almost always live on the same IPs — the
	// real parallelism of connecting to one IP is limited by the per-IP
	// transport semaphore (PerIPLimit, the spec 5.5 recommendation: 3–4);
	// 8 = a doubled ceiling covers domains with several addresses without
	// giving one domain an unfair share of the budget. An extension to the
	// minimal set of spec 5.10 (T-139).
	TaskPerDomain int `yaml:"task_per_domain"`
	// DeadHostCutoff — the dead-host cutoff (T-164): apex and www that
	// did not pass the connect phase (connection refused or a connect
	// timeout over both protocols) finish the q:http task without checking
	// the other subdomains — the dead hosts of mass zones (the com/xyz
	// mixes, the measurement §12 of docs/load-measurements.md: 60% of the
	// tasks in the 10–30 s bucket at a transport of 0.54 s/request) stop
	// burning slots with a timeout tail. The records of the skipped
	// subdomains — per §8 ("no answer": no records), the sign is the
	// dead_host_cutoff bit in domains.flags and the counters
	// crawler_http_dead_host_cutoff_*. The default true: the signature is
	// strict (both main hosts × both protocols × the connect phase only —
	// an RST refusal or SYN silence, not TLS/headers/the total timeout);
	// a transient false cutoff (both hosts "silent" once) gives temp_error
	// at connect timeouts and self-heals with the T-031 recheck; the share
	// of false ones is measured by a control sample over the flag bit
	// (the criterion < 1%, T-164). false — all the subdomains are checked
	// as before. An extension to the minimal set of spec 5.10.
	DeadHostCutoff bool `yaml:"dead_host_cutoff"`
	// MemLimit — the soft memory ceiling of the http-worker Go runtime
	// (debug.SetMemoryLimit, T-163): approaching the ceiling, GC runs the
	// cycle more often and returns the pages to the OS itself, without
	// driving anon to the MemoryHigh/MemoryMax of the unit — the kernel
	// reclaim storms (120,095 events and two fatals per day, the
	// measurement §12, T-157) cost more than extra GC cycles. The page
	// cache of the pack records is not part of the runtime accounting —
	// the ceiling must be set below the MemoryHigh of the unit with a
	// margin for it (the default 768 MiB vs MemoryHigh 850M / MemoryMax
	// 900M). A live heap above the ceiling does not kill the process: GC
	// works up to 50% CPU (a slowdown, not death).
	// 0 — disabled (the Go default: no ceiling). An extension to the
	// minimal set of spec 5.10.
	MemLimit ByteSize `yaml:"mem_limit"`
}

// Render engines (the render spec — tasks/TZ_render_engines.md, §4.1 item
// 1, FR-1): the values of the render.engine field. Strings, not a numeric
// iota: the value is visible in the config, the metrics (the engine label,
// FR-15) and the logs as is.
const (
	// EngineChromium — its own headless-shell process (the production
	// render image) or an external CDP of the dev stack (render.remote_url).
	EngineChromium = "chromium"
	// EngineMoli / EngineLightpanda — external CDP servers: the engine
	// container is brought up by the infrastructure, the worker only
	// connects (render.remote_url is required, FR-3).
	EngineMoli       = "moli"
	EngineLightpanda = "lightpanda"
)

// Render — the render parameters (spec 5.6). RemoteURL is a dev environment
// extension: the address of an external Chromium over CDP (ws://…); in
// production mode the worker starts the browser itself and the field stays
// empty.
type Render struct {
	// Engine — the render engine (the render spec §4.1 item 1, FR-1):
	// chromium (the default, reproduces today's behavior pixel-by-pixel —
	// an engine change only by an explicit switch), moli|lightpanda —
	// external CDP servers: render.remote_url is required for them and a
	// non-zero restart_memory_bytes is forbidden (FR-3, the binding is
	// checked by validation). The core of internal/render does not
	// distinguish the engines (FR-4: the differences are the allocator
	// wiring, the deploy, the documentation). "" — a config without
	// Default(): validation treats it as chromium (like "" at log.level).
	Engine            string   `yaml:"engine"`
	RemoteURL         string   `yaml:"remote_url"`
	Contexts          int      `yaml:"contexts"`
	Timeout           Duration `yaml:"timeout"`
	TimeoutAttempts   int      `yaml:"timeout_attempts"`
	RestartEveryPages int      `yaml:"restart_every_pages"`
	// RestartMemoryBytes — the memory restart threshold of the browser
	// (spec 5.6 "when the memory threshold is exceeded"): the total RSS of
	// the Chromium processes ≥ the threshold → a restart (T-065). An
	// extension to the minimal set of spec 5.10: the spec sets no number;
	// the guideline is the Chromium budget ≈ 2 GB (spec 3.2, the cgroup
	// MemoryMax), the threshold is taken below the container limit.
	// 0 — the control is disabled; in remote mode (dev) the RSS is
	// unavailable and the control does not work (the ceiling is held by
	// the container's mem_limit).
	RestartMemoryBytes ByteSize `yaml:"restart_memory_bytes"`
	// BlockStylesheet — whether to block CSS in the render context
	// (T-213): at true the list of the blocked render types (T-062:
	// Image/Media/Font — 91% of the page weight) is extended with
	// Stylesheet — the remaining 5–10% of the bytes plus a noticeable part
	// of the style/layout CPU; per the research (31.10) blocking CSS plus
	// images gives another −third of the load time. The default false is
	// a deliberate decision: on rare sites JS reads computed styles/media
	// queries and changes behavior on unapplied styles (the detection DOM
	// could differ), so enabling it is only after an A/B in production:
	// a diff of the Laravel signatures before/after on the same sample
	// (the criterion: the share of detection mismatches <1%) and a rate no
	// worse. An extension to the minimal set of spec 5.10.
	BlockStylesheet bool `yaml:"block_stylesheet"`
	// BlockTrackerDomains — whether to block, in the render context,
	// requests to web analytics and telemetry domains (T-214): Google
	// Analytics, GTM, DoubleClick, Yandex Metrika, Hotjar etc. — the full
	// list and the selection principle in blockTrackerDomains of
	// internal/render (specialized analytics only, not general-purpose
	// CDNs). Analytics arrives as Script/XHR requests — blocking by types
	// (T-062/T-213) does not catch it (SPA scripts must load), while it
	// cuts the traffic (5–15 requests, 100–500 KB per page per the
	// source measurements) and the CPU of the tracker timers prolonging
	// the network quiet (T-063); Fetch.enable gets urlPattern patterns by
	// domain, the verdict per each suspended request is an exact host
	// check: the site's own scripts and paths with a similar substring
	// (/js/google-analytics.com.js) are not affected. The default true —
	// unlike block_stylesheet (T-213): trackers do not participate in the
	// Laravel detection (the rules look for the framework signatures in
	// the content and the scripts of the site itself); the residual risk
	// is exotics collecting visible content with tag-manager tags; the
	// benefit is direct, so no A/B is required, the control after
	// enabling is the usual stage metrics (the render rate and success no
	// worse than current). false — the trackers load as before. An
	// extension to the minimal set of spec 5.10.
	BlockTrackerDomains bool `yaml:"block_tracker_domains"`
	// QueueHighWatermark — the capacity threshold of the q:render queue
	// (T-174; the publication gate T-066 — HasCapacity in RenderQueue):
	// the stream depth (the backlog of the consumer group, the clamp of
	// T-168) ≥ the threshold — new SPA tasks are not published, the
	// domains get the render_pending flag by the same transaction and are
	// finished by a later pass (the tasks are not lost). A separate
	// threshold, not the shared scheduler.queue_high_watermark: with the
	// clamp of T-168 (min(lag, XLEN)+pending) the shared threshold of
	// 100 k kept the gate open at a lag of 98.4 k out of 100 k — the
	// inflow equaled the consumption (the equilibrium of 24.10, stage 25
	// of tasks.md), the debt drain was zero. The default 10 k is ≈2 h of
	// consumption buffer (the measurement of 24.10: ≈5.1 k tasks/h): it
	// closes the gate at the current debt (the lag of 98.4 k + 344.5 k
	// render_pending flags) and returns the "draining — admitting"
	// hysteresis; decidedly below half of the MAXLEN limit of q:render
	// (75 k, T-025) — the threshold clamp is not active.
	// 0 — the shared scheduler.queue_high_watermark (the semantics of
	// content.retention_analyze_watermark); the effective value with the
	// clamp min(watermark, MAXLEN/2) — like the publication gate T-026 —
	// is computed by the cmd/http-worker wiring. An extension to the
	// minimal set of spec 5.10.
	QueueHighWatermark int `yaml:"queue_high_watermark"`
	// RequeueDeprioritizedZones — the names of the zones (punycode, as in
	// the zones reference table; the IDN zone .рф is xn--p1ai) whose
	// render debt (the render_pending flag) is served by the
	// RequeueRenderPending finish pass on a gets-the-leftovers basis
	// (T-184, decision (a) of T-176 — the operator on 24.10: we render
	// everything, we lose nothing; the zones with a high DOM contribution
	// first, the parking .рф last): the batch selection is two-stage —
	// first the domains of the zones NOT in the list, on exhaustion — the
	// zones of the list (the debt queue loses no records). An empty list —
	// the previous order (one stage without a zone filter, T-181). A name
	// may be missing from the reference table (ingest creates the zone
	// with the first file, spec 5.1) — the stage filter matches by name in
	// the selection itself and takes effect with the zone's appearance;
	// there is no separate start-time resolution. It only affects the
	// order of the debt drain: the publication of the SPA results of the
	// live check is not prioritized (its dosing is the q:render gates),
	// the undelivered tail of the stream is drained FIFO as is. An
	// extension to the minimal set of spec 5.10.
	RequeueDeprioritizedZones []string `yaml:"requeue_deprioritized_zones"`
	// LateSelectTimeout — the ceiling of one batch selection of the late
	// re-publisher of the render (T-274; the ceiling itself —
	// T-271/T-273): the server-side statement_timeout of the tx shell of
	// the selection and the client ctx deadline; a timeout smaller than
	// the cost of the first catch-up pass from a zero cursor turns the
	// slow drain into an eternal loop of doomed repeats (the T-271
	// regression, the incident of 09–10.11.2026). The default 300 s
	// (T-273): the re-publisher selection is adaptive (the unpinned form
	// of the plan) and fast, the catch-up from zero is finished in
	// batches. The knob is added for the case of an underestimation —
	// changing the ceiling without a deploy; 0 — the code default. An
	// extension to the minimal set of spec 5.10.
	LateSelectTimeout Duration `yaml:"late_select_timeout"`
	// Fallback — the render.fallback subsection: the Chromium fallback
	// queue (the render spec §4.1 item 1, FR-6…FR-14). The fields describe
	// the whole feature contract; the publication from the finalization
	// paths — T-238 (renderworker), the health gate and the share budget —
	// T-239, the worker B rate — T-240. The default is disabled and does
	// not affect anything: the field ranges are valid even with
	// enabled=false, so enabling by editing one flag (FR-14) does not blow
	// up the start.
	Fallback RenderFallback `yaml:"fallback"`
}

// RenderFallback — the render.fallback subsection (the render spec §4.1
// item 1): tasks that failed to render on the primary engine for an
// engine-dependent reason (a tarpit T-148, the task class T-172 on the last
// attempt) are finished by a separate chromium worker B from the
// q:render-fallback queue. Engine-independent reasons (DNS T-207, cert
// T-159) get no fallback — Chromium will not fix them (FR-7); worker B is
// the same binary in another configuration build (FR-12), a recursive
// fallback of the fallback is forbidden (FR-10).
type RenderFallback struct {
	// Enabled — the fallback switch (FR-14): false — the primary worker
	// finalizes without render as today (the render_failed_at marker,
	// T-210), the subsection affects nothing; true — the publications into
	// q:render-fallback are subject to the ceilings below. Disabling —
	// without deploying new entities: what accumulated in the queue is
	// finished by worker B to empty.
	Enabled bool `yaml:"enabled"`
	// WorkerRate — the rate ceiling of worker B (FR-13): the records per
	// the consumption window of the worker — the batch mode "collect and
	// finish" without scaling up the chromium capacity (the window is the
	// tick/window of the worker loop, T-240). The default 100 ≈ the full
	// capacity of one chromium (8 contexts × ~5 s/page ≈ 1.6 pages/s):
	// a starting value, calibrated by a bench run (the render spec §4.4
	// item 4 — the ceiling is configured per the bench).
	WorkerRate int `yaml:"worker_rate"`
	// SharePercent — the fallback share budget (FR-9b): the ceiling of the
	// share of publications into the fallback, % of the render flow per
	// window; above it the fallback suspends until the end of the next
	// window, the tasks finalize as today, an alert is raised (the window
	// is 1000 confirmed deliveries of the stage, T-239).
	// The default 30 — the rejection threshold of the render spec §5.3
	// (a fallback rate above ~30% zeroes the feature economics, §7 item
	// 2): the budget is a storm breaker protecting from overflowing the
	// whole flow onto the expensive engine, not a canary criterion.
	// 0 — no ceiling.
	SharePercent float64 `yaml:"share_percent"`
	// HealthGateStreak — the health gate threshold (FR-9a): that many
	// consecutive infra errors of the browser class (ClassBrowser — the
	// primary engine is unavailable; the streak counts the errors since
	// the last successful render — success resets it, the task/context
	// class errors do not touch it) suspend the fallback publication
	// until the engine recovers: overflowing the whole flow onto the
	// expensive chromium while the light one is down is excluded. The
	// default 3: a single infra error is the noise of a live flow (the
	// fallback is not damped), a lying engine gives a streak on every
	// delivery — the threshold is reached in seconds. The sample of the
	// streak logic — the renderFailBackoff of the worker (a reset by
	// success, growth by failure).
	HealthGateStreak int `yaml:"health_gate_streak"`
}

// Content — the content storage (spec 5.7).
type Content struct {
	Dir            string   `yaml:"dir"`
	PackSize       ByteSize `yaml:"pack_size"`
	MaxStoreBytes  ByteSize `yaml:"max_store_bytes"`
	ZstdDictionary string   `yaml:"zstd_dictionary"`
	// DiskFreePercent — the threshold of the reaction to disk overflow
	// (spec 8): when the free share of the content directory drops below
	// the threshold, content writing pauses (ErrStoreFull) and an alert is
	// raised. An extension to the minimal set of spec 5.10; the name — per
	// the meaning of the configurable threshold of spec 8 ("the threshold
	// is configurable"): the share of free space, %.
	DiskFreePercent float64 `yaml:"disk_free_percent"`
	// Retention — the life window of the content and the jsonb non-finds
	// (T-166; spec 5.7 "the content is stored no less than the time the
	// rules need to be reapplied", the operator decision of 2026-10-23:
	// the disk without new capacity): the subdomain_results records older
	// than the window, not Laravel finds and not waiting for render, lose
	// content_hash and have headers/cookies/tls/redirects/addrs/cname →
	// NULL; the record passport always stays. The steady-state content
	// volume = the daily inflow × the window (20 GiB/day: 6 h ≈ 5 GiB;
	// 48 h would eat the whole volume). 0 — retention disabled. An
	// extension to the minimal set of spec 5.10.
	Retention Duration `yaml:"retention"`
	// RetentionInterval — the period of the RetentionLoop cycle passes
	// (T-166); the pack hourly compaction (T-045) returns the space to
	// the content, retention "feeds" it — more often than an hour is
	// pointless.
	RetentionInterval Duration `yaml:"retention_interval"`
	// RetentionBatch — the rows in one retention chunk-transaction
	// (T-166): short transactions and HOT updates (fillfactor 85,
	// migration 000002); the working range 10–50 k per the tasking.
	RetentionBatch int `yaml:"retention_batch"`
	// RetentionAnalyzeWatermark — the retention interlock threshold: the
	// depth of q:analyze (the backlog of the consumer group) ≥ the
	// threshold — the analyzer does not keep up, the bodies and the
	// headers/cookies are still needed, the pass is skipped.
	// 0 — the shared threshold scheduler.queue_high_watermark (the
	// publication gate of q:analyze works by the same "strictly below the
	// threshold" criterion, T-026); the interlock is disabled by an
	// explicitly large value at one's own risk.
	RetentionAnalyzeWatermark int `yaml:"retention_analyze_watermark"`
	// RetentionSelectTimeout — the ceiling of one retention batch
	// selection (T-274; the ceiling itself — T-271/T-273): the server-side
	// statement_timeout of the plan pinning tx shell and the client ctx
	// deadline. The default 2 h — a doubled margin over the production
	// measurements of 10–11.11.2026 of the first catch-up pass from a
	// zero cursor (the predicate "there is something to clean" is outside
	// the index — a heap visit of every row of the clean prefix: ~60 min
	// idle, >60 min under the full front; 3600 s of T-273 sufficed only
	// for a calm system — three passes under the front hit the limit).
	// 0 — the code default; the knob for the case of an underestimation —
	// changing the ceiling without a deploy. An extension to the minimal
	// set of spec 5.10.
	RetentionSelectTimeout Duration `yaml:"retention_select_timeout"`
}

// Analyzer — the Laravel analyzer (spec 5.8).
type Analyzer struct {
	RulesDir        string  `yaml:"rules_dir"`
	LaravelMinScore float64 `yaml:"laravel_min_score"`
}

// Monitor — the monitoring service (spec 5.9).
type Monitor struct {
	Listen string `yaml:"listen"`
	// ZoneStatsInterval — the period of the incremental counter
	// reconciliation (ZoneStatsLoop, T-178): the combined pass is the only
	// full domains pass in steady state. The unit of the stall threshold
	// (AlertStallFactor × the interval): zones.updated_at is written by
	// this very loop, the threshold must count its misses. The alert loop
	// since T-206 runs on its own interval (AlertsInterval below).
	ZoneStatsInterval Duration `yaml:"zone_stats_interval"`
	// AlertsInterval — the period of the alert evaluation loop
	// (AlertsLoop, T-206): 5 minutes — the former frequency. T-161 kept
	// "one pair of loops" on zone_stats_interval, T-199 diluted the
	// reconciliation to 30 min — the detection windows set in passes
	// stretched 6× (queue_dead 15 → 90 min, memory_high/render_debt 10 →
	// 60 min, bad_hashes — a window of 75 min → 7.5 h); the decoupling
	// (the operator decision of 26.10) returns the former effective
	// windows and the PSI sample error of ~5 min. The stall threshold
	// stays in the reconciliation periods (zone_stats_interval):
	// updated_at is written by ZoneStatsLoop; a threshold of 5 × 5 min
	// with a 30-min write would fire forever.
	AlertsInterval Duration `yaml:"alerts_interval"`
	// LoadReportInterval — the period of the system load report to the
	// operator (T-173): a snapshot of CPU/memory/processes/disk/queues
	// goes to Telegram (monitor.alerts.telegram) and the log; 0 — the
	// loop is disabled.
	LoadReportInterval Duration `yaml:"load_report_interval"`
	// SlowQueryInterval — the period of polling pg_stat_activity for slow
	// queries (T-177): client operators running longer than
	// SlowQueryMinDuration go to Telegram (SlowQueryTelegram below) with
	// the same delivery as the alerts/load report; 0 — the loop is
	// disabled.
	SlowQueryInterval Duration `yaml:"slow_query_interval"`
	// SlowQueryMinDuration — the slow query threshold (T-177): 3 s — the
	// threshold of the production log_min_duration_statement
	// (deploy/postgres/postgresql.conf), one boundary of "pathology" in
	// the server log and in Telegram. 0 — the loop default.
	SlowQueryMinDuration Duration `yaml:"slow_query_min_duration"`
	// SlowQueryTelegram — delivering the slow queries (T-177) to Telegram
	// with the same delivery as the alerts and the load report. false —
	// the default: a slow query is diagnostics, not a state transition
	// requiring a "now" reaction (like the T-162 alerts); it is already
	// visible in three surfaces — the Warn log of monitor, the metrics
	// crawler_pg_slow_queries* and the full post-mortem trace in the
	// server log; it goes to the Telegram group only by an explicit
	// enabling.
	SlowQueryTelegram bool `yaml:"slow_query_telegram"`
	// RenderDebtInterval — the period of the render debt loop (T-175): the
	// gauge crawler_render_pending_domains + a summary line in the log;
	// the pass is a point Lookup of the counter and an XLEN of q:render
	// (milliseconds). The render_debt alert runs on the alert pass
	// (alerts_interval, T-206); this field does not set its frequency.
	RenderDebtInterval Duration `yaml:"render_debt_interval"`
	// IngestMetricsURL — the address of the /metrics of the ingest service
	// (T-099): the junk content_hash cleanup alert reads the counter from
	// the ingest exposure with a probe once per alert period. The same
	// address Prometheus scrapes; an empty string — the alert "is not
	// measured".
	IngestMetricsURL string `yaml:"ingest_metrics_url"`
	// RenderMetricsURL — the address of the /metrics of the render worker
	// (T-246): the fallback share budget exceedance alert reads the
	// suspension event counter (crawler_render_fallback_suspended_total)
	// from the probe exposure once per alert period. The same address
	// Prometheus scrapes; an empty string — the alert "is not measured"
	// (quiet).
	RenderMetricsURL string `yaml:"render_metrics_url"`
	// Alerts — the missing thresholds of spec 5.9 and the alert delivery
	// to the operator (T-162, the measurement §12: all the P0s of that
	// period were found manually).
	Alerts Alerts `yaml:"alerts"`
}

// Alerts — the monitor.alerts section (T-162): the thresholds of the new
// alerts and the Telegram delivery enabling. The thresholds of the first
// alerts (DNS, queues, progress, the disk percent) stay with their fields —
// the queue at scheduler.queue_high_watermark, the percent at
// content.disk_free_percent (a smaller surface, each threshold lives where
// its mechanism is).
type Alerts struct {
	// Telegram — enabling the delivery of the alert transitions
	// (triggering/clearing) to Telegram via deploy/telegram-notify.sh
	// (spec 10.4 — the same bot and group as the CI/CD notifications).
	// The secrets are not here but in the EnvironmentFile of the unit
	// (TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID,
	// deploy/systemd/crawler.env.example): missing any of them — the
	// delivery is disabled, the alerts stay as Warns in the monitor log.
	Telegram bool `yaml:"telegram"`
	// TelegramScript — the path of telegram-notify.sh on the host: the
	// script rides in the deploy archive together with the binaries
	// (deploy.yml → /opt/crawler/bin; no separate installation needed —
	// docs/operations.md §4.3).
	TelegramScript string `yaml:"telegram_script"`
	// DiskFreeMinBytes — the absolute minimum of the free space of the
	// content directory, a Warn (the disk_free_min alert): the percent
	// threshold content.disk_free_percent on a large volume is late —
	// 10% of 79 GB ≈ 5 h until full at a rate of ≈36 GiB/day (the
	// measurement §12).
	DiskFreeMinBytes ByteSize `yaml:"disk_free_min_bytes"`
	// DiskFreeCritBytes — the escalation of the minimum, a separate
	// disk_free_crit alert: action is needed immediately, the margin is
	// hours.
	DiskFreeCritBytes ByteSize `yaml:"disk_free_crit_bytes"`
	// QueueFreeze — the stillness window of the last-delivered-id of the
	// working stream group at a depth > 0 (the queue_frozen alert, the
	// poison signature T-159).
	QueueFreeze Duration `yaml:"queue_freeze_window"`
	// PSICPUPercent — the PSI CPU some avg300 threshold, % (the psi_cpu
	// alert).
	PSICPUPercent float64 `yaml:"psi_cpu_percent"`
	// PSIWindow — the PSI stability window above the threshold.
	PSIWindow Duration `yaml:"psi_cpu_window"`
	// MemHighPasses — the alert passes in a row with growth of the
	// memory.events high counter of the unit (the memory_high alert, the
	// MemoryHigh throttling of the http-worker — the precursor of the
	// T-157 fatals).
	MemHighPasses int `yaml:"mem_high_passes"`
	// RenderDebtDomains — the render debt threshold (the render_pending
	// flags + the q:render lag) that enables the render_debt alert on
	// growth (T-175): 50 k — below the 24.10 freeze debt (443 k), above
	// the healthy background of local overflows (thousands).
	RenderDebtDomains int64 `yaml:"render_debt_domains"`
	// RenderDebtPasses — the alert passes in a row with debt growth until
	// render_debt fires (T-175; following the pattern of mem_high_passes).
	RenderDebtPasses int `yaml:"render_debt_passes"`
}

// Web — the public landing service (stage 13, T-121; a production
// extension outside the main spec — a port from larasites). The public
// listener is its own http.Server inside OnRun: the service framework gives
// one port per service and hangs /metrics there (it must not be exposed
// publicly), so MetricsAddr stays on loopback while web.listen serves the
// visitor (the precedent of lifecycle control inside OnRun — the worker
// clients).
type Web struct {
	// Listen — the public address of the landing: [host]:port or an
	// absolute unix socket path (the deployment option behind nginx,
	// T-124). The default is loopback: publishing outward is a reverse
	// proxy with TLS (deploy/nginx/).
	Listen string `yaml:"listen"`
	// RateLimit — the limit of the live form checks per IP, requests per
	// minute (the fixed decision of stage 13: 4/min, like larasites).
	RateLimit int `yaml:"rate_limit"`
	// Timeout — the ceiling of a live form check (T-123): the full
	// pipeline of one name (DNS → HTTP → SPA render → analysis); the
	// answer to the visitor no later than the ceiling.
	Timeout Duration `yaml:"timeout"`
	// LiveConcurrency — the ceiling of the concurrent live form checks
	// (T-220): a semaphore before starting the pipeline — the excess gets
	// a fast 503 with Retry-After instead of waiting for a slot until
	// web.timeout. The pipeline runs on the shared long-lived circuit
	// (one resolver chain, a shared Fetcher, one render.Manager); the
	// parallelism is limited by the memory of the checks (page bodies,
	// render) in the unit budget.
	LiveConcurrency int `yaml:"live_concurrency"`
	// PublicHost — the canonical host of the public address of the
	// landing (T-227, for example "crawler.example.com"; with a
	// non-standard port — with it: "host:8443"): the absolute URLs of
	// sitemap.xml are built from it. An empty string — a fallback to the
	// request Host: behind the nginx reverse proxy it passes the
	// visitor's value as $host, and <loc> depends on the header of an
	// untrusted side (host-header poisoning) — a publicly published
	// landing must set the field. The field does not set the sitemap
	// scheme: a direct connection is a TLS fact, behind a proxy —
	// X-Forwarded-Proto of a trusted peer (see internal/web publicScheme).
	// An extension to the minimal set of spec 5.10.
	PublicHost string `yaml:"public_host"`
}

// Log — the logging level of the services (the internal/service framework).
// An extension to the minimal set of spec 5.10: the level is needed by the
// framework, not by a separate stage, hence its own top-level section.
type Log struct {
	Level string `yaml:"level"` // debug | info | warn | error
}

// Dashboard — the operator web dashboard of the pipeline (the dashboard
// spec §8.1; stage 15, T-127). Username/PasswordHash are not checked by
// the shared Validate: the credentials being required is a startup check of
// cmd/dashboard (the dashboard spec §7.7), otherwise the requirement would
// hit all the services of the project.
type Dashboard struct {
	// Listen — the public address of the dashboard ([host]:port): the
	// pages and the API (the dashboard spec §4.2). The default is
	// loopback: publishing outward is a reverse proxy with TLS (the
	// dashboard spec §9.1), not the service itself.
	Listen string `yaml:"listen"`
	// Refresh — the period of the background state snapshot build (the
	// dashboard spec §4.4), the corridor 1s..5m (the dashboard spec §8.3).
	Refresh Duration `yaml:"refresh"`
	// StatusCacheTTL — the TTL of the cache of the heavy status aggregates
	// (zones/rate/statuses — full domains passes, T-161): within the TTL
	// window the refresh passes reuse the last DB answer, after expiry —
	// one selection per section. The corridor 1m..5m (the T-161 tasking):
	// below a minute the cache does not help at the default refresh of
	// 5 s; above 5 minutes the summary loses its operational value (the
	// pipeline numbers change in seconds).
	StatusCacheTTL Duration `yaml:"status_cache_ttl"`
	// Username — the operator login (HTTP Basic, the dashboard spec §7.1).
	Username string `yaml:"username"`
	// PasswordHash — the password hash of the format of the dashboard
	// spec §7.2 (generation — crawlerctl dashboard-secret, parsing/
	// verification — internal/dashboard/auth); a plain password in the
	// config and the environment is forbidden.
	PasswordHash string `yaml:"password_hash"`
	// Probes — the liveness probes of the pipeline services for the health
	// section (the dashboard spec §4.3, §5.8): a name → the base metrics
	// URL of the service (the internal/service framework serves /livez on
	// METRICS_ADDR; in the docker network — http://<service-name>:<port>).
	// A nil/empty map — the section is "n/a".
	Probes map[string]string `yaml:"probes"`
	// RenderMetrics — the /metrics addresses of the render workers for the
	// engines panel (the render spec §4.6 item 4, T-246): the main build
	// (the stage engine — the engine labels at the counters) and the
	// fallback worker B build (the consumption/depth of q:render-fallback).
	// The same addresses Prometheus scrapes; nil/empty — the panel is
	// "n/a" (not an error, the style of probes).
	RenderMetrics []string `yaml:"render_metrics_urls"`
}

// Default returns the configuration with the default values of spec 5.10
// (except postgres.dsn: it must be set in the YAML or the environment).
func Default() *Config {
	return &Config{
		Postgres: Postgres{
			DSN:      "",
			PoolSize: 20,
		},
		Redis: Redis{
			Addr: "127.0.0.1:6379",
			// The per-stream limits — from queue.DefaultStreamMaxLen (T-025).
			MaxLen:      0,
			MaxAttempts: 3, // spec 4.2: 3 by default
			// More than the worst hold window of deliveries by a live
			// worker (render, the sliding consumption of T-142: a prefetch
			// of up to 2 batches +Concurrency ≈ 40 records; at the default
			// contexts=8/timeout=25 s the tail waits ≤ ~2.1 min; at
			// contexts=1 — up to ~13.8 min, the threshold is raised
			// together with lowering contexts (the numbers recalculated
			// for the default 8, T-169), see queue.DefaultClaimMinIdle).
			ClaimMinIdle: Duration(10 * time.Minute),
			// The rate of returning stuck records (T-153): 64 per pass —
			// the lower edge of the raised batch, consistent with the
			// invariant "the batch traversal < claim_min_idle": a typical
			// render record ~5 s × 64 ≈ 5.3 min < 10 min (a ×1.9 margin);
			// 128 no longer fits the typical case (≈10.7 min). The
			// rationale and the retry cost — queue.DefaultRecoverBatch
			// (the synchronous default of the queue).
			RecoverBatch: 64,
			// 30 s — the return latency MinIdle + up to one interval
			// (queue.DefaultRecoverInterval, the synchronous default of
			// the queue).
			RecoverInterval: Duration(30 * time.Second),
			// A minute — single percents of the Redis budget between
			// passes at the current stage rates
			// (queue.DefaultTrimInterval).
			TrimInterval: Duration(time.Minute),
			// 2 h — with a margin over the quiet between the deliveries of
			// an active stage (seconds) and shorter than the
			// between-deploy interval (queue.DefaultConsumerIdle, T-155).
			ConsumerIdle: Duration(2 * time.Hour),
		},
		Ingest: Ingest{
			Dir:           "/data/incoming",
			BatchSize:     50000,
			StableSeconds: 10,
			ScanInterval:  Duration(30 * time.Second),
			// Single junk does not hurt a file, while a file in the wrong
			// zone or a broken dump gives an error share of ~1 (T-016).
			MaxErrorRatio: 0.1,
			// The uncompressed .com zone ≈ 1.5 GB — a cap with a growth
			// and concatenation margin, without an excessive window for
			// zip bombs (T-133).
			ArchiveMaxUncompressed: ByteSize(8 << 30),
			// Real zone dumps are a few files (T-133).
			ArchiveMaxFiles: 1000,
		},
		Scheduler: Scheduler{
			RecheckInterval: Duration(4380 * time.Hour), // ≈ 6 months
			// The lower normative bound of spec 5.3 (1000–10000) and the
			// size of a measured q:dns record ≈ 64 KiB (T-141): the batch
			// is the unit of reprocessing when a stuck delivery returns
			// (XAUTOCLAIM returns the batch entirely) and of the reselection
			// by lease; a smaller batch is a smaller retry window; the
			// stream memory caps are consistent with this value
			// (queue.DefaultStreamMaxLen, docs/redis.md §3.1).
			BatchSize:           1000,
			QueueHighWatermark:  100000,
			Lease:               Duration(30 * time.Minute),
			TempErrorRetryAfter: Duration(24 * time.Hour),
			TempErrorMaxRetries: 3,
			// 5 minutes (T-161, the measurement §12): a GROUP BY pass over
			// 47 M rows of 17+ s every 30 s is one of the three main CPU
			// items of postgres; the dashboard gauge tolerates
			// five-minute freshness (the rationale —
			// internal/scheduler/backlog.go).
			BacklogInterval: Duration(5 * time.Minute),
			// No zone weights — an equal round-robin order (spec 5.3:
			// "optional").
			ZoneWeights: nil,
		},
		DNS: DNS{
			Resolvers:         []string{"127.0.0.1:53"},
			FallbackResolvers: []string{"1.1.1.1:53", "9.9.9.9:53"},
			Concurrency:       2000,
			MaxQPS:            10000,
			IPv6:              true,
			// The local unbound (spec 5.4) answers fast even on a cold
			// cache; the TCP fallback is rarer and costlier — a connection
			// setup.
			Timeout:    Duration(2 * time.Second),
			TCPTimeout: Duration(5 * time.Second),
			// Like the delivery attempts of the queue (T-025) and the
			// temp_error rechecks (T-031): a unified retry policy of the
			// system.
			Retries:    2,
			RetryDelay: Duration(200 * time.Millisecond),
			// = concurrency: the pool must not cut the planned
			// parallelism.
			UDPPoolSize: 2000,
		},
		HTTP: HTTP{
			Concurrency:    2000,
			PerIPLimit:     4,
			ConnectTimeout: Duration(5 * time.Second),
			// The limits table of spec 5.5: a TLS handshake of 5 s.
			TLSHandshakeTimeout: Duration(5 * time.Second),
			HeaderTimeout:       Duration(10 * time.Second),
			TotalTimeout:        Duration(20 * time.Second),
			MaxRedirects:        8,
			MaxBodyBytes:        ByteSize(2 << 20), // 2 MiB
			UserAgent:           DefaultUserAgent,
			// The limits table of spec 5.5: a minimum speed of 2 KB/s
			// (tarpits).
			MinBodySpeed: ByteSize(2 << 10),
			// The natural rate of the stage ~1000–2000 RPS (a page 1–2 s,
			// concurrency 2000): the default holds the order of the
			// outbound load of one IP (network tuning — spec 10.2).
			MaxRPS: 1000,
			// The task parallelism (T-139; the original default 256 — the
			// measurements T-092 §3/§11: 47.6 domains/s, anon ≈855 MiB).
			// Lowered to 192 (T-157): a day on 256 slots — two unit fatals
			// (§12: 05:48 OOM-kill, 09:17 the peak of 900.5 MiB =
			// MemoryMax; MemoryHigh 850M — 120,095 reclaim events); 192
			// slots are stable (the soak of wave 21: 860–893 MiB, oom=0,
			// NRestarts=0), the rate ≈17–19 domains/s. The slot
			// measurements before the early Put of T-147 (the bodies were
			// held until the end of the task); T-163 closed the rest of
			// the profile (the measurement metrics + the soft heap
			// ceiling mem_limit below) — the return to 256 per the
			// sampler measurement after the deploy (256 × 8 checks =
			// 2048 ≈ the transport connection ceiling 2000).
			TaskConcurrency: 192,
			// The read batch and the prefetch buffer: the read window does
			// not limit the parallelism (the sliding consumption); 16
			// smooths the round trips to Redis.
			TaskConsume: 16,
			// The pool inside a task: the per-IP semaphore (4) cuts the
			// real connection parallelism anyway (spec 5.5: 3–4 per IP).
			TaskPerDomain: 8,
			// The dead-host cutoff (T-164): enabled — the dead host
			// signature is strict (apex and www × both protocols × the
			// connect phase only); the rationale is at the field above.
			DeadHostCutoff: true,
			// The soft heap ceiling of the http-worker (T-163): below
			// the MemoryHigh 850M of the unit with a margin for the page
			// cache of the pack records (not part of the runtime
			// accounting). 0 — disabled.
			MemLimit: ByteSize(768 << 20),
		},
		Render: Render{
			// Chromium (FR-1): the default reproduces today's behavior
			// pixel-by-pixel — the production circuit changes only by an
			// explicit switch to moli|lightpanda (and then
			// render.remote_url must accompany it, FR-3).
			Engine:    EngineChromium,
			RemoteURL: "",
			// 8 (T-169) — a deliberate exceeding of the spec 5.6
			// recommendation (2–4) by measurement; the precedent of the
			// step 3→4 — T-142. The demand at a free gate is ≈1.2 pages/s
			// (the measurement §12, docs/load-measurements.md); the
			// capacity of 4 contexts is ≈0.8 pages/s (5.1 s/page, T-092
			// §4): the q:render drain of 29–34 tasks/min equaled the
			// inflow — the lag of ~99 k at a watermark of 100 k did not
			// melt since 22.10 (the disk is coupled with the drain: the
			// retention cohort waits for message consumption). 8 ≈ 1.6
			// pages/s — above the demand. Memory: the peak RSS of
			// Chromium at contexts=3 is 1024 MiB (T-092 §6), at 4
			// contexts ~550 MB (the measurement of 23.10) — sublinear
			// growth (the base process is shared); the restart threshold
			// restart_memory_bytes of 1.5 GiB and the cgroup of 2 GB
			// (spec 3.2) hold; the RSS check once per task, the snapshot
			// is no older than the 10 s cache window (T-065, T-229) +
			// a re-measurement after the deploy — with a systematic
			// living at the threshold one Chromium is saturated; the
			// answer is a second Chromium (:9223) and a second worker
			// against it (deploy.md §2.2), not the threshold. The
			// render-worker unit holds the DOM in flight ≤ Concurrency ×
			// max_store_bytes (≈16 MiB + transient copies): 128M is
			// already tight (§6: max 128 MiB, by 23.10 117–128 MB + swap)
			// — MemoryMax 256M (T-169, deploy.md §2).
			Contexts: 8,
			Timeout:  Duration(25 * time.Second),
			// The tarpit budget (T-148): 2 of the 3 deliveries
			// (redis.max_attempts) may end in a page timeout, a tarpit on
			// the next one — the record finalizes without render: the
			// work is not lost to q:dead, the slots are not occupied for
			// days. The first attempt may time out from a jam (the
			// measurement of 2026-10-23: 0.34 pages/s, the slots in
			// timeouts) — a retry after redis.claim_min_idle gives the
			// page a chance in changed conditions; further the retry is
			// predictable. The incident of 2026-10-23: 5507 dead records
			// of q:render "the attempt limit 3 is exhausted" within hours
			// — all timeouts.
			TimeoutAttempts:   2,
			RestartEveryPages: 500,
			// 1.5 GiB = 75% of the Chromium budget ≈ 2 GB (spec 3.2, the
			// cgroup MemoryMax ≈ 2 GB): a restart — before a cgroup-OOM;
			// the margin is for the growth between checks (once per task,
			// the RSS snapshot is no older than the 10 s cache window,
			// T-229) and for tmpfs /dev/shm, which counts in the cgroup
			// memory but not in the process RSS. At contexts=8 the
			// threshold is not raised (T-169): the check once per task —
			// the pages in flight grow the RSS even after the last
			// check; a 512 MiB margin covers that growth; narrowing it
			// by raising the threshold brings the soft restart closer to
			// the hard cgroup OOM. A systematic RSS living above the
			// threshold is a saturation signal of one Chromium: the
			// answer is a second Chromium (:9223) and a second worker
			// (deploy.md §2.2), not a threshold raise.
			RestartMemoryBytes: ByteSize(1536 << 20),
			// Blocking CSS in the render context (T-213) is disabled by
			// default: the production behavior does not change until the
			// A/B (the computed styles risk — the rationale at the
			// BlockStylesheet field).
			BlockStylesheet: false,
			// Blocking tracker domains in the render context (T-214) is
			// enabled by default: analytics does not participate in the
			// detection; the traffic and CPU savings are direct (the
			// rationale at the BlockTrackerDomains field).
			BlockTrackerDomains: true,
			// The q:render capacity gates (T-174): ≈2 h of consumption
			// buffer (the measurement of 24.10: ≈5.1 k tasks/h) — the
			// rationale at the QueueHighWatermark field.
			QueueHighWatermark: 10000,
			// The zone priority of the finish pass (T-184) is not set —
			// all the zones are drained in one order (T-181); production
			// puts the parking zones here per the T-176 measurement
			// (xn--p1ai).
			RequeueDeprioritizedZones: nil,
			// The late re-publisher selection ceiling (T-274; the value
			// T-273): the selection is adaptive and fast — the rationale
			// at the LateSelectTimeout field.
			LateSelectTimeout: Duration(300 * time.Second),
			// The Chromium fallback queue (FR-14) is disabled: the primary
			// worker finalizes without render as today — enabling is a
			// separately configured flag, without deploying new entities.
			// The rationales of the ceilings — the RenderFallback fields.
			Fallback: RenderFallback{
				Enabled:          false,
				WorkerRate:       100,
				SharePercent:     30,
				HealthGateStreak: 3,
			},
		},
		Content: Content{
			Dir:            "/data/content",
			PackSize:       ByteSize(512 << 20), // 512 MiB
			MaxStoreBytes:  ByteSize(2 << 20),   // 2 MiB
			ZstdDictionary: "/data/content/dict.zst",
			// Spec 8 sets no number: 5% is a typical lower bound for data
			// volumes (the space is needed for both the compaction and
			// the logs); the validation does not let the control be
			// disabled with zero.
			DiskFreePercent: 5,
			// The life window of the non-find content (T-166, the
			// measurements §12 of docs/load-measurements.md): 6 h ≈ 5 GiB
			// steady at an inflow of 20 GiB/day — fits the volume without
			// an extension; the consumer lags (the analysis — minutes,
			// the render queue — hours) are covered with a margin; the
			// rationale — docs/content.md "Content retention and the
			// non-find trim".
			Retention: Duration(6 * time.Hour),
			// An hour, like the pack compaction (T-045): its pass returns
			// the space, retention only prepares the dead blocks.
			RetentionInterval: Duration(time.Hour),
			// The lower bound of the tasking range (10–50 k): ~13 chunks
			// in an hourly pass drain an inflow of ≈3 M rows/day.
			RetentionBatch: 10000,
			// 0 — the shared threshold scheduler.queue_high_watermark (the
			// interlock by the same criterion as the q:analyze publication
			// gate, T-026).
			RetentionAnalyzeWatermark: 0,
			// 2 h (T-274, formerly 3600 s as a T-273 constant): a doubled
			// margin over the production measurements of 10–11.11.2026 of
			// the first catch-up pass from a zero cursor under load — the
			// rationale at the RetentionSelectTimeout field. Duplicates
			// the code default (content.DefaultRetentionSelectTimeout)
			// like the other retention keys (config does not import the
			// service packages).
			RetentionSelectTimeout: Duration(7200 * time.Second),
		},
		Analyzer: Analyzer{
			RulesDir:        "/etc/crawler/rules",
			LaravelMinScore: 0.6,
		},
		// The initial fill of the subdomains table; "" is the domain apex.
		Subdomains: []string{"", "www", "admin", "crm", "api", "app"},
		Monitor: Monitor{
			Listen: ":9100",
			// 30 minutes (T-199): T-161 raised the original 60 s to
			// 5 minutes (the measurement §12: the UPDATE aggregate over
			// 47 M rows is one of the three main CPU items of postgres);
			// after the 3bffc62 deploy the slow query window measurement
			// showed the reconciliation as the only remaining full
			// domains pass — 48% of the slow DB time at a 5 min interval;
			// the operator decision of 26.10 per the incremental-counters
			// spec §7.1 — dilute to 30 min (the rationale —
			// internal/monitor/zonestats.go). The stall threshold is
			// 5 × the interval = 2.5 h (the unit is the period of this
			// loop).
			ZoneStatsInterval: Duration(30 * time.Minute),
			// 5 minutes (T-206): the former alert frequency before T-199 —
			// the detection windows in passes returned to the former
			// minutes (the rationale — internal/monitor/alerts.go,
			// DefaultAlertsInterval).
			AlertsInterval: Duration(5 * time.Minute),
			// The T-173 load report: 10 minutes — the window catches the
			// CPU/memory starvation episodes (tens of minutes); 144
			// messages/day do not drown the alerts in the group;
			// 0 — disable (the Validate corridor).
			LoadReportInterval: Duration(10 * time.Minute),
			// The slow queries T-177: 10 s — a pg_stat_activity snapshot
			// (microseconds at ≤ 50 connections); longer — the episodes
			// shorter than the interval are lost; a threshold of 3 s —
			// the production log_min_duration_statement threshold.
			SlowQueryInterval:    Duration(10 * time.Second),
			SlowQueryMinDuration: Duration(3 * time.Second),
			// The Telegram delivery of the slow queries is disabled:
			// unlike the alerts (Telegram: true above — the operator does
			// not read the logs, the T-162 motivation) a slow query is
			// diagnostics, already duplicated by the monitor log, the
			// metrics and the PostgreSQL server log; delivering it to the
			// group is a deliberate choice (the rationale — the
			// SlowQueryTelegram field).
			SlowQueryTelegram: false,
			// The render debt (T-175): 5 minutes (the parity with the zone
			// recount ended — T-199 diluted the reconciliation to
			// 30 min); the pass is cheap (a Lookup + an XLEN), the period
			// chooses the gauge freshness — the debt changes at the drain
			// rate (~1 row/s), it does not move faster than a five-minute
			// window; the corridor — following the pattern of
			// zone_stats_interval. The render_debt alert is evaluated by
			// the alert pass (alerts_interval, T-206), not by this field.
			RenderDebtInterval: Duration(5 * time.Minute),
			// The ingest loopback via METRICS_ADDR (the systemd units bind
			// 127.0.0.1:9101); the docker networks override it with the
			// service name (config.dev.yaml:
			// http://ingest:9101/metrics).
			IngestMetricsURL: "http://127.0.0.1:9101/metrics",
			// The render worker loopback via METRICS_ADDR (port 9105);
			// the docker networks override it with the service name
			// (config.dev.yaml: http://render-worker:9105/metrics).
			RenderMetricsURL: "http://127.0.0.1:9105/metrics",
			Alerts: Alerts{
				// Enabled by default: the delivery decision is made by
				// the secrets/script on the host (missing any — silently
				// disabled), not by the config silence — the operator
				// does not read the logs (the T-162 motivation).
				Telegram: true,
				// The script rides in the deploy archive together with
				// the binaries (deploy.yml: deploy/telegram-notify.sh →
				// /opt/crawler/bin, install chmod 0755) — the production
				// standard path; no separate installation needed
				// (docs/operations.md §4.3).
				TelegramScript: "/opt/crawler/bin/telegram-notify.sh",
				// 15 GiB Warn / 10 GiB escalation: 10% of the 79 GB
				// volume is a late percent threshold (≈5 h until full at
				// ≈36 GiB/day, the measurement §12); the rationales —
				// internal/monitor DefaultDiskFreeMinBytes/CritBytes.
				DiskFreeMinBytes:  ByteSize(15 << 30),
				DiskFreeCritBytes: ByteSize(10 << 30),
				QueueFreeze:       Duration(15 * time.Minute),
				PSICPUPercent:     90,
				PSIWindow:         Duration(30 * time.Minute),
				MemHighPasses:     2,
				// The 24.10 freeze debt was 443 k: the threshold is an
				// order of magnitude lower; the background of local
				// overflows is thousands (the rationale — internal/monitor
				// DefaultRenderDebtDomains).
				RenderDebtDomains: 50000,
				RenderDebtPasses:  2,
			},
		},
		Web: Web{
			// Loopback: publishing outward is a reverse proxy with TLS
			// (T-124), not the service itself.
			Listen: "127.0.0.1:8080",
			// The fixed decision of stage 13 — like the larasites form
			// throttling: 4 checks per minute from one IP.
			RateLimit: 4,
			// The full pipeline of one name: the DNS episode (seconds) +
			// the HTTP of both schemes (2 × http.total_timeout 20 s) +
			// the SPA render (render.timeout 25 s) + the analysis
			// (moments) — 60 s covers the worst case without an eternal
			// wait for the visitor.
			Timeout: Duration(60 * time.Second),
			// The resource ceiling of the form (T-220): the pipelines run
			// on the shared long-lived circuit, but every live check holds
			// page bodies and the SPA render — a parallelism above a few
			// hits the MemoryMax 256M of the unit. 4 — like rate_limit:
			// a rare visitor does not wait for a slot, a burst of
			// interest is discharged with a 503.
			LiveConcurrency: 4,
			// The canonical host of the public address is not set:
			// sitemap.xml builds <loc> from the request Host (T-227); a
			// publicly published landing sets web.public_host to its own
			// domain.
			PublicHost: "",
		},
		Dashboard: Dashboard{
			// Loopback: publishing outward is a reverse proxy with TLS
			// (the dashboard spec §9.1), not the service itself.
			Listen: "127.0.0.1:8090",
			// The "now" of the operator summary (the dashboard spec §6.4:
			// the frontend auto-refresh ~5 s) without extra DB load (the
			// dashboard spec §4.4).
			Refresh: Duration(5 * time.Second),
			// The lower bound of the T-161 corridor (1–5 min): at a
			// refresh of 5 s — one full domains pass per minute instead
			// of twelve (the rationale — internal/status/cache.go).
			StatusCacheTTL: Duration(time.Minute),
			// The credentials are not set: cmd/dashboard checks that they
			// are required at start (the dashboard spec §7.7), not the
			// shared Validate.
			Username:     "",
			PasswordHash: "",
			// No probes — the health section is "n/a" (the dashboard spec
			// §5.8).
			Probes: nil,
			// The engines panel (T-246, S-P2-5): the same exposure of the
			// main render-worker build as the monitor probe
			// (RenderMetricsURL above, the metrics port 9105) — the panel
			// works out of the box; the docker networks override it with
			// the service name (config.dev.yaml:
			// http://render-worker:9105). The worker B exposure of the
			// fallback is added when enabling the fallback (deploy.md
			// §2.3); an empty list — the panel is "n/a".
			RenderMetrics: []string{"http://127.0.0.1:9105/metrics"},
		},
		Log: Log{
			Level: "info",
		},
	}
}

// normalize brings the values to the canonical form (case/spaces).
func (c *Config) normalize() {
	for i, s := range c.Subdomains {
		c.Subdomains[i] = strings.ToLower(strings.TrimSpace(s))
	}
	// The names of the deprioritized zones — punycode in lower case
	// without spaces (like zones.name and the keys of
	// scheduler.zone_weights); a duplicate after folding is caught by
	// Validate.
	for i, s := range c.Render.RequeueDeprioritizedZones {
		c.Render.RequeueDeprioritizedZones[i] = strings.ToLower(strings.TrimSpace(s))
	}
	// The zone path allowlist (the tld-only-zones spec §6.1) — the same
	// canonical form as the zones of the reference table: NormalizeZone
	// reduces to lower-case punycode (idn.NormalizeZone, like the ingest
	// directories in ParsePath). A junk entry is not normalized and stays
	// as is — Validate will catch it with the original value in the
	// message. Duplicates after folding collapse (the style of the zone
	// weight keys): the list is a set of names, the first-occurrence
	// order is preserved.
	if len(c.Ingest.AllowedZonePaths) > 0 {
		seen := make(map[string]bool, len(c.Ingest.AllowedZonePaths))
		out := make([]string, 0, len(c.Ingest.AllowedZonePaths))
		for _, z := range c.Ingest.AllowedZonePaths {
			if nz, err := idn.NormalizeZone(z); err == nil {
				z = nz
			}
			if seen[z] {
				continue
			}
			seen[z] = true
			out = append(out, z)
		}
		c.Ingest.AllowedZonePaths = out
	}
	// The render engine — lower case without edge spaces (like log.level):
	// "Chromium"/" MOLI " in the YAML/environment is an acceptable
	// spelling; validation compares the canonical one and the consumers
	// read it (the render spec §4.1 item 1).
	c.Render.Engine = strings.ToLower(strings.TrimSpace(c.Render.Engine))
	c.Log.Level = strings.ToLower(strings.TrimSpace(c.Log.Level))
	// The canonical host of the landing — without edge spaces (no IDN
	// normalization: the field sets the public address host as is;
	// punycode is the operator's concern; the shape is checked by
	// Validate).
	c.Web.PublicHost = strings.TrimSpace(c.Web.PublicHost)
	// The zone weight keys — punycode names in lower case (like
	// zones.name); the map is rebuilt in ascending key order so that a
	// collision after folding ("Com" and "com" in one YAML) resolves
	// deterministically — the lexicographically greater key wins.
	if len(c.Scheduler.ZoneWeights) > 0 {
		names := make([]string, 0, len(c.Scheduler.ZoneWeights))
		for name := range c.Scheduler.ZoneWeights {
			names = append(names, name)
		}
		sort.Strings(names)
		w := make(map[string]int, len(names))
		for _, name := range names {
			w[strings.ToLower(strings.TrimSpace(name))] = c.Scheduler.ZoneWeights[name]
		}
		c.Scheduler.ZoneWeights = w
	}
	// The dashboard probe keys — service names in lower case without
	// spaces (like the names in the docker network and the health section
	// header); the values are the probe URLs with trimmed edge spaces.
	// The map is rebuilt in ascending key order: a collision after
	// folding ("Mon" and "mon" in one YAML) resolves deterministically —
	// the lexicographically greater key wins (like the zone weights).
	if len(c.Dashboard.Probes) > 0 {
		names := make([]string, 0, len(c.Dashboard.Probes))
		for name := range c.Dashboard.Probes {
			names = append(names, name)
		}
		sort.Strings(names)
		p := make(map[string]string, len(names))
		for _, name := range names {
			p[strings.ToLower(strings.TrimSpace(name))] = strings.TrimSpace(c.Dashboard.Probes[name])
		}
		c.Dashboard.Probes = p
	}
	// The /metrics addresses of the render workers of the engines panel —
	// without edge spaces and empty elements (an extra comma in the YAML
	// must not bring the section down); the order is preserved — it does
	// not affect the aggregation (the counters are summed) but is
	// deterministic for the logs.
	if len(c.Dashboard.RenderMetrics) > 0 {
		urls := make([]string, 0, len(c.Dashboard.RenderMetrics))
		for _, u := range c.Dashboard.RenderMetrics {
			if u = strings.TrimSpace(u); u != "" {
				urls = append(urls, u)
			}
		}
		c.Dashboard.RenderMetrics = urls
	}
}

var subdomainLabelRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// zoneNameRe — a zone name in lower-case punycode, as the zones reference
// table stores it (ingest normalizes via idn.NormalizeZone, spec 5.2):
// DNS labels separated by dots, IDN zones (xn--p1ai) are allowed, the
// unicode form is not (the reference table does not contain it, the key
// would never match). The regex is used only by the validations of the
// keys of scheduler.zone_weights and render.requeue_deprioritized_zones
// (the keys are TLDs, the tld-only-zones spec §6.2); the dots in the
// pattern are left deliberately — do not change (the spec §6.2): the
// allowlist ingest.allowed_zone_paths is not validated by this regex at
// all (its own check — idn.NormalizeZone + at least one dot, the spec
// §6.1). The full length separately — ≤ 253 octets (spec 5.2 item 4).
var zoneNameRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*$`)

// Validate checks the configuration values and returns an error with the
// list of all the problems at once (the format "section.field: what is
// wrong").
func (c *Config) Validate() error {
	var problems []string

	// postgres
	if strings.TrimSpace(c.Postgres.DSN) == "" {
		problems = append(problems, "postgres.dsn: required (postgres.dsn in YAML or the CRAWLER_POSTGRES_DSN/APP_PG_DSN env vars)")
	}
	if !inRange(c.Postgres.PoolSize, 1, 100) {
		problems = append(problems, rangeMsg("postgres.pool_size", c.Postgres.PoolSize, 1, 100)+"; spec recommendation: 20–50 across ALL services")
	}

	// redis
	if err := checkHostPort(c.Redis.Addr); err != nil {
		problems = append(problems, fmt.Sprintf("redis.addr: expected host:port, got %q", c.Redis.Addr))
	}
	// 0 — the per-stream limits from the queue package (T-025); an
	// explicit limit below 1000 would truncate the working set of even a
	// small queue.
	if c.Redis.MaxLen != 0 && !inRange(c.Redis.MaxLen, 1000, 100000000) {
		problems = append(problems, rangeMsg("redis.maxlen", c.Redis.MaxLen, 1000, 100000000)+" (0 — per-stream default limits)")
	}
	// A minimum of 1: zero deliveries would mean q:dead before the first
	// processing.
	if !inRange(c.Redis.MaxAttempts, 1, 100) {
		problems = append(problems, rangeMsg("redis.max_attempts", c.Redis.MaxAttempts, 1, 100)+" (spec 4.2: default 3)")
	}
	if c.Redis.ClaimMinIdle <= 0 {
		problems = append(problems, nonPositiveDur("redis.claim_min_idle", c.Redis.ClaimMinIdle))
	}
	// The rate of returning stuck records (T-153): a minimum of 1 — one
	// re-claim per pass; the ceiling of 1000 is against typos (the real
	// ceiling is the invariant "batch × the worst record time <
	// claim_min_idle"; it is arithmetic, not a range — see
	// docs/configuration.md, the redis section).
	if !inRange(c.Redis.RecoverBatch, 1, 1000) {
		problems = append(problems, rangeMsg("redis.recover_batch", c.Redis.RecoverBatch, 1, 1000))
	}
	// More often than a second — idle passes over the four streams (like
	// trim_interval); rarer than an hour — the PEL traversal after an
	// incident stretches to days (5 k records / 64 per pass × 1 h ≈
	// 78 h).
	if c.Redis.RecoverInterval < Duration(time.Second) || c.Redis.RecoverInterval > Duration(time.Hour) {
		problems = append(problems, fmt.Sprintf("redis.recover_interval: expected %s..%s, got %s",
			Duration(time.Second), Duration(time.Hour), c.Redis.RecoverInterval))
	}
	// The corridor of the meaningful values: more often than a second —
	// idle passes over the four streams; rarer than a day — the history
	// accumulates to the MAXLEN cap (the protection is there anyway, but
	// the meaning of the mechanism is lost).
	if c.Redis.TrimInterval < Duration(time.Second) || c.Redis.TrimInterval > Duration(24*time.Hour) {
		problems = append(problems, fmt.Sprintf("redis.trim_interval: expected %s..%s, got %s",
			Duration(time.Second), Duration(24*time.Hour), c.Redis.TrimInterval))
	}
	// The consumer hygiene (T-155): a threshold below a minute would
	// clean out the consumers of live workers between reads of an empty
	// stream (XREADGROUP without records does not refresh the consumer
	// idle) — an idle churn of recreation; above 30 days — the dead ones
	// accumulate faster than deploys, the mechanism is pointless.
	if c.Redis.ConsumerIdle < Duration(time.Minute) || c.Redis.ConsumerIdle > Duration(720*time.Hour) {
		problems = append(problems, fmt.Sprintf("redis.consumer_idle: expected %s..%s, got %s",
			Duration(time.Minute), Duration(720*time.Hour), c.Redis.ConsumerIdle))
	}

	// ingest
	if c.Ingest.Dir == "" {
		problems = append(problems, "ingest.dir: not set (the watched incoming directory)")
	}
	if !inRange(c.Ingest.BatchSize, 1, 1000000) {
		problems = append(problems, rangeMsg("ingest.batch_size", c.Ingest.BatchSize, 1, 1000000))
	}
	if !inRange(c.Ingest.StableSeconds, 1, 3600) {
		problems = append(problems, rangeMsg("ingest.stable_seconds", c.Ingest.StableSeconds, 1, 3600))
	}
	if c.Ingest.ScanInterval < Duration(10*time.Millisecond) || c.Ingest.ScanInterval > Duration(time.Hour) {
		problems = append(problems, fmt.Sprintf("ingest.scan_interval: expected %s..%s, got %s",
			Duration(10*time.Millisecond), Duration(time.Hour), c.Ingest.ScanInterval))
	}
	if c.Ingest.MaxErrorRatio <= 0 || c.Ingest.MaxErrorRatio > 1 {
		problems = append(problems, fmt.Sprintf("ingest.max_error_ratio: expected (0, 1], got %v", c.Ingest.MaxErrorRatio))
	}
	// The archive limits (T-133) cannot be disabled with zero: the zip
	// bomb protection is mandatory; the upper bound is against typos, not
	// against operations.
	if c.Ingest.ArchiveMaxUncompressed <= 0 || c.Ingest.ArchiveMaxUncompressed > 1<<40 {
		problems = append(problems, fmt.Sprintf(
			"ingest.archive_max_uncompressed: expected %s..%s, got %s (cap on the total uncompressed size of archive members)",
			ByteSize(1), ByteSize(1<<40), c.Ingest.ArchiveMaxUncompressed))
	}
	if !inRange(c.Ingest.ArchiveMaxFiles, 1, 1000000) {
		problems = append(problems, rangeMsg("ingest.archive_max_files", c.Ingest.ArchiveMaxFiles, 1, 1000000))
	}
	// The zone path allowlist (the tld-only-zones spec §6.1): every entry
	// is a valid zone name (NormalizeZone, the same pipeline as the ingest
	// directories) with at least one dot: a TLD zone has no business in
	// the allowlist — its directory is working anyway. The normalization
	// and dedup already happened at load; the normalization skips junk —
	// here it is caught with the original value in the message.
	for _, z := range c.Ingest.AllowedZonePaths {
		nz, err := idn.NormalizeZone(z)
		if err != nil {
			problems = append(problems, fmt.Sprintf(
				"ingest.allowed_zone_paths: %q is not a zone name (%v); expected a multi-level zone name in lowercase punycode with at least one dot — e.g. crawl.test", z, err))
			continue
		}
		if !strings.Contains(nz, ".") {
			problems = append(problems, fmt.Sprintf(
				"ingest.allowed_zone_paths: %q — a TLD has no place in the allowlist (its directory is already the working one); expected a multi-level name with a dot — e.g. crawl.test", z))
		}
	}

	// scheduler
	if c.Scheduler.RecheckInterval <= 0 {
		problems = append(problems, nonPositiveDur("scheduler.recheck_interval", c.Scheduler.RecheckInterval))
	}
	// The period of the backlog depth collector (T-161): the corridor
	// 30s..1h — the lower bound of the spec 5.9 corridor (more often — an
	// aggregate over all of domains without an index, T-104, costlier
	// than the metric itself); the upper bound is against absurdity: a
	// depth older than an hour stops being an observable quantity (the
	// stall alert is zones.updated_at, not this gauge).
	if c.Scheduler.BacklogInterval < Duration(30*time.Second) || c.Scheduler.BacklogInterval > Duration(time.Hour) {
		problems = append(problems, fmt.Sprintf("scheduler.backlog_interval: expected %s..%s, got %s (backlog depth gauge period, T-161)",
			Duration(30*time.Second), Duration(time.Hour), c.Scheduler.BacklogInterval))
	}
	// Spec 5.3: the scheduler batches of 1000–10000 are normative bounds
	// (T-027), not a recommendation: the batch sets the selection memory
	// (spec 3.5) and the size of the q:dns messages. The stream caps
	// against the Redis budget (T-141, spec 3.2) are consistent for the
	// whole range (the min/maxSchedulerBatchSize constants): the invariant
	// "the sum of the caps ≤ the budget" is pinned by
	// TestSchedulerBatchFitsStreamBudget (config_test.go) — it fires when
	// the policy constants or the range bounds change. An explicit
	// redis.maxlen is a deliberate exit from the policy: the operator
	// holds its arithmetic (the arithmetic — docs/redis.md §3.1).
	if !inRange(c.Scheduler.BatchSize, minSchedulerBatchSize, maxSchedulerBatchSize) {
		problems = append(problems, rangeMsg("scheduler.batch_size", c.Scheduler.BatchSize, minSchedulerBatchSize, maxSchedulerBatchSize)+" (spec 5.3)")
	}
	if c.Scheduler.QueueHighWatermark < 1 {
		problems = append(problems, "scheduler.queue_high_watermark: expected a positive integer, got "+strconv.Itoa(c.Scheduler.QueueHighWatermark))
	}
	if c.Scheduler.Lease <= 0 {
		problems = append(problems, nonPositiveDur("scheduler.lease", c.Scheduler.Lease))
	}
	if c.Scheduler.TempErrorRetryAfter <= 0 {
		problems = append(problems, nonPositiveDur("scheduler.temp_error_retry_after", c.Scheduler.TempErrorRetryAfter))
	}
	if !inRange(c.Scheduler.TempErrorMaxRetries, 1, 100) {
		problems = append(problems, rangeMsg("scheduler.temp_error_max_retries", c.Scheduler.TempErrorMaxRetries, 1, 100)+"; minimum 1 — temp_error by definition requires a retry (spec 6.2)")
	}
	// The stream caps (T-141) — see the comment at batch_size.
	// The zone weights (spec 5.3, T-106): the keys in ascending order —
	// a deterministic list of problems. A name missing from the zones
	// reference table is not a configuration error (ingest creates the
	// zone with the first file, spec 5.1): the weighting simply does not
	// fire, the resolution in cmd/scheduler warns with a Warn. A
	// syntactically impossible name is caught here — the reference table
	// cannot contain such a name.
	for _, name := range sortedKeys(c.Scheduler.ZoneWeights) {
		if name == "" || len(name) > 253 || !zoneNameRe.MatchString(name) {
			problems = append(problems, fmt.Sprintf(
				"scheduler.zone_weights: %q — expected a zone name: a TLD in lowercase punycode, as in the zones reference table (in, uk; IDN — xn--p1ai)", name))
			continue
		}
		if w := c.Scheduler.ZoneWeights[name]; !inRange(w, 1, 1000) {
			problems = append(problems, fmt.Sprintf(
				"scheduler.zone_weights: zone %q: expected weight 1..1000, got %d (a zone's share of a chunk is proportional to its weight; 1 — equal round-robin order)", name, w))
		}
	}

	// dns
	if len(c.DNS.Resolvers) == 0 {
		problems = append(problems, "dns.resolvers: at least one resolver is required (e.g. a local unbound)")
	}
	for _, r := range c.DNS.Resolvers {
		if err := checkHostPort(r); err != nil {
			problems = append(problems, fmt.Sprintf("dns.resolvers: %q: expected host:port", r))
		}
	}
	for _, r := range c.DNS.FallbackResolvers {
		if err := checkHostPort(r); err != nil {
			problems = append(problems, fmt.Sprintf("dns.fallback_resolvers: %q: expected host:port", r))
		}
	}
	if !inRange(c.DNS.Concurrency, 1, 100000) {
		problems = append(problems, rangeMsg("dns.concurrency", c.DNS.Concurrency, 1, 100000))
	}
	if !inRange(c.DNS.MaxQPS, 1, 1000000) {
		problems = append(problems, rangeMsg("dns.max_qps", c.DNS.MaxQPS, 1, 1000000))
	}
	if c.DNS.Timeout <= 0 {
		problems = append(problems, nonPositiveDur("dns.timeout", c.DNS.Timeout))
	}
	if c.DNS.TCPTimeout <= 0 {
		problems = append(problems, nonPositiveDur("dns.tcp_timeout", c.DNS.TCPTimeout))
	}
	// A minimum of 1 — spec 5.4 requires retries; the maximum —
	// dnsclient.MaxRetries.
	if !inRange(c.DNS.Retries, 1, 10) {
		problems = append(problems, rangeMsg("dns.retries", c.DNS.Retries, 1, 10)+"; minimum 1 — spec 5.4 requires retries")
	}
	if c.DNS.RetryDelay <= 0 {
		problems = append(problems, nonPositiveDur("dns.retry_delay", c.DNS.RetryDelay))
	}
	if !inRange(c.DNS.UDPPoolSize, 1, 100000) {
		problems = append(problems, rangeMsg("dns.udp_pool_size", c.DNS.UDPPoolSize, 1, 100000))
	}

	// http
	if !inRange(c.HTTP.Concurrency, 1, 100000) {
		problems = append(problems, rangeMsg("http.concurrency", c.HTTP.Concurrency, 1, 100000))
	}
	if !inRange(c.HTTP.PerIPLimit, 1, 64) {
		problems = append(problems, rangeMsg("http.per_ip_limit", c.HTTP.PerIPLimit, 1, 64)+"; spec recommendation: 3–4")
	}
	// The parker prefixes (T-165): the key must be a parseable CIDR
	// (IPv4/IPv6), the limit — in the per_ip_limit range; one network
	// cannot be listed twice — the masked "195.208.0.0/16" and
	// "195.208.9.9/16" are one network, a duplicate would make the limit
	// choice ambiguous (longest-prefix does not distinguish equal
	// lengths).
	seenNets := make(map[string]struct{}, len(c.HTTP.PerIPOverrides))
	for cidr, limit := range c.HTTP.PerIPOverrides {
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil {
			problems = append(problems, fmt.Sprintf("http.per_ip_overrides: key %q — expected an IPv4/IPv6 CIDR (e.g. 195.208.0.0/16)", cidr))
			continue
		}
		if !inRange(limit, 1, 64) {
			problems = append(problems, rangeMsg("http.per_ip_overrides["+cidr+"]", limit, 1, 64)+"; same range as http.per_ip_limit")
		}
		masked := prefix.Masked().String()
		if _, dup := seenNets[masked]; dup {
			problems = append(problems, fmt.Sprintf("http.per_ip_overrides: network %s listed twice (after CIDR masking)", masked))
			continue
		}
		seenNets[masked] = struct{}{}
	}
	if c.HTTP.ConnectTimeout <= 0 {
		problems = append(problems, nonPositiveDur("http.connect_timeout", c.HTTP.ConnectTimeout))
	}
	if c.HTTP.TLSHandshakeTimeout <= 0 {
		problems = append(problems, nonPositiveDur("http.tls_handshake_timeout", c.HTTP.TLSHandshakeTimeout))
	}
	if c.HTTP.HeaderTimeout <= 0 {
		problems = append(problems, nonPositiveDur("http.header_timeout", c.HTTP.HeaderTimeout))
	}
	if c.HTTP.TotalTimeout <= 0 {
		problems = append(problems, nonPositiveDur("http.total_timeout", c.HTTP.TotalTimeout))
	}
	if !inRange(c.HTTP.MaxRedirects, 0, 100) {
		problems = append(problems, rangeMsg("http.max_redirects", c.HTTP.MaxRedirects, 0, 100)+" (0 — do not follow redirects)")
	}
	if c.HTTP.MaxBodyBytes <= 0 {
		problems = append(problems, "http.max_body_bytes: expected a positive size, got "+c.HTTP.MaxBodyBytes.String())
	}
	if c.HTTP.MinBodySpeed <= 0 {
		problems = append(problems, "http.min_body_speed: expected a positive speed (bytes/s), got "+c.HTTP.MinBodySpeed.String())
	}
	if !inRange(c.HTTP.MaxRPS, 1, 1000000) {
		problems = append(problems, rangeMsg("http.max_rps", c.HTTP.MaxRPS, 1, 1000000))
	}
	// The task parallelism (T-139): the ceiling of 10000 is against
	// typos, not operations (the real ceiling is memory: each task holds
	// the domain result with the bodies of the original records); the
	// read batch is the prefetch buffer size; a pool inside a task wider
	// than the number of checks (2 × subdomains ≤ 126, §5.10) makes no
	// sense.
	if !inRange(c.HTTP.TaskConcurrency, 1, 10000) {
		problems = append(problems, rangeMsg("http.task_concurrency", c.HTTP.TaskConcurrency, 1, 10000))
	}
	if !inRange(c.HTTP.TaskConsume, 1, 1000) {
		problems = append(problems, rangeMsg("http.task_consume", c.HTTP.TaskConsume, 1, 1000))
	}
	if !inRange(c.HTTP.TaskPerDomain, 1, 126) {
		problems = append(problems, rangeMsg("http.task_per_domain", c.HTTP.TaskPerDomain, 1, 126)+" (checks per task ≤ 2 × 63 subdomains)")
	}
	// The soft heap ceiling (T-163): 0 — disabled; the working range is
	// from the worker idle footprint (below — an eternal GC) to a
	// reasonable budget of one Go service (spec 3.2). A value above the
	// MemoryHigh/MemoryMax of the unit is meaningless but not forbidden:
	// the ceiling is set by the operator per their deployment (a unit, a
	// dev stack container); the config does not know about the unit.
	if t := c.HTTP.MemLimit; t != 0 && (t < 256<<20 || t > 16<<30) {
		problems = append(problems, fmt.Sprintf("http.mem_limit: expected 0 (disabled) or %s..%s, got %s",
			ByteSize(256<<20), ByteSize(16<<30), t))
	}
	if strings.TrimSpace(c.HTTP.UserAgent) == "" {
		problems = append(problems, "http.user_agent: required (an honest User-Agent with contacts, spec 5.5/§9)")
	}
	// The value goes into the request header as one field: line breaks
	// and control characters would break the request (and \r\n is a
	// header injection).
	for _, r := range c.HTTP.UserAgent {
		if r < 0x20 || r == 0x7f {
			problems = append(problems, "http.user_agent: newlines and control characters are not allowed (the value goes into a header)")
			break
		}
	}
	if n := len(c.HTTP.UserAgent); n > maxUserAgentLen {
		problems = append(problems, fmt.Sprintf("http.user_agent: too long (%d bytes, maximum %d) — a name and contacts, not prose", n, maxUserAgentLen))
	}

	// render
	// The render engine (the render spec §4.1 item 2, FR-3): the
	// enumeration and the "engine × remote_url × restart_memory_bytes"
	// binding. moli|lightpanda — external CDP servers: remote_url is
	// required (its own browser process — chromium only), the RSS of a
	// foreign process is unavailable — restart_memory_bytes must be 0
	// (exactly like the memory control not working in the remote mode of
	// the dev stack today). chromium allows both modes (FR-2: its own
	// production headless-shell + the external CDP of the dev stack); the
	// memory threshold at an external chromium does not work but is not
	// forbidden — like today.
	switch c.Render.Engine {
	case "", EngineChromium, EngineMoli, EngineLightpanda: // "" — a config without Default() (chromium)
	default:
		problems = append(problems, fmt.Sprintf("render.engine: expected %s|%s|%s, got %q",
			EngineChromium, EngineMoli, EngineLightpanda, c.Render.Engine))
	}
	if c.Render.Engine == EngineMoli || c.Render.Engine == EngineLightpanda {
		if c.Render.RemoteURL == "" {
			problems = append(problems, fmt.Sprintf(
				"render.engine: %s requires render.remote_url (an external CDP server ws://host:9222; its own browser process — only %s)",
				c.Render.Engine, EngineChromium))
		}
		if t := c.Render.RestartMemoryBytes; t != 0 {
			problems = append(problems, fmt.Sprintf(
				"render.restart_memory_bytes: for engine %s expected 0 (the RSS of a foreign process is unavailable), got %s",
				c.Render.Engine, t))
		}
	}
	if c.Render.RemoteURL != "" {
		u, err := url.Parse(c.Render.RemoteURL)
		if err != nil || u.Host == "" {
			problems = append(problems, fmt.Sprintf("render.remote_url: expected a ws://host:9222-style URL, got %q", c.Render.RemoteURL))
		}
	}
	if !inRange(c.Render.Contexts, 1, 16) {
		problems = append(problems, rangeMsg("render.contexts", c.Render.Contexts, 1, 16)+"; spec recommendation: 2–4")
	}
	if c.Render.Timeout <= 0 {
		problems = append(problems, nonPositiveDur("render.timeout", c.Render.Timeout))
	}
	// The tarpit limit (T-148): a minimum of 1 — zero would mean
	// finalization before the first attempt; the ceiling is against
	// typos. Above redis.max_attempts is meaningless: the record would
	// die in q:dead by the general limit before finalizing without
	// render (the footgun of the 2026-10-23 incident — the task is
	// exactly about not losing tarpits), so exceeding it is a
	// configuration error, not a silent rollback to the old behavior.
	if !inRange(c.Render.TimeoutAttempts, 1, 100) {
		problems = append(problems, rangeMsg("render.timeout_attempts", c.Render.TimeoutAttempts, 1, 100))
	}
	if c.Render.TimeoutAttempts > c.Redis.MaxAttempts {
		problems = append(problems, fmt.Sprintf(
			"render.timeout_attempts: %d > redis.max_attempts %d — a tarpit would end up in q:dead before finalization without render (T-148); raise redis.max_attempts or lower render.timeout_attempts",
			c.Render.TimeoutAttempts, c.Redis.MaxAttempts))
	}
	if !inRange(c.Render.RestartEveryPages, 1, 1000000) {
		problems = append(problems, rangeMsg("render.restart_every_pages", c.Render.RestartEveryPages, 1, 1000000))
	}
	// 0 — the control is disabled (the remote mode and small machines);
	// the working range is from the idle RSS of headless-shell with
	// contexts (below — a crash loop of restarts) to a reasonable budget
	// of one worker (spec 3.2).
	if t := c.Render.RestartMemoryBytes; t != 0 && (t < 256<<20 || t > 16<<30) {
		problems = append(problems, fmt.Sprintf("render.restart_memory_bytes: expected 0 (disabled) or %s..%s, got %s",
			ByteSize(256<<20), ByteSize(16<<30), t))
	}
	// The q:render capacity gates (T-174): 0 — the shared threshold
	// scheduler.queue_high_watermark; a positive value no higher than the
	// stream cap (10⁸ — the validation boundary of redis.maxlen, the same
	// order as content.retention_analyze_watermark). The MAXLEN clamp is
	// done by the wiring (not here — the limit depends on the
	// redis.maxlen of the stream).
	if w := c.Render.QueueHighWatermark; w != 0 && !inRange(w, 1, 100000000) {
		problems = append(problems, fmt.Sprintf("render.queue_high_watermark: expected 0 (follows scheduler.queue_high_watermark) or %d..%d, got %d",
			1, 100000000, w))
	}
	// The late re-publisher selection ceiling (T-274): 0 — the code
	// default (300 s, T-273); the corridor 1m..24h — minutes do not cover
	// a catch-up selection (the loop of doomed repeats of T-271); a day
	// and more makes no sense at a minute tick of the loop.
	if d := c.Render.LateSelectTimeout; d != 0 && (d < Duration(time.Minute) || d > Duration(24*time.Hour)) {
		problems = append(problems, fmt.Sprintf("render.late_select_timeout: expected 0 (code default) or %s..%s, got %s",
			Duration(time.Minute), Duration(24*time.Hour), d))
	}
	// The zone priority of the finish pass (T-184): a syntactically
	// impossible name is caught here (the reference table cannot contain
	// such a name — like the zone weights of T-106); a name may be
	// missing from the reference table — not an error (ingest creates the
	// zone with the first file; the filter matches by name in the
	// selection itself and takes effect with the zone's appearance).
	seenDepr := make(map[string]bool, len(c.Render.RequeueDeprioritizedZones))
	for _, name := range c.Render.RequeueDeprioritizedZones {
		if name == "" || len(name) > 253 || !zoneNameRe.MatchString(name) {
			problems = append(problems, fmt.Sprintf(
				"render.requeue_deprioritized_zones: %q — expected a zone name: a TLD in lowercase punycode, as in the zones reference table (in, uk; IDN — xn--p1ai)", name))
			continue
		}
		if seenDepr[name] {
			problems = append(problems, fmt.Sprintf("render.requeue_deprioritized_zones: duplicate %q", name))
			continue
		}
		seenDepr[name] = true
	}
	// The fallback subsection (the render spec §4.1 item 2): the ranges
	// are checked even with enabled=false — enabling the fallback by
	// editing one flag (FR-14) must not blow up the start with invalid
	// neighbors (the style of monitor.alerts: the thresholds are valid
	// regardless of the delivery enabling). The rate ceiling — like
	// restart_every_pages (a record counter); the streak threshold — like
	// timeout_attempts; the share is a percent (0, 100] with disabling by
	// zero (the style of mem_limit, not disk_free_percent: the control is
	// optional). The form !(p >= 0 && p <= 100), not p < 0 || p > 100:
	// NaN is insensitive to comparisons and would pass through, silently
	// disabling the protective ceiling (the T-233 review; setFloat
	// accepts NaN from ParseFloat).
	if !inRange(c.Render.Fallback.WorkerRate, 1, 1000000) {
		problems = append(problems, rangeMsg("render.fallback.worker_rate", c.Render.Fallback.WorkerRate, 1, 1000000))
	}
	if p := c.Render.Fallback.SharePercent; p != 0 && !(p >= 0 && p <= 100) {
		problems = append(problems, fmt.Sprintf("render.fallback.share_percent: expected 0 (no cap) or (0..100], got %v", p))
	}
	if !inRange(c.Render.Fallback.HealthGateStreak, 1, 100) {
		problems = append(problems, rangeMsg("render.fallback.health_gate_streak", c.Render.Fallback.HealthGateStreak, 1, 100))
	}

	// content
	if c.Content.Dir == "" {
		problems = append(problems, "content.dir: not set (the pack file directory)")
	}
	if c.Content.PackSize < 1<<20 || c.Content.PackSize > 8<<30 {
		problems = append(problems, fmt.Sprintf("content.pack_size: expected %s..%s, got %s (spec recommendation: 512 MB–1 GB)",
			ByteSize(1<<20), ByteSize(8<<30), c.Content.PackSize))
	}
	if c.Content.MaxStoreBytes <= 0 || c.Content.MaxStoreBytes > 16<<20 {
		problems = append(problems, fmt.Sprintf("content.max_store_bytes: expected %s..%s, got %s (spec recommendation: 1–2 MB)",
			ByteSize(1), ByteSize(16<<20), c.Content.MaxStoreBytes))
	}
	// The disk overflow threshold (spec 8): 0 would disable the write
	// pause mandatory for the spec; > 95 would stop writing on a
	// demonstrably live volume.
	if c.Content.DiskFreePercent <= 0 || c.Content.DiskFreePercent > 95 {
		problems = append(problems, fmt.Sprintf("content.disk_free_percent: expected (0, 95], got %v", c.Content.DiskFreePercent))
	}
	// Retention (T-166): 0 — disabled; the corridor 1m..30d: a window
	// below a minute drowns in the consumer latency (the analysis —
	// seconds); above 30 days it is not retention but an archive (for
	// that the operator brings up separate capacity, the decision of
	// 2026-10-23). The minute boundary also keeps the PostgreSQL
	// interval format free of microsecond components of Duration.String()
	// ("µs" in the server interval syntax).
	if r := c.Content.Retention; r != 0 && (r < Duration(time.Minute) || r > Duration(720*time.Hour)) {
		problems = append(problems, fmt.Sprintf("content.retention: expected 0 (disabled) or %s..%s, got %s",
			Duration(time.Minute), Duration(720*time.Hour), r))
	}
	// The pass period: more often than a minute — extra aging scans (the
	// batch is up to 50 k rows anyway); rarer than a day — the window
	// floats by days.
	if c.Content.RetentionInterval < Duration(time.Minute) || c.Content.RetentionInterval > Duration(24*time.Hour) {
		problems = append(problems, fmt.Sprintf("content.retention_interval: expected %s..%s, got %s",
			Duration(time.Minute), Duration(24*time.Hour), c.Content.RetentionInterval))
	}
	// The batch: 1000 — not to drown in round trips at an inflow of ≈3 M
	// rows/day; 100 k — the tasking range ceiling × 2 (the transaction
	// holds the row locks of the batch, no need to hypertrophy it).
	if !inRange(c.Content.RetentionBatch, 1000, 100000) {
		problems = append(problems, rangeMsg("content.retention_batch", c.Content.RetentionBatch, 1000, 100000)+" (production range 10–50k, T-166)")
	}
	// The interlock threshold: 0 — the shared threshold
	// scheduler.queue_high_watermark; a positive value no higher than the
	// stream cap (10⁸ — the validation boundary of redis.maxlen, the same
	// order).
	if w := c.Content.RetentionAnalyzeWatermark; w != 0 && !inRange(w, 1, 100000000) {
		problems = append(problems, fmt.Sprintf("content.retention_analyze_watermark: expected 0 (follows scheduler.queue_high_watermark) or %d..%d, got %d",
			1, 100000000, w))
	}
	// The retention batch selection ceiling (T-274): 0 — the code default
	// (7200 s); the corridor 1m..24h: below a minute — the loop of doomed
	// selections of T-271 (the regular measurement batch of T-201 up to
	// 222.8 s, a catch-up pass — tens of minutes); above a day —
	// meaningless (the pass interval is an hour).
	if d := c.Content.RetentionSelectTimeout; d != 0 && (d < Duration(time.Minute) || d > Duration(24*time.Hour)) {
		problems = append(problems, fmt.Sprintf("content.retention_select_timeout: expected 0 (code default) or %s..%s, got %s",
			Duration(time.Minute), Duration(24*time.Hour), d))
	}

	// analyzer
	if c.Analyzer.RulesDir == "" {
		problems = append(problems, "analyzer.rules_dir: not set (the Laravel YAML rules directory)")
	}
	if c.Analyzer.LaravelMinScore <= 0 || c.Analyzer.LaravelMinScore > 1 {
		problems = append(problems, fmt.Sprintf("analyzer.laravel_min_score: expected (0, 1], got %v", c.Analyzer.LaravelMinScore))
	}

	// subdomains
	switch {
	case len(c.Subdomains) == 0:
		problems = append(problems, "subdomains: the list must not be empty (at least the apex is required — an empty string)")
	case len(c.Subdomains) > maxSubdomains:
		problems = append(problems, fmt.Sprintf("subdomains: at most %d (the dns_mask bit width), got %d", maxSubdomains, len(c.Subdomains)))
	}
	seen := make(map[string]bool, len(c.Subdomains))
	for _, s := range c.Subdomains {
		if s != "" && !subdomainLabelRe.MatchString(s) {
			problems = append(problems, fmt.Sprintf("subdomains: %q — expected a lowercase DNS label without dots (an empty string is the apex)", s))
		}
		if seen[s] {
			problems = append(problems, fmt.Sprintf("subdomains: duplicate %q", s))
		}
		seen[s] = true
	}

	// monitor
	if err := checkHostPort(c.Monitor.Listen); err != nil {
		problems = append(problems, fmt.Sprintf("monitor.listen: expected [host]:port, got %q", c.Monitor.Listen))
	}
	// The reconciliation period (T-161 raised the default 60s→5m, T-199 —
	// 5m→30m per the slow window measurement after 3bffc62): the corridor
	// 30s..1h — the lower bound of the spec 5.9 corridor (more often — an
	// UPDATE aggregate over all of domains); the upper bound limits the
	// stall threshold too (5 × the interval, 2.5 h at the default):
	// updated_at is written by this loop.
	if c.Monitor.ZoneStatsInterval < Duration(30*time.Second) || c.Monitor.ZoneStatsInterval > Duration(time.Hour) {
		problems = append(problems, fmt.Sprintf("monitor.zone_stats_interval: expected %s..%s, got %s",
			Duration(30*time.Second), Duration(time.Hour), c.Monitor.ZoneStatsInterval))
	}
	// The alert loop period (T-206 — the decoupling from the
	// reconciliation): the corridor is the same 30s..1h, following the
	// pattern of zone_stats_interval — more often than 30 s the alert
	// pass (the /metrics probe of ingest, XINFO, systemctl) becomes noise
	// itself; longer than an hour — the detection windows in passes
	// stretch.
	if c.Monitor.AlertsInterval < Duration(30*time.Second) || c.Monitor.AlertsInterval > Duration(time.Hour) {
		problems = append(problems, fmt.Sprintf("monitor.alerts_interval: expected %s..%s, got %s",
			Duration(30*time.Second), Duration(time.Hour), c.Monitor.AlertsInterval))
	}
	// The load report (T-173): 0 — disabled; otherwise the corridor
	// 1m..1h — more often is spam to the operator group, rarer — the
	// window is wider than the starvation episodes.
	if lr := c.Monitor.LoadReportInterval; lr != 0 &&
		(lr < Duration(time.Minute) || lr > Duration(time.Hour)) {
		problems = append(problems, fmt.Sprintf("monitor.load_report_interval: expected 0 (disabled) or %s..%s, got %s",
			Duration(time.Minute), Duration(time.Hour), lr))
	}
	// The slow queries (T-177): a period of 0 — disabled, otherwise the
	// corridor 5s..10m — a pg_stat_activity snapshot is cheap (≤ 50
	// connections); more often — an extra poll, rarer — the episodes
	// shorter than the interval are lost. The threshold 1s..1h: below a
	// second the point batches of the pipeline are noisy; above an hour
	// it is not about a "slow query"; shorter than the loop period is
	// allowed — the loop catches short episodes partially (the full
	// trace is the server log).
	if sq := c.Monitor.SlowQueryInterval; sq != 0 &&
		(sq < Duration(5*time.Second) || sq > Duration(10*time.Minute)) {
		problems = append(problems, fmt.Sprintf("monitor.slow_query_interval: expected 0 (disabled) or %s..%s, got %s",
			Duration(5*time.Second), Duration(10*time.Minute), sq))
	}
	if c.Monitor.SlowQueryInterval != 0 && c.Monitor.SlowQueryMinDuration != 0 &&
		(c.Monitor.SlowQueryMinDuration < Duration(time.Second) || c.Monitor.SlowQueryMinDuration > Duration(time.Hour)) {
		problems = append(problems, fmt.Sprintf("monitor.slow_query_min_duration: expected 0 (default) or %s..%s, got %s",
			Duration(time.Second), Duration(time.Hour), c.Monitor.SlowQueryMinDuration))
	}
	// The render debt (T-175): the pass is cheaper than any reasonable
	// period; the bounds are like zone_stats_interval (the gauge
	// freshness; below 30 s there is nothing to poll, above an hour the
	// gauge lags the 10 min load report summary).
	if c.Monitor.RenderDebtInterval < Duration(30*time.Second) || c.Monitor.RenderDebtInterval > Duration(time.Hour) {
		problems = append(problems, fmt.Sprintf("monitor.render_debt_interval: expected %s..%s, got %s",
			Duration(30*time.Second), Duration(time.Hour), c.Monitor.RenderDebtInterval))
	}
	if c.Monitor.IngestMetricsURL != "" {
		if u, err := url.Parse(c.Monitor.IngestMetricsURL); err != nil ||
			(u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			problems = append(problems, fmt.Sprintf(
				"monitor.ingest_metrics_url: expected an absolute http(s) URL, got %q", c.Monitor.IngestMetricsURL))
		}
	}
	if c.Monitor.RenderMetricsURL != "" {
		if u, err := url.Parse(c.Monitor.RenderMetricsURL); err != nil ||
			(u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			problems = append(problems, fmt.Sprintf(
				"monitor.render_metrics_url: expected an absolute http(s) URL, got %q", c.Monitor.RenderMetricsURL))
		}
	}

	// monitor.alerts (T-162): the thresholds of the new alerts and the
	// Telegram delivery. The escalation must be below the Warn minimum,
	// otherwise it is unreachable; the stability windows — from a minute
	// (the lower bound of 1m cuts off both the sub-second settings and a
	// triggering within one pass; a window shorter than the alert loop
	// period alerts_interval degenerates into "two neighboring passes" —
	// psi_cpu, see DefaultPSIWindow), no more than the operator shift
	// change (6h) — nobody waits for an alert silent longer than that.
	if c.Monitor.Alerts.DiskFreeCritBytes >= c.Monitor.Alerts.DiskFreeMinBytes {
		problems = append(problems, fmt.Sprintf(
			"monitor.alerts.disk_free_crit_bytes: the escalation (%s) must be below the disk_free_min_bytes minimum (%s)",
			c.Monitor.Alerts.DiskFreeCritBytes, c.Monitor.Alerts.DiskFreeMinBytes))
	}
	if c.Monitor.Alerts.QueueFreeze < Duration(time.Minute) || c.Monitor.Alerts.QueueFreeze > Duration(6*time.Hour) {
		problems = append(problems, fmt.Sprintf("monitor.alerts.queue_freeze_window: expected %s..%s, got %s",
			Duration(time.Minute), Duration(6*time.Hour), c.Monitor.Alerts.QueueFreeze))
	}
	if c.Monitor.Alerts.PSICPUPercent <= 0 || c.Monitor.Alerts.PSICPUPercent > 100 {
		problems = append(problems, fmt.Sprintf(
			"monitor.alerts.psi_cpu_percent: expected 0–100 (a %% share), got %v",
			c.Monitor.Alerts.PSICPUPercent))
	}
	if c.Monitor.Alerts.PSIWindow < Duration(time.Minute) || c.Monitor.Alerts.PSIWindow > Duration(6*time.Hour) {
		problems = append(problems, fmt.Sprintf("monitor.alerts.psi_cpu_window: expected %s..%s, got %s",
			Duration(time.Minute), Duration(6*time.Hour), c.Monitor.Alerts.PSIWindow))
	}
	if c.Monitor.Alerts.MemHighPasses < 1 || c.Monitor.Alerts.MemHighPasses > 10 {
		problems = append(problems, fmt.Sprintf(
			"monitor.alerts.mem_high_passes: expected 1..10 passes, got %d",
			c.Monitor.Alerts.MemHighPasses))
	}
	// The render debt (T-175): the threshold in domains is 1..100 M (the
	// upper bound is above the corpus of 41.8 M: a threshold above the
	// debt never fires — protection against a typo in the number of
	// zeros); the growth series — like mem_high_passes.
	if dd := c.Monitor.Alerts.RenderDebtDomains; dd < 1 || dd > 100000000 {
		problems = append(problems, fmt.Sprintf(
			"monitor.alerts.render_debt_domains: expected 1..100000000 domains, got %d", dd))
	}
	if c.Monitor.Alerts.RenderDebtPasses < 1 || c.Monitor.Alerts.RenderDebtPasses > 10 {
		problems = append(problems, fmt.Sprintf(
			"monitor.alerts.render_debt_passes: expected 1..10 passes, got %d",
			c.Monitor.Alerts.RenderDebtPasses))
	}
	if c.Monitor.Alerts.Telegram && c.Monitor.Alerts.TelegramScript == "" {
		problems = append(problems,
			"monitor.alerts.telegram_script: the telegram-notify.sh path is required when delivery is enabled (monitor.alerts.telegram)")
	}

	// web (stage 13, T-121)
	// listen — [host]:port (TCP) or an absolute unix socket path (the
	// deployment option behind nginx, T-124): Run distinguishes the forms
	// by "/".
	if err := checkHostPort(c.Web.Listen); err != nil {
		if !strings.HasPrefix(c.Web.Listen, "/") {
			problems = append(problems, fmt.Sprintf("web.listen: expected [host]:port or an absolute unix socket path, got %q", c.Web.Listen))
		} else if len(c.Web.Listen) > 100 {
			// The ceiling of sockaddr_un.sun_path is 108 bytes: a long
			// path would fail at Listen already at startup; validation
			// gives a configuration error.
			problems = append(problems, fmt.Sprintf("web.listen: the unix socket path is longer than 100 bytes, got %q", c.Web.Listen))
		}
	}
	// A minimum of 1 — zero would mean an unlimited form; the ceiling is
	// against absurdity, not operations.
	if !inRange(c.Web.RateLimit, 1, 1000) {
		problems = append(problems, rangeMsg("web.rate_limit", c.Web.RateLimit, 1, 1000)+" (form checks per minute per IP)")
	}
	// The semaphore of the concurrent live checks (T-220): a minimum of
	// 1 — the form works; the ceiling of 64 — far beyond the resource
	// budget of the unit (MemoryMax 256M); the practical values are a few.
	if !inRange(c.Web.LiveConcurrency, 1, 64) {
		problems = append(problems, rangeMsg("web.live_concurrency", c.Web.LiveConcurrency, 1, 64)+" (concurrent live checks)")
	}
	if c.Web.Timeout <= 0 || c.Web.Timeout > Duration(10*time.Minute) {
		problems = append(problems, fmt.Sprintf("web.timeout: expected %s..%s, got %s",
			Duration(time.Second), Duration(10*time.Minute), c.Web.Timeout))
	}
	// The canonical host of the public address (T-227): host[:port]
	// without a scheme, path and userinfo — the value goes into <loc> of
	// sitemap.xml entirely (the port is optional: a host without a port
	// is a normal address behind a proxy on 80/443; an IPv6 literal is in
	// square brackets, as in URLs). Control characters would break the
	// URL/XML; 253 octets — the DNS name limit (without a port). An empty
	// field — the sitemap fallback to the request Host.
	if h := c.Web.PublicHost; h != "" {
		hostPart := h
		if i := strings.LastIndex(h, ":"); i >= 0 {
			hostPart = h[:i]
			if n, err := strconv.Atoi(h[i+1:]); err != nil || n < 1 || n > 65535 {
				problems = append(problems, fmt.Sprintf(
					"web.public_host: port not in 1..65535 — expected host[:port] (e.g. crawler.example.com:8443), got %q", h))
			}
		}
		// A ":" without a port ("https:"), an empty host, userinfo
		// ("user@host"), a bare IPv6 without brackets ("::1"),
		// schemes/paths/fragments and control characters — not a host.
		if hostPart == "" || len(hostPart) > 253 || strings.ContainsAny(hostPart, "/?#@") ||
			strings.ContainsFunc(h, unicode.IsControl) ||
			(strings.Contains(hostPart, ":") &&
				(!strings.HasPrefix(hostPart, "[") || !strings.HasSuffix(hostPart, "]"))) {
			problems = append(problems, fmt.Sprintf(
				"web.public_host: expected host[:port] without scheme, path or userinfo (e.g. crawler.example.com, IPv6 in brackets), got %q", h))
		}
	}

	// dashboard (the dashboard spec §8.1/§8.3, T-127). Username/PasswordHash
	// are not checked by the shared Validate: the credentials being
	// required is a startup check of cmd/dashboard (the dashboard spec
	// §7.7), otherwise the requirement would hit all the services of the
	// project; the password_hash format itself is parsed by
	// internal/dashboard/auth at dashboard start.
	if err := checkHostPort(c.Dashboard.Listen); err != nil {
		problems = append(problems, fmt.Sprintf("dashboard.listen: expected [host]:port, got %q", c.Dashboard.Listen))
	}
	if c.Dashboard.Refresh < Duration(time.Second) || c.Dashboard.Refresh > Duration(5*time.Minute) {
		problems = append(problems, fmt.Sprintf("dashboard.refresh: expected %s..%s, got %s",
			Duration(time.Second), Duration(5*time.Minute), c.Dashboard.Refresh))
	}
	// The TTL of the heavy aggregate cache (T-161): the tasking corridor
	// 1–5 min — below a minute the cache does not help at the default
	// refresh of 5 s; above 5 minutes the summary loses its operational
	// value.
	if c.Dashboard.StatusCacheTTL < Duration(time.Minute) || c.Dashboard.StatusCacheTTL > Duration(5*time.Minute) {
		problems = append(problems, fmt.Sprintf("dashboard.status_cache_ttl: expected %s..%s, got %s",
			Duration(time.Minute), Duration(5*time.Minute), c.Dashboard.StatusCacheTTL))
	}
	for _, name := range sortedStringKeys(c.Dashboard.Probes) {
		if name == "" {
			problems = append(problems, "dashboard.probes: the probe name must not be empty")
			continue
		}
		u, err := url.Parse(c.Dashboard.Probes[name])
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			problems = append(problems, fmt.Sprintf(
				"dashboard.probes: %s=%q — expected an absolute http(s) URL (the service metrics base URL, dashboard spec §8.3)", name, c.Dashboard.Probes[name]))
		}
	}
	// The engines panel addresses (T-246) — the same absolute http(s)
	// address criterion as the probes: the /metrics exposure of the
	// render workers.
	for _, raw := range c.Dashboard.RenderMetrics {
		u, err := url.Parse(raw)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			problems = append(problems, fmt.Sprintf(
				"dashboard.render_metrics_urls: %q — expected an absolute http(s) URL (the render worker /metrics exposition)", raw))
		}
	}

	// log (a framework extension, T-012)
	switch c.Log.Level {
	case "", "debug", "info", "warn", "error": // "" — a config without Default()
	default:
		problems = append(problems, fmt.Sprintf("log.level: expected debug|info|warn|error, got %q", c.Log.Level))
	}

	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("invalid values (%d): %s", len(problems), strings.Join(problems, "; "))
}

// checkHostPort checks the host:port shape (the resolver addresses, redis,
// the listening address): an empty host is allowed (":9100" — all
// interfaces).
func checkHostPort(s string) error {
	_, port, err := net.SplitHostPort(s)
	if err != nil {
		return err
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("port %q", port)
	}
	return nil
}

func inRange(v, lo, hi int) bool { return v >= lo && v <= hi }

// sortedKeys — the map keys in ascending order (a deterministic order of
// the validation messages; Go maps are unordered).
func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// sortedStringKeys — the same for maps of string values (the dashboard
// probes).
func sortedStringKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func rangeMsg(field string, v, lo, hi int) string {
	return fmt.Sprintf("%s: expected %d..%d, got %d", field, lo, hi, v)
}

func nonPositiveDur(field string, d Duration) string {
	return fmt.Sprintf("%s: expected a positive duration, got %s", field, d)
}

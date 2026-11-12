// Snapshot collector (dashboard spec §4.3–4.4, T-129): a background
// loop with a dashboard.refresh period — the first collection right at
// startup (a fresh snapshot after idle/restart, monitor.ZoneStatsLoop
// style), then on a ticker; the collected snapshot atomically replaces
// the previous one (atomic.Pointer — handlers read without locks).
//
// Sources (dashboard spec §4.3, all read-only):
//
//   - PostgreSQL — internal/status sections (zones/throughput — the
//     same SQL as crawlerctl status, T-126; statuses/Laravel/ingest —
//     §5.5–5.7); the heavy domains passes (zones/throughput/statuses)
//     go through the status.Cache TTL cache (T-161): within
//     dashboard.status_cache_ttl the refresh passes do not pay the
//     full aggregate, light sections stay live;
//   - Redis — the depth of five streams via the same narrow Len API
//     (BackpressureGate, dashboard spec §4.3.2): the client is
//     assembled by cmd/dashboard from the redis config — like
//     cmd/monitor; the gate threshold takes no part (Len only,
//     crawlerctl status style);
//   - HTTP exposures /metrics of render workers (T-246, renders.go) —
//     the engines panel: per-engine rate/success (the T-235 metric)
//     and the published/consumed fallback; the q:render-fallback
//     depth — the same Len over Redis;
//   - /livez probes of pipeline services from dashboard.probes — in
//     parallel (goroutines + WaitGroup; errgroup is not used —
//     golang.org/x/sync in go.mod is indirect, dashboard spec §3.2.1
//     introduces no new dependencies), a 2 s timeout per probe (§5.8).
//
// A section error interrupts only that section (the snapshot is
// published degraded, §5.9); an error of the whole pass comes only
// from ctx cancellation (service shutdown). "Successful collection"
// (dashboard spec §5.9: until the first one /api/status answers 503) —
// the first pass with a live summary skeleton (zones/totals §5.1): the
// hierarchy is the same as crawlerctl status — an unavailable Redis
// does not hide the DB summary (queues "n/a", the §12.3 acceptance
// also works for a restart with Redis down), a down DB holds 503 and
// the result=error metric until recovery — a snapshot without the
// zone skeleton is meaningless. The readiness latch fires once —
// later degraded snapshots are still served (§4.4 atomic replacement
// + §5.9 does not take down the response).
package dashboard

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/status"
)

// DefaultRefreshInterval — the collection period for a zero Interval
// (mirrors the dashboard.refresh default, dashboard spec §8.1).
const DefaultRefreshInterval = 5 * time.Second

// DefaultStatusCacheTTL — the TTL of the heavy-aggregate cache for a
// zero StatusCacheTTL (mirrors the dashboard.status_cache_ttl default,
// T-161): the lower bound of the 1–5 min enqueue corridor — with the
// default refresh of 5 s one full domains pass per minute instead of
// twelve.
const DefaultStatusCacheTTL = time.Minute

// probeTimeout — the budget of one /livez probe (dashboard spec §5.8: 2 s).
const probeTimeout = 2 * time.Second

// queueFetchTimeout — the budget of the queues section (§5.4). A
// stopped Redis container does not answer with RST (the network
// endpoint persists behind the stopped container), so a dial waits for
// the default go-redis timeouts (DialTimeout 5 s × retries) and hangs
// for tens of seconds, blocking the collection loop entirely — the
// "n/a" degradation (§5.9) does not arrive within a reasonable ~2
// refresh cycles. The budget cuts off a hung stream poll: the section
// degrades, the rest are collected. A variable (not a const) so that
// unit tests do not wait real seconds.
var queueFetchTimeout = 2 * time.Second

// probeBodyLimit — how much of a probe body to read before closing
// (connection reuse; the framework /livez — the "ok" bytes).
const probeBodyLimit = 4 << 10

// Collection result labels (§10).
const (
	snapshotResultOK    = "ok"
	snapshotResultError = "error"
)

// CollectorOptions — collector parameters; zero values are replaced
// with defaults (monitor.ZoneStatsOptions style).
type CollectorOptions struct {
	// Interval — the cycle period (DefaultRefreshInterval); the
	// working value — dashboard.refresh from the config.
	Interval time.Duration
	// StatusCacheTTL — the TTL of the status heavy-aggregate cache
	// (DefaultStatusCacheTTL); the working value —
	// dashboard.status_cache_ttl from the config. 0 within the TTL —
	// the zones/throughput/statuses sections are collected from the
	// cache without hitting the DB (T-161, the §12 measurement: full
	// domains passes every refresh are a large share of postgres CPU);
	// after expiry — one selection per section.
	StatusCacheTTL time.Duration
	// Probes — service name → base metrics URL (dashboard.probes): the
	// probe goes to base + "/livez" (the internal/service framework,
	// dashboard spec §4.3.3). nil/empty — the health section is "n/a"
	// (Health = nil, not an error).
	Probes map[string]string
	// QueueWatermark — the scheduler.queue_high_watermark backpressure
	// threshold: the base of the effective queue highlight thresholds
	// (§5.4); ≤ 0 — the gate is off (all thresholds 0, no highlight).
	// The effective stream threshold is
	// queue.StreamWatermark(Watermark, MaxLen, stream): no higher than
	// half the MAXLEN limit of the stream — the same single
	// computation as in the publication gate (T-026) and the monitor queue
	// overflow alert (T-080).
	QueueWatermark int64
	// QueueMaxLen — the stream length limit MAXLEN (mirrors
	// redis.maxlen; 0 — per-stream policy limits T-025): needed for the
	// effective stream threshold (see QueueWatermark).
	QueueMaxLen int64
	// RenderMetrics — /metrics addresses of render workers for the
	// engines panel (T-246; the working value —
	// dashboard.render_metrics_urls): nil/empty — the render section is
	// "n/a" (not an error, Probes style).
	RenderMetrics []string
	// Log — the loop logger; nil — records suppressed
	// (slog.DiscardHandler).
	Log *slog.Logger
	// Metrics — the service registry (§10 metrics); nil — no metrics
	// (a test seam; the monitor.NewNearGauge precedent).
	Metrics *metrics.Registry
	// Now — the collection moment clock (built_at); nil — time.Now.
	Now func() time.Time
	// HTTPClient — the probe client; nil — own one with the
	// probeTimeout timeout. A test seam: a short timeout instead of
	// waiting two seconds.
	HTTPClient *http.Client
}

func (o CollectorOptions) withDefaults() CollectorOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultRefreshInterval
	}
	if o.StatusCacheTTL <= 0 {
		o.StatusCacheTTL = DefaultStatusCacheTTL
	}
	if o.Log == nil {
		o.Log = slog.New(slog.DiscardHandler)
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o
}

// Collector — the background snapshot collector: collects sections
// independently, publishes atomically. Safe for concurrent use: Loop
// writes, Current is read by handlers.
type Collector struct {
	pool    db.DB
	lens    status.StreamLener
	opt     CollectorOptions
	client  *http.Client
	metrics *collectorMetrics
	cache   *status.Cache
	// rates — the engines-panel rate memory between passes (worker
	// exposure cumulatives and the moment of the previous scrape,
	// T-246); written only by Loop/CollectOnce — a single owner, no
	// lock.
	rates renderRateState

	snap  atomic.Pointer[Snapshot]
	ready atomic.Bool
}

// NewCollector assembles the collector: pool — the DB pool (required),
// lens — the Redis stream depths (nil — the queues section is always
// degraded: FetchQueueLens will honestly fail), cfg — the dashboard
// section (period/probes).
func NewCollector(pool db.DB, lens status.StreamLener, cfg config.Dashboard, opts CollectorOptions) (*Collector, error) {
	if pool == nil {
		return nil, errors.New("dashboard: the snapshot collector: the DB pool is not set")
	}
	if cfg.Refresh.D() > 0 && opts.Interval == 0 {
		opts.Interval = cfg.Refresh.D()
	}
	if cfg.StatusCacheTTL.D() > 0 && opts.StatusCacheTTL == 0 {
		opts.StatusCacheTTL = cfg.StatusCacheTTL.D()
	}
	if opts.Probes == nil {
		opts.Probes = cfg.Probes
	}
	if opts.RenderMetrics == nil {
		opts.RenderMetrics = cfg.RenderMetrics
	}
	o := opts.withDefaults()
	client := o.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: probeTimeout,
			// The probe does not follow redirects: /livez answers 200
			// directly, and someone else's Location must not steer the
			// outgoing connection to an unconfigured host (dashboard
			// spec §9.3 — configured probes only); a redirect stays a
			// != 200 response → down with a reason.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	return &Collector{
		pool:    pool,
		lens:    lens,
		opt:     o,
		client:  client,
		metrics: newCollectorMetrics(o.Metrics),
		// The heavy-aggregate cache (T-161): one instance per collector
		// process — the same one for all Loop passes.
		cache: status.NewCache(status.CacheOptions{TTL: o.StatusCacheTTL}),
	}, nil
}

// Current — the latest snapshot for /api/status; nil until at least one
// successful collection (§5.9 — the summary skeleton has not been
// collected yet).
func (c *Collector) Current() *Snapshot {
	if !c.ready.Load() {
		return nil
	}
	return c.snap.Load()
}

// CollectOnce — one collection pass: sections independently (§5.9), a
// snapshot ready for serving. A ctx cancellation in the middle of a
// pass leaves the collected sections with context errors in the errors
// map — Loop does not publish such a snapshot.
func (c *Collector) CollectOnce(ctx context.Context) *Snapshot {
	now := c.opt.Now()
	snap := &Snapshot{
		Errors:          map[string]string{},
		BuiltAt:         rfc3339(now),
		QueueWatermarks: c.queueWatermarks(), // §5.4: highlight thresholds — data, not a section
	}

	// Zones (§5.1/§5.3) → zones + totals; the input of rates
	// (pending+queued). SummaryJSON is embedded in Snapshot — fields
	// are assigned directly. Heavy sections (T-161) go through the
	// status.Cache TTL cache: within the TTL window the refresh passes
	// reuse the last DB answer — a cache error is the selection error
	// (not cached, the section degradation stays visible), a cancelled
	// ctx behaves as without the cache.
	var tot status.Totals
	zonesOK := false
	zones, err := c.cache.ZoneStats(ctx, c.pool)
	if err != nil {
		snap.Errors["zones"] = err.Error()
	} else {
		zonesOK = true
		tot = status.ComputeTotals(zones)
		snap.Zones = status.ZonesToJSON(zones)
		t := status.TotalsToJSON(tot)
		snap.Totals = &t
	}

	// Throughput (§5.2).
	var thr status.Throughput
	thrOK := false
	thr, err = c.cache.Throughput(ctx, c.pool)
	if err != nil {
		snap.Errors["throughput"] = err.Error()
	} else {
		thrOK = true
		t := status.ThroughputToJSON(thr)
		snap.Throughput = &t
	}

	// Rates/ETA (§5.1–5.2): derivatives of both sections — the
	// degradation of either leaves rates null.
	if zonesOK && thrOK {
		r := status.RatesToJSON(status.ComputeRates(thr, tot.Pending, tot.Queued))
		snap.Rates = &r
	}

	// Queues (§5.4): a nil map → null in JSON. A separate
	// queueFetchTimeout budget — see the comment at the variable: a
	// hung Redis poll must not block the collection loop and the other
	// sections.
	qctx, cancel := context.WithTimeout(ctx, queueFetchTimeout)
	lens, err := status.FetchQueueLens(qctx, c.lens)
	cancel()
	if err != nil {
		snap.Errors["queues"] = err.Error()
	} else {
		snap.Queues = lens
	}

	// Domain statuses (§5.5) — also a domains pass, the cache (T-161).
	if snap.Statuses, err = c.cache.DomainStatuses(ctx, c.pool); err != nil {
		snap.Statuses = nil
		snap.Errors["statuses"] = err.Error()
	}

	// Laravel finds (§5.6).
	if laravel, err := status.FetchLaravelStats(ctx, c.pool); err != nil {
		snap.Errors["laravel"] = err.Error()
	} else {
		snap.Laravel = &LaravelJSON{
			Total:   laravel.Total,
			Last24h: laravel.Last24h,
			Recent:  make([]LaravelItemJSON, 0, len(laravel.Recent)),
		}
		for _, it := range laravel.Recent {
			snap.Laravel.Recent = append(snap.Laravel.Recent, LaravelItemJSON{
				Domain: it.Domain, Zone: it.Zone, Score: it.Score,
				VerMin: it.VerMin, VerMax: it.VerMax, UpdatedAt: rfc3339(it.UpdatedAt),
			})
		}
	}

	// Ingest log (§5.7).
	if reports, err := status.FetchIngestReports(ctx, c.pool); err != nil {
		snap.Errors["ingest"] = err.Error()
	} else {
		snap.Ingest = make([]IngestFileJSON, 0, len(reports))
		for _, r := range reports {
			snap.Ingest = append(snap.Ingest, IngestFileJSON{
				Path: r.Path, Action: r.Action,
				LinesTotal: r.LinesTotal, Added: r.Added, Existed: r.Existed,
				Deleted: r.Deleted, Rejected: r.Rejected,
				ProcessedAt: rfc3339Zero(r.ProcessedAt), // NULL processed_at — empty
			})
		}
	}

	// Render engines panel and fallback (render spec §4.6 item 4,
	// T-246): /metrics exposures of render workers + the
	// q:render-fallback depth. Addresses not set — Render nil ("n/a",
	// not an error, Health style); a collection error degrades the
	// section (the reason is in errors), the other sections live.
	if r, rerr := c.collectRender(ctx); rerr != nil {
		snap.Errors["render"] = rerr.Error()
	} else {
		snap.Render = r
	}

	// Service health (§5.8): probes in parallel; an empty probe list —
	// the section is "n/a" (null), not an error.
	snap.Health = c.probeAll(ctx)
	return snap
}

// queueWatermarks — the effective backpressure thresholds of the
// working streams for queue highlighting (§5.4): queue.StreamWatermark
// per stream except q:dead (not limited by the gate —
// backpressure.go). The same single computation as in the publication gate
// (T-026) and the monitor queue overflow alert (T-080, T-144): a
// stream threshold is no higher than half its MAXLEN limit — after
// T-141 these are different numbers for different streams (q:dns —
// 125 chunks, single items — 75,000 at defaults), so the sum of
// depths against a single threshold no longer makes sense. Recomputed
// on every pass — the snapshot is honest to the current option values.
// Watermark ≤ 0 — all thresholds 0 (the gate is off).
func (c *Collector) queueWatermarks() map[string]int64 {
	wm := make(map[string]int64, len(status.Streams))
	for _, s := range status.Streams {
		if s == queue.StreamDead {
			continue
		}
		wm[s] = queue.StreamWatermark(c.opt.QueueWatermark, c.opt.QueueMaxLen, s)
	}
	return wm
}

// probeAll — probes of all services in parallel (§5.8), the response
// sorted by probe name (deterministic JSON). A ctx cancellation
// interrupts the probes themselves too (the request context): the
// collected down records go away with the cancelled pass — it is not
// published.
func (c *Collector) probeAll(ctx context.Context) []ProbeJSON {
	if len(c.opt.Probes) == 0 {
		return nil
	}
	names := make([]string, 0, len(c.opt.Probes))
	for name := range c.opt.Probes {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]ProbeJSON, len(names))
	var wg sync.WaitGroup
	for i, name := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out[i] = c.probeOne(ctx, name, c.opt.Probes[name])
		}()
	}
	wg.Wait()
	return out
}

// probeOne — a single probe: GET base+"/livez" with the client timeout
// (probeTimeout). up — 200 OK; unavailability, timeout and non-200 —
// down with a reason (§5.8 "unavailable or timed out"). The URL is
// assembled with JoinPath: correct both for a base with a trailing
// slash and for a base with a path/query (config validation allows
// that).
func (c *Collector) probeOne(ctx context.Context, name, base string) ProbeJSON {
	u, err := url.Parse(base)
	if err != nil {
		return ProbeJSON{Name: name, Error: err.Error()}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.JoinPath("livez").String(), nil)
	if err != nil {
		return ProbeJSON{Name: name, Error: err.Error()}
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return ProbeJSON{Name: name, Error: err.Error()}
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, probeBodyLimit))
	if resp.StatusCode != http.StatusOK {
		return ProbeJSON{Name: name, Error: fmt.Sprintf("HTTP %d", resp.StatusCode)}
	}
	return ProbeJSON{Name: name, Up: true}
}

// Loop — the background collection cycle (dashboard spec §4.4): the
// first collection immediately, then with the Interval period; the
// snapshot is replaced atomically. Section errors — Warn with a reason
// map and the result=error metric, the loop lives (the OnRun
// contract); a ctx cancellation — a clean nil (graceful shutdown). A
// collection longer than the period simply shifts the next tick (the
// ticker does not accumulate misses).
func (c *Collector) Loop(ctx context.Context) error {
	c.opt.Log.Info("the dashboard snapshot build loop",
		"interval", c.opt.Interval.String(), "probes", len(c.opt.Probes), "render_metrics", len(c.opt.RenderMetrics))

	pass := func() {
		started := time.Now()
		snap := c.CollectOnce(ctx)
		if ctx.Err() != nil {
			return // cancellation mid-pass — part of shutdown, not a snapshot
		}
		result := snapshotResultOK
		if len(snap.Errors) > 0 {
			result = snapshotResultError
		}
		// Successful collection — the summary skeleton is live (Totals
		// exists ⇔ the zones section collected): queues/probes may keep
		// degrading — their "n/a" is served (§5.9), not held at 503.
		if snap.Totals != nil {
			c.ready.Store(true)
		}
		c.snap.Store(snap)
		c.metrics.observe(result, time.Since(started).Seconds())
		if result == snapshotResultError {
			c.opt.Log.Warn("the dashboard snapshot built with section errors", "errors", snap.Errors)
		} else {
			c.opt.Log.Debug("the dashboard snapshot built", "built_at", snap.BuiltAt)
		}
	}

	pass()
	ticker := time.NewTicker(c.opt.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pass()
		}
	}
}

// --- own-service metrics (dashboard spec §10) ----------------------------------

// collectorMetrics — crawler_dashboard_snapshots_total{result} and
// crawler_dashboard_snapshot_seconds; a nil receiver — no-op (a
// collector without a registry, monitor.NewNearGauge style).
type collectorMetrics struct {
	snapshots *prometheus.CounterVec
	seconds   prometheus.ObserverVec
}

// newCollectorMetrics — the instruments in the reg registry
// (nil — nil). The histogram buckets — exponent from 50 ms to ~3
// minutes: a collection is full aggregate passes (status
// §"aggregates and windows"), seconds on the production corpus — the
// default prometheus buckets (a 10 s ceiling) are too small.
func newCollectorMetrics(reg *metrics.Registry) *collectorMetrics {
	if reg == nil {
		return nil
	}
	return &collectorMetrics{
		snapshots: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_dashboard_snapshots_total",
			Help: "Dashboard snapshot builds by outcome: ok — all sections are alive, error — some degraded (the snapshot is published, dashboard spec §5.9).",
		}, "result"),
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_dashboard_snapshot_seconds",
			Help:    "The dashboard snapshot build duration (a single pass over all sections, dashboard spec §4.4).",
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 13),
		}),
	}
}

// observe — record a pass; nil — no-op.
func (m *collectorMetrics) observe(result string, seconds float64) {
	if m == nil {
		return
	}
	m.snapshots.WithLabelValues(result).Inc()
	m.seconds.WithLabelValues().Observe(seconds)
}

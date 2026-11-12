// A TTL cache of the heavy summary selections (T-161, the measurement of
// §12 docs/load-measurements.md): the zones with the live check_state
// aggregate, the throughput and the status distribution — full passes
// over the large domains table (there are no indexes by the query
// filters, T-104), on a corpus of 47M rows — tens of seconds of postgres
// CPU. Ad hoc consumers (crawlerctl status — diagnostics on demand) do
// not need the cache: fresh data matters more, the process is one-off —
// there would be no hits. For the constant consumer — the background
// loop of the dashboard snapshot assembly (dashboard.refresh, seconds) —
// the cache returns the last answer until the TTL expires and goes to
// the DB no more than once per period: the frequency of the full passes
// stops depending on the frequency of the operator's screen refresh.
//
// The cache lives in the shared status package (the T-161 assignment):
// any summary consumer gets the same mechanism; the instance is one per
// process (the project services are single-process; the system does not
// introduce an inter-process cache). The landing web does not read
// internal/status (its data is the caching zones counters, cheap by
// construction, T-122) — today only the dashboard caches. The light
// selections (the recent finds by the laravel_domains_updated_at_idx
// index, the ingest_files log) are not cached: those are not passes over
// large tables, and the freshness of the "recent finds" is the part of
// the snapshot visible to the operator.
//
// Selection errors are not cached: the next call will repeat the DB call
// (the section degradation stays visible — the errors map of the
// snapshot, dashboard spec §5.9 — instead of freezing at "empty").
// The TTL is one for all sections — set by the assignment (1–5 min) and
// configurable by a field of the consumer configuration
// (dashboard.status_cache_ttl).
package status

import (
	"context"
	"sync"
	"time"

	"crawler/internal/db"
)

// DefaultCacheTTL — the default entry lifetime: 60 s, the lower bound of
// the T-161 assignment corridor (1–5 min). With the default
// dashboard.refresh of 5 s — one full pass over domains per minute
// instead of twelve; the five-minute corridor ceiling is left to the
// operator for really cramped machines (the summary freshness degrades
// to the bar the assignment found acceptable).
const DefaultCacheTTL = time.Minute

// Cache — a TTL cache of the heavy summary aggregates: the zones (with
// the live check_state aggregate), the throughput and the status
// distribution. Thread-safe; the selection runs under a shared mutex
// (the single periodic consumer — the dashboard background loop — is
// single-threaded, selection duplicates do not accumulate under
// concurrent calls, and the serialization is simpler than a
// single-fetch mechanic per key).
type Cache struct {
	opt CacheOptions

	mu       sync.Mutex
	zones    cacheEntry[[]ZoneStats]
	thr      cacheEntry[Throughput]
	statuses cacheEntry[map[string]int64]
}

// cacheEntry — one cached section: the value and the moment until which
// it is valid.
type cacheEntry[T any] struct {
	value     T
	valid     bool
	expiresAt time.Time
}

// CacheOptions — the cache parameters; the zero values are replaced with
// the defaults (the monitor.ZoneStatsOptions style).
type CacheOptions struct {
	// TTL — the entry lifetime (DefaultCacheTTL).
	TTL time.Duration
	// Now — the clock of the record moment and of the TTL check;
	// nil — time.Now. The tests substitute fixed clocks (the scheduler
	// style, T-030).
	Now func() time.Time
}

func (o CacheOptions) withDefaults() CacheOptions {
	if o.TTL <= 0 {
		o.TTL = DefaultCacheTTL
	}
	return o
}

// NewCache assembles the cache (zero options — the defaults).
func NewCache(opts CacheOptions) *Cache {
	o := opts.withDefaults()
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Cache{opt: o}
}

// cached — the common record mechanic: a live value is returned without
// a selection; an expired/absent one — a selection and a record until
// now+TTL. A selection error is not cached. A copy of the value is
// returned — a mutation of the call result does not spoil the cache for
// the next calls.
func cached[T any](c *Cache, e *cacheEntry[T], fetch func() (T, error)) (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.opt.Now()
	if e.valid && now.Before(e.expiresAt) {
		return cloneOf(e.value), nil
	}
	v, err := fetch()
	if err != nil {
		var zero T
		return zero, err
	}
	e.value, e.valid, e.expiresAt = v, true, now.Add(c.opt.TTL)
	return cloneOf(e.value), nil
}

// cloneOf — a defensive copy for handing out (slices and maps are
// reference types; value types like Throughput are copied by assignment).
func cloneOf[T any](v T) T {
	switch t := any(v).(type) {
	case []ZoneStats:
		return any(append([]ZoneStats(nil), t...)).(T)
	case map[string]int64:
		m := make(map[string]int64, len(t))
		for k, val := range t {
			m[k] = val
		}
		return any(m).(T)
	default:
		return v
	}
}

// ZoneStats — the zone summary (FetchZoneStats) from the cache: a live
// entry — no DB call; an expired one — a selection and a rewrite.
func (c *Cache) ZoneStats(ctx context.Context, pool db.DB) ([]ZoneStats, error) {
	return cached(c, &c.zones, func() ([]ZoneStats, error) {
		return FetchZoneStats(ctx, pool)
	})
}

// Throughput — the throughput measurements (FetchThroughput) from the
// cache.
func (c *Cache) Throughput(ctx context.Context, pool db.DB) (Throughput, error) {
	return cached(c, &c.thr, func() (Throughput, error) {
		return FetchThroughput(ctx, pool)
	})
}

// DomainStatuses — the domain status distribution (FetchDomainStatuses)
// from the cache.
func (c *Cache) DomainStatuses(ctx context.Context, pool db.DB) (map[string]int64, error) {
	return cached(c, &c.statuses, func() (map[string]int64, error) {
		return FetchDomainStatuses(ctx, pool)
	})
}

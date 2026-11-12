package httpfetch

import (
	"context"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"
)

// Resolver resolves the names of hosts that have no DNS stage addresses
// (spec 5.5 item 2: "for redirects to other hosts — resolve via the same
// resolver with the cache"). "The same resolver" is the DNS stage resolver
// (internal/dnsclient over Unbound): the Exchange(A/AAAA) → addresses
// adapter is assembled at worker wiring time (T-053), where the dns.ipv6
// flag and the fallback-resolver policy are known; the transport depends
// on the interface, not on the DNS client.
//
// IP literals never get here: the transport handles them itself (lookup).
type Resolver interface {
	// LookupNetIP returns the host's IP addresses (A/AAAA in the resolver
	// response order). The consumer does not modify the returned slice.
	LookupNetIP(ctx context.Context, host string) ([]netip.Addr, error)
}

// ResolverFunc is a function adapter for Resolver (like http.HandlerFunc):
// unit-test stubs and the stage's resolver adapters.
type ResolverFunc func(ctx context.Context, host string) ([]netip.Addr, error)

// LookupNetIP implements Resolver.
func (fn ResolverFunc) LookupNetIP(ctx context.Context, host string) ([]netip.Addr, error) {
	return fn(ctx, host)
}

// SystemResolver is the system resolver (net.DefaultResolver): the
// transport default when the stage resolver is not wired in (local
// development, unit tests). In the production worker it is replaced by the
// dnsclient adapter.
var SystemResolver Resolver = ResolverFunc(func(ctx context.Context, host string) ([]netip.Addr, error) {
	return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
})

// The default cache parameters.
const (
	// DefaultCacheTTL is the entry lifetime. Never above the typical
	// minimum TTL of public zones (60 s): a redirect host is resolved
	// again within the chain and neighboring tasks, and an address change
	// is picked up a minute later — not critical for the crawler (checks
	// are periodic, §3.3). The cache saves a trip to the resolver but does
	// not replace it: Unbound itself caches by the real TTLs.
	DefaultCacheTTL = 60 * time.Second
	// DefaultCacheEntries is the entry limit: ~4096 redirect hosts with
	// address slices — a few megabytes of worker memory (§3.2).
	DefaultCacheEntries = 4096
)

// CacheOptions are the CachingResolver parameters. Zero values are
// replaced with defaults. The cache parameters are not exposed in the
// configuration: they are not in the spec 5.5 limits table — a transport
// detail.
type CacheOptions struct {
	TTL        time.Duration
	MaxEntries int
}

// cacheEntry is a cache record: the addresses and the expiry moment.
type cacheEntry struct {
	addrs  []netip.Addr
	expiry time.Time
}

// CachingResolver is a cache wrapper over a resolver: a hit on a
// non-expired entry never touches the resolver.
//
// The cache is a separate decorator layer over Resolver, not a transport
// field: T-048 resolves Location and meta refresh hosts with the same
// cache (meta refresh is the content level, bypassing the transport's
// dial), while the transport stays dependent on a single interface. Errors
// are not cached — a resolver failure is retried by a real request (the
// "negative answer forever" loop clipper is not needed: resolver
// degradation is a monitoring signal, not a normal mode). Concurrent
// misses on one host both go to the resolver (singleflight was not
// introduced: redirect hosts within a task barely overlap, the savings
// are rare, and the coordination complexity is real).
type CachingResolver struct {
	inner   Resolver
	ttl     time.Duration
	max     int
	mu      sync.Mutex
	entries map[string]cacheEntry
	now     func() time.Time // for the determinism of the TTL tests
}

// NewCachingResolver wraps inner with a TTL- and entry-limit cache.
func NewCachingResolver(inner Resolver, opts CacheOptions) *CachingResolver {
	if opts.TTL <= 0 {
		opts.TTL = DefaultCacheTTL
	}
	if opts.MaxEntries <= 0 {
		opts.MaxEntries = DefaultCacheEntries
	}
	return &CachingResolver{
		inner:   inner,
		ttl:     opts.TTL,
		max:     opts.MaxEntries,
		entries: make(map[string]cacheEntry, 64),
		now:     time.Now,
	}
}

// LookupNetIP resolves the host with the cache: a hit within the TTL —
// without touching the resolver; an expired entry is deleted, a miss goes
// to inner and is stored on success.
func (c *CachingResolver) LookupNetIP(ctx context.Context, host string) ([]netip.Addr, error) {
	key := strings.ToLower(strings.TrimSpace(host))
	now := c.now()

	c.mu.Lock()
	if e, ok := c.entries[key]; ok {
		if now.Before(e.expiry) {
			c.mu.Unlock()
			return e.addrs, nil
		}
		delete(c.entries, key) // expired
	}
	c.mu.Unlock()

	addrs, err := c.inner.LookupNetIP(ctx, host)
	if err != nil {
		return nil, err
	}
	c.store(key, addrs, c.now())
	return addrs, nil
}

// store saves an entry; when the limit is reached the expired entries are
// purged first, and if that freed no room an arbitrary entry is evicted
// (Go's map traversal order is random). For a precise eviction policy
// (LRU) this traded simplicity and predictable memory: redirect-host
// addresses are cheap, evicting a live entry costs one resolve, not
// correctness (spec 3.2 — memory is bounded).
func (c *CachingResolver) store(key string, addrs []netip.Addr, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= c.max {
		for k, e := range c.entries {
			if !now.Before(e.expiry) {
				delete(c.entries, k)
			}
		}
	}
	if len(c.entries) >= c.max {
		for k := range c.entries {
			delete(c.entries, k)
			break
		}
	}
	c.entries[key] = cacheEntry{addrs: addrs, expiry: now.Add(c.ttl)}
}

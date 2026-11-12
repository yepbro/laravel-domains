// Resolve-hook tests: the ResolverFunc adapter, the system resolver and
// the cache (hit/miss, TTL, eviction, errors, concurrency) on a fake inner
// resolver and injected clocks.

package httpfetch

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeResolver is a resolver stub with call counting.
type fakeResolver struct {
	calls atomic.Int32
	addrs []netip.Addr
	err   error
}

func (f *fakeResolver) LookupNetIP(_ context.Context, host string) ([]netip.Addr, error) {
	f.calls.Add(1)
	return f.addrs, f.err
}

var testAddrs = []netip.Addr{netip.MustParseAddr("192.0.2.10"), netip.MustParseAddr("2001:db8::10")}

// TestCachingResolverHitMissTTL: a miss goes to the resolver, a hit does
// not, an expired entry is a miss again.
func TestCachingResolverHitMissTTL(t *testing.T) {
	inner := &fakeResolver{addrs: testAddrs}
	clock := fakeClock(time.Unix(0, 0))
	c := NewCachingResolver(inner, CacheOptions{TTL: 60 * time.Second, MaxEntries: 10})
	c.now = clock.now

	ctx := context.Background()

	// A miss: the first call goes to the resolver.
	addrs, err := c.LookupNetIP(ctx, "redirect.example.net")
	if err != nil || len(addrs) != 2 {
		t.Fatalf("the first resolve: %v %v", addrs, err)
	}
	if got := inner.calls.Load(); got != 1 {
		t.Fatalf("resolver calls: %d, 1 expected", got)
	}

	// A hit: within the TTL the resolver is not touched.
	clock.advance(59 * time.Second)
	if _, err := c.LookupNetIP(ctx, "redirect.example.net"); err != nil {
		t.Fatalf("a resolve from the cache: %v", err)
	}
	if got := inner.calls.Load(); got != 1 {
		t.Fatalf("the cache did not work: %d calls, 1 expected", got)
	}

	// Expiry: an entry older than the TTL — the resolver again.
	clock.advance(2 * time.Second) // 61 s in total > 60 s
	if _, err := c.LookupNetIP(ctx, "redirect.example.net"); err != nil {
		t.Fatalf("a resolve after the expiry: %v", err)
	}
	if got := inner.calls.Load(); got != 2 {
		t.Fatalf("a new resolve expected after the TTL: %d calls", got)
	}
	if len(c.entries) != 1 {
		t.Errorf("cache entries: %d, 1 expected", len(c.entries))
	}

	// The key is case-insensitive: the same host in another case is a hit.
	if _, err := c.LookupNetIP(ctx, "  Redirect.Example.NET "); err != nil {
		t.Fatalf("a resolve in a different case: %v", err)
	}
	if got := inner.calls.Load(); got != 2 {
		t.Errorf("the host case must hit: %d calls, 2 expected", got)
	}
}

// TestCachingResolverErrorsNotCached: a resolver failure is retried by a
// real request, an empty successful response is cached.
func TestCachingResolverErrorsNotCached(t *testing.T) {
	boom := errors.New("the resolver is unavailable")
	inner := &fakeResolver{err: boom}
	c := NewCachingResolver(inner, CacheOptions{})

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := c.LookupNetIP(ctx, "broken.example.net"); !errors.Is(err, boom) {
			t.Fatalf("the resolver error does not propagate: %v", err)
		}
	}
	if got := inner.calls.Load(); got != 2 {
		t.Errorf("the error must not be cached: %d calls, 2 expected", got)
	}

	// An empty successful response is a legal outcome (NODATA): cache it
	// so as not to poke the resolver on every chain step.
	inner.err = nil
	inner.addrs = nil
	if addrs, err := c.LookupNetIP(ctx, "nodata.example.net"); err != nil || addrs != nil {
		t.Fatalf("an empty answer: %v %v", addrs, err)
	}
	if _, err := c.LookupNetIP(ctx, "nodata.example.net"); err != nil {
		t.Fatalf("an empty answer from the cache: %v", err)
	}
	if got := inner.calls.Load(); got != 3 {
		t.Errorf("an empty answer must be cached: %d calls, 3 expected", got)
	}
}

// TestCachingResolverEviction: the entry limit is held; when full, the
// expired entries are purged first.
func TestCachingResolverEviction(t *testing.T) {
	inner := &fakeResolver{addrs: testAddrs}
	clock := fakeClock(time.Unix(0, 0))
	c := NewCachingResolver(inner, CacheOptions{TTL: time.Minute, MaxEntries: 2})
	c.now = clock.now

	ctx := context.Background()
	if _, err := c.LookupNetIP(ctx, "a.example.net"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.LookupNetIP(ctx, "b.example.net"); err != nil {
		t.Fatal(err)
	}
	if len(c.entries) != 2 {
		t.Fatalf("entries: %d, 2 expected", len(c.entries))
	}

	// A third entry with two live ones: one was evicted (which one is
	// arbitrary policy), the new one stored, the limit held.
	if _, err := c.LookupNetIP(ctx, "c.example.net"); err != nil {
		t.Fatal(err)
	}
	if len(c.entries) != 2 {
		t.Errorf("the entry cap is not held: %d", len(c.entries))
	}
	if _, ok := c.entries["c.example.net"]; !ok {
		t.Error("the new entry was evicted instead of the old one")
	}

	// After the TTL expires, an overflow purges the expired entries, not
	// the live ones.
	clock.advance(2 * time.Minute)
	if _, err := c.LookupNetIP(ctx, "d.example.net"); err != nil {
		t.Fatal(err)
	}
	if len(c.entries) != 1 {
		t.Errorf("expired entries are not purged: %d, 1 alive expected", len(c.entries))
	}
	if _, ok := c.entries["d.example.net"]; !ok {
		t.Error("the entry is missing after the purge")
	}
}

// TestCachingResolverConcurrent: concurrent requests of one host are
// correct (-race catches races).
func TestCachingResolverConcurrent(t *testing.T) {
	inner := &fakeResolver{addrs: testAddrs}
	c := NewCachingResolver(inner, CacheOptions{})

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.LookupNetIP(context.Background(), "hot.example.net"); err != nil {
				t.Errorf("a concurrent resolve: %v", err)
			}
		}()
	}
	wg.Wait()
	if len(c.entries) != 1 {
		t.Errorf("entries: %d, 1 expected", len(c.entries))
	}
}

// TestResolverFunc is the function adapter for the interface.
func TestResolverFunc(t *testing.T) {
	var fn ResolverFunc = func(ctx context.Context, host string) ([]netip.Addr, error) {
		if host != "fn.example.net" {
			return nil, errors.New("an unexpected host")
		}
		return testAddrs, nil
	}
	var r Resolver = fn // interface compilation needs no separate check
	addrs, err := r.LookupNetIP(context.Background(), "fn.example.net")
	if err != nil || len(addrs) != len(testAddrs) {
		t.Fatalf("the adapter: %v %v", addrs, err)
	}
}

// TestSystemResolverLocalhost — the system resolver is alive and returns
// addresses (localhost from /etc/hosts; the Docker environment has it).
func TestSystemResolverLocalhost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	addrs, err := SystemResolver.LookupNetIP(ctx, "localhost")
	if err != nil {
		t.Fatalf("the system resolver: %v", err)
	}
	if len(addrs) == 0 {
		t.Fatal("localhost without addresses")
	}
}

// --- fake clocks ----------------------------------------------------------------

// fakeClock is deterministic time for the TTL tests.
type fakeClock time.Time

func (c *fakeClock) now() time.Time          { return time.Time(*c) }
func (c *fakeClock) advance(d time.Duration) { *c = fakeClock(time.Time(*c).Add(d)) }

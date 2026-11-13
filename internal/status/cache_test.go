// Unit tests of the TTL cache of heavy selections (T-161) on the fake
// pool of status_test.go: a hit within the TTL window (no repeated SQL),
// a recount after expiry (the time-based invalidation on the fake's
// clock), independent sections (the cache "keys"), errors are not
// cached, a value copy does not spoil the cache, the option defaults.
package status

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"crawler/internal/model"
)

// cacheClock — the controlled cache clock: the test moves the time past
// the TTL without real waiting.
type cacheClock struct{ now time.Time }

func (c *cacheClock) tick(d time.Duration) { c.now = c.now.Add(d) }
func (c *cacheClock) Now() time.Time       { return c.now }

func newCacheTestPool() *statusPool {
	p := newStatusPool()
	p.seedZone(1, "crawl.test", true, 100, 30, 5, 40)
	p.zoneLaravel = 4
	p.thr = Throughput{DoneToday: 10, ElapsedSec: 3600, ResultsHour: 5, RenderedHour: 2, HourWindowSec: 5400}
	p.domStatuses = []statusStatusSeed{{name: model.StatusNotDelegated.String(), count: 7}}
	return p
}

// countSQL — how many times the pool saw a query containing what.
func countSQL(calls []string, what string) int {
	n := 0
	for _, q := range calls {
		if strings.Contains(q, what) {
			n++
		}
	}
	return n
}

// TestCacheTTLHit — within the TTL window the section is handed out
// without a DB call; after expiry (the boundary: now == expiresAt is
// already expired) — a re-selection.
func TestCacheTTLHit(t *testing.T) {
	pool := newCacheTestPool()
	clock := &cacheClock{now: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)}
	c := NewCache(CacheOptions{TTL: time.Minute, Now: clock.Now})

	ctx := context.Background()
	z1, err := c.ZoneStats(ctx, pool)
	if err != nil {
		t.Fatalf("ZoneStats: %v", err)
	}
	if len(z1) != 1 || z1[0].DomainsTotal != 100 {
		t.Fatalf("zones = %+v, want one crawl.test zone", z1)
	}
	// Within the window — the cache, not SQL.
	clock.tick(59 * time.Second)
	if _, err := c.ZoneStats(ctx, pool); err != nil {
		t.Fatalf("ZoneStats from the cache: %v", err)
	}
	if n := countSQL(pool.calls(), "FROM zones AS z"); n != 1 {
		t.Errorf("zone selects = %d, want 1 (within the TTL)", n)
	}

	// Exactly at the TTL boundary the entry is no longer valid (the cache
	// is strictly earlier than expiresAt); after it — a re-selection.
	clock.tick(time.Second)
	z2, err := c.ZoneStats(ctx, pool)
	if err != nil {
		t.Fatalf("ZoneStats after the TTL: %v", err)
	}
	if n := countSQL(pool.calls(), "FROM zones AS z"); n != 2 {
		t.Errorf("zone selects = %d, want 2 (the TTL expired — a re-count)", n)
	}
	if !reflect.DeepEqual(z1, z2) {
		t.Errorf("after the re-count the zones = %+v, want the same as the first time %+v", z2, z1)
	}
}

// TestCacheSectionsIndependent — the sections ("keys") are independent:
// their own entry and their own TTL start moment — one expired does not
// touch the other.
func TestCacheSectionsIndependent(t *testing.T) {
	pool := newCacheTestPool()
	clock := &cacheClock{now: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)}
	c := NewCache(CacheOptions{TTL: time.Minute, Now: clock.Now})
	ctx := context.Background()

	if _, err := c.ZoneStats(ctx, pool); err != nil {
		t.Fatalf("ZoneStats: %v", err)
	}
	clock.tick(30 * time.Second) // the zones live until 1:30, the throughput starts at 0:30
	if _, err := c.Throughput(ctx, pool); err != nil {
		t.Fatalf("Throughput: %v", err)
	}
	if _, err := c.DomainStatuses(ctx, pool); err != nil {
		t.Fatalf("DomainStatuses: %v", err)
	}
	clock.tick(45 * time.Second) // 1:15: the zones expired, the throughput/statuses are alive

	if _, err := c.ZoneStats(ctx, pool); err != nil {
		t.Fatalf("ZoneStats: %v", err)
	}
	if _, err := c.Throughput(ctx, pool); err != nil {
		t.Fatalf("Throughput: %v", err)
	}
	if _, err := c.DomainStatuses(ctx, pool); err != nil {
		t.Fatalf("DomainStatuses: %v", err)
	}

	calls := pool.calls()
	if n := countSQL(calls, "FROM zones AS z"); n != 2 {
		t.Errorf("zone selects = %d, want 2 (the first + after expiry)", n)
	}
	if n := countSQL(calls, "AS done_today"); n != 1 {
		t.Errorf("throughput selects = %d, want 1 (the entry is alive)", n)
	}
	// The statuses marker is LIKE 'status (domainStatusSQL): with T-179
	// the throughputSQL also reads crawler_counters (the daily check
	// counter) — the FROM crawler_counters substring no longer
	// distinguishes the queries.
	if n := countSQL(calls, "LIKE 'status"); n != 1 {
		t.Errorf("status selects = %d, want 1 (the entry is alive)", n)
	}
}

// TestCacheErrorsNotCached — a selection error is returned and is not
// cached: the next call repeats the DB call.
func TestCacheErrorsNotCached(t *testing.T) {
	pool := newCacheTestPool()
	clock := &cacheClock{now: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)}
	c := NewCache(CacheOptions{TTL: time.Minute, Now: clock.Now})
	ctx := context.Background()

	boom := errors.New("db unavailable")
	pool.setQueryErr(boom)
	if _, err := c.ZoneStats(ctx, pool); !errors.Is(err, boom) {
		t.Fatalf("the error = %v, want the original DB error", err)
	}
	pool.setQueryErr(nil)
	z, err := c.ZoneStats(ctx, pool)
	if err != nil {
		t.Fatalf("ZoneStats after the recovery: %v", err)
	}
	if len(z) != 1 {
		t.Errorf("zones = %+v, want the recovered DB data", z)
	}
	if n := countSQL(pool.calls(), "FROM zones AS z"); n != 2 {
		t.Errorf("zone selects = %d, want 2 (the error is not cached)", n)
	}
}

// TestCacheCloneOnReturn — the returned value is a copy: a mutation of
// the call result does not change the subsequent cache answers.
func TestCacheCloneOnReturn(t *testing.T) {
	pool := newCacheTestPool()
	clock := &cacheClock{now: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)}
	c := NewCache(CacheOptions{TTL: time.Minute, Now: clock.Now})
	ctx := context.Background()

	z1, err := c.ZoneStats(ctx, pool)
	if err != nil {
		t.Fatalf("ZoneStats: %v", err)
	}
	z1[0].DomainsTotal = 999999
	s1, err := c.DomainStatuses(ctx, pool)
	if err != nil {
		t.Fatalf("DomainStatuses: %v", err)
	}
	s1["new"] = 999999

	z2, err := c.ZoneStats(ctx, pool)
	if err != nil {
		t.Fatalf("ZoneStats from the cache: %v", err)
	}
	if z2[0].DomainsTotal != 100 {
		t.Errorf("mutating the result corrupted the zone cache: %+v", z2[0])
	}
	s2, err := c.DomainStatuses(ctx, pool)
	if err != nil {
		t.Fatalf("DomainStatuses from the cache: %v", err)
	}
	if s2["new"] == 999999 {
		t.Errorf("mutating the result corrupted the status cache: %v", s2)
	}
}

// TestCacheNilPool — a nil pool is a selection error (as FetchZoneStats),
// not a panic and not a caching of the error.
func TestCacheNilPool(t *testing.T) {
	clock := &cacheClock{now: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)}
	c := NewCache(CacheOptions{TTL: time.Minute, Now: clock.Now})
	if _, err := c.ZoneStats(context.Background(), nil); err == nil ||
		!strings.Contains(err.Error(), "pool not set") {
		t.Fatalf("a nil pool = %v, want a FetchZoneStats error", err)
	}
}

// TestCacheOptionsDefaults — a zero/negative TTL — the default
// (the pin literal 60s: a comparison with the constant mutates together
// with it); the set values are not overwritten.
func TestCacheOptionsDefaults(t *testing.T) {
	if DefaultCacheTTL != time.Minute {
		t.Errorf("DefaultCacheTTL = %v, want 60s", DefaultCacheTTL)
	}
	if got := (CacheOptions{}).withDefaults().TTL; got != time.Minute {
		t.Errorf("TTL = %v, want the default 60s", got)
	}
	if got := (CacheOptions{TTL: -time.Second}).withDefaults().TTL; got != time.Minute {
		t.Errorf("TTL = %v after a negative value, want the default", got)
	}
	if got := (CacheOptions{TTL: 5 * time.Minute}).withDefaults().TTL; got != 5*time.Minute {
		t.Errorf("TTL = %v, want the set value (the default must not overwrite)", got)
	}
}

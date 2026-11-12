package render

// The triage of the surviving mutants of the CI run, part C (render,
// Nov 10, 2026, score 0.916 — 54 survivors): the behavioral holes are
// closed by the tests of this file and by extending the tables of
// TestWhatwgParseIPv4 and TestOptionsValidate in render_test.go; the
// provably equivalent mutants are suppressed by the blacklist
// (test/mutation/blacklist.txt, the rationale groups —
// docs/mutation-testing.md §4). The tests are hermetic (the LookupHost,
// procRSS/timeNow seams, the fake cache time) and do not sleep longer
// than 50 ms per exchange: the suite of the package runs for every
// mutant in CI and cannot afford extra seconds.

import (
	"context"
	"math"
	"net/netip"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
)

// TestWhatwgIPv4Number — the direct contract of the number of one part
// of a WHATWG IPv4 host: both hexadecimal prefixes (0x/0X) are equal,
// the forms hold the full uint64 width (the 2^63/maxuint64 boundary —
// bitSize 64, not "almost 64"), a foreign character is not a number
// (the hex detection by s[1] without a leading-zero check would take
// "9x" for an empty mantissa).
func TestWhatwgIPv4Number(t *testing.T) {
	cases := []struct {
		s    string
		want uint64
		ok   bool
	}{
		{"0x7f", 0x7f, true},
		{"0X1F", 31, true},
		{"0x", 0, true},
		{"0x8000000000000000", 1 << 63, true},
		{"01000000000000000000000", 1 << 63, true},
		{"18446744073709551615", math.MaxUint64, true},
		{"010", 8, true},
		{"9x", 0, false},
		{"", 0, false},
		{"09", 0, false},
	}
	for _, tc := range cases {
		v, ok := whatwgIPv4Number(tc.s)
		if ok != tc.ok || (ok && v != tc.want) {
			t.Errorf("whatwgIPv4Number(%q) = %d %v, want %d %v",
				tc.s, v, ok, tc.want, tc.ok)
		}
	}
}

// TestValidateTaskURLInternalTriage — the contracts of the task address
// check (T-219): an unparseable URL and an empty host are already
// rejected by the scheme validation (validateTaskURL) — there is no
// address check (nil) and there must be no panic on a nil URL; an
// internal numeric-like host — a task error before navigating to the
// page.
func TestValidateTaskURLInternalTriage(t *testing.T) {
	for _, raw := range []string{"http://[::1", "http:///path", "not a url"} {
		if err := validateTaskURLInternal(raw); err != nil {
			t.Errorf("%q: want nil (the scheme/host are rejected earlier), got %v", raw, err)
		}
	}
	err := validateTaskURLInternal("http://127.1/")
	if err == nil || ClassOf(err) != ClassTask {
		t.Fatalf("an internal numeric-looking host: want a task error, got %v", err)
	}
}

// TestRequestURLInternalTriage — literal host forms are not resolved
// at all (the address is known without a DNS exchange): an external
// IPv6 literal (bypassing the WHATWG numeric-like branch) and an
// external decimal numeric-like form ("134744072" = 8.8.8.8) get a
// verdict without a single resolver call; an unparseable URL is not
// internal, without a panic. A hostname reaches the resolve exactly
// once.
func TestRequestURLInternalTriage(t *testing.T) {
	lookup := &fakeLookupHost{addrs: map[string][]netip.Addr{
		"fallback.example.net": {netip.MustParseAddr("203.0.113.7")},
	}}
	dns := newDNSCache(lookup.lookup)
	ctx := context.Background()

	if requestURLInternal(ctx, "http://[::1", dns) {
		t.Error("an unparseable URL: want not-internal")
	}
	if requestURLInternal(ctx, "http://[2001:db8::1]/", dns) {
		t.Error("an external IPv6 literal: want not-internal")
	}
	if requestURLInternal(ctx, "http://134744072/", dns) {
		t.Error("an external numeric-looking form: want not-internal")
	}
	if n := len(lookup.callsOf()); n != 0 {
		t.Fatalf("resolver exchanges %d (%v), want 0 — a literal is not resolved",
			n, lookup.callsOf())
	}

	if requestURLInternal(ctx, "http://fallback.example.net/", dns) {
		t.Error("a name with a public resolution: want not-internal")
	}
	if n := len(lookup.callsOf()); n != 1 {
		t.Fatalf("resolver exchanges %d, want 1 (the name resolves)", n)
	}
}

// TestDNSCacheDefaultsTriage — the seams and production constants of
// the resolve cache (T-230): a nil LookupHost is replaced by the system
// resolver, the time seam is built lazily, the exchange cap is exactly
// 3 s (the dnsLookupTimeout doc comment: above the cold unbound
// recursion, below the page timeout). A cache without a preset now
// works and caches the verdict.
func TestDNSCacheDefaultsTriage(t *testing.T) {
	c := newDNSCache(nil)
	if c.lookup == nil {
		t.Fatal("a nil resolver seam: want the system resolver (lookupHostSystem)")
	}
	if c.now == nil {
		t.Fatal("the time seam is not set (want time.Now by default)")
	}
	if c.timeout != 3*time.Second {
		t.Fatalf("the exchange cap %s, want exactly 3s (dnsLookupTimeout)", c.timeout)
	}

	c2 := &dnsCache{
		lookup: (&fakeLookupHost{addrs: map[string][]netip.Addr{
			"rebind.example.net": {netip.MustParseAddr("169.254.169.254")},
		}}).lookup,
		timeout: time.Hour,
	}
	if !c2.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("the first resolution: want internal (169.254.169.254)")
	}
	if !c2.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("a repeat: want internal (the verdict from the cache)")
	}
	if len(c2.entries) != 1 {
		t.Fatalf("cache entries %d, want 1 (the verdict is cached)", len(c2.entries))
	}
}

// TestDNSCacheExchangeTimeoutTriage — the boundary caps of a single
// exchange (T-230): timeout == 0 means "no deadline wrapper" — the
// resolve goes directly and its answer (an internal address) becomes
// the verdict; a positive cap below the answer time cuts the exchange
// off — the verdict is fail-open and is not cached. The resolver
// respects the exchange context; the 50 ms margin for the answer
// against a nanosecond deadline removes the dependence on the
// scheduler timings.
func TestDNSCacheExchangeTimeoutTriage(t *testing.T) {
	lookup := func(ctx context.Context, _ string) ([]netip.Addr, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
			return []netip.Addr{netip.MustParseAddr("169.254.169.254")}, nil
		}
	}

	direct := newDNSCache(lookup)
	direct.timeout = 0
	if !direct.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("timeout=0: a direct resolution without a deadline — want an internal verdict")
	}
	if len(direct.entries) != 1 {
		t.Fatalf("timeout=0: entries %d, want 1 (a success is cached)", len(direct.entries))
	}

	cut := newDNSCache(lookup)
	cut.timeout = 1 // 1 ns: a positive cap — the exchange is cut off
	if cut.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("timeout=1ns: want fail-open (the deadline outruns the exchange)")
	}
	if len(cut.entries) != 0 {
		t.Fatalf("timeout=1ns: entries %d, want 0 (a hangup is not cached)", len(cut.entries))
	}
}

// TestDNSCacheTTLBoundaryTriage — the TTL boundaries of the resolve
// cache: an entry aged 29.5 s is still fresh (the verdict from the
// cache, no exchange), aged exactly 30 s — expired (a fresh exchange
// takes the new DNS answer). The literals pin the production 30 s
// (dnsCacheTTL): the slice of boundaries is what keeps the
// DNS-rebinding guard (T-230) from the verdict sticking longer than
// the window.
func TestDNSCacheTTLBoundaryTriage(t *testing.T) {
	addrs := []netip.Addr{netip.MustParseAddr("169.254.169.254")}
	calls := 0
	c := newDNSCache(func(_ context.Context, _ string) ([]netip.Addr, error) {
		calls++
		return addrs, nil
	})
	now := time.Now()
	c.now = func() time.Time { return now }

	if !c.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("the first exchange: want internal")
	}

	// The age of 29.5 s < 30 s: the cache is fresh — the new DNS answer
	// is not visible.
	addrs = []netip.Addr{netip.MustParseAddr("203.0.113.7")}
	now = now.Add(29500 * time.Millisecond)
	if !c.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("age 29.5s: want internal from the cache")
	}
	if calls != 1 {
		t.Fatalf("exchanges %d, want 1 (a fresh entry — no exchange)", calls)
	}

	// The age of exactly 30 s: expired — the fresh exchange carries the
	// external answer.
	now = now.Add(500 * time.Millisecond)
	if c.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("age 30s: want external by the fresh exchange")
	}
	if calls != 2 {
		t.Fatalf("exchanges %d, want 2 (an expired entry is refreshed)", calls)
	}
}

// TestDNSCacheCanceledNotCachedTriage — an exchange cut off by a
// context cancellation (the end of the page) is not a DNS answer: the
// verdict is fail-open but is NOT cached; a repeated request for the
// same host exchanges again and already sees the internal address (the
// guard does not freeze at "external" after the cut-off).
func TestDNSCacheCanceledNotCachedTriage(t *testing.T) {
	canceled := false
	c := newDNSCache(func(_ context.Context, _ string) ([]netip.Addr, error) {
		if canceled {
			return nil, context.Canceled
		}
		return []netip.Addr{netip.MustParseAddr("169.254.169.254")}, nil
	})
	now := time.Now()
	c.now = func() time.Time { return now }

	canceled = true
	if c.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("a hangup on cancellation: want fail-open (not internal)")
	}
	if len(c.entries) != 0 {
		t.Fatalf("entries %d, want 0 (a hangup is not cached)", len(c.entries))
	}

	canceled = false
	if !c.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("a repeat after the hangup: a fresh exchange — an internal address")
	}
}

// TestProbeConnTriage — the contracts of the connection probe
// (T-218/T-226): a nil browser and a stuck connection loss are not
// probed (false); a unit-test browser without a chromedp context and a
// context without a browser session (Browser == nil before the first
// Run) — the probe has nothing to check, the verdict is "alive"
// without a transport round-trip.
func TestProbeConnTriage(t *testing.T) {
	var nilBrowser *Browser
	if nilBrowser.probeConn() {
		t.Error("a nil browser: the probe fails")
	}

	lost := testBrowser(1)
	lost.lost.Store(true)
	if lost.probeConn() {
		t.Error("a sticky connection loss: the probe fails")
	}

	if !testBrowser(1).probeConn() {
		t.Error("a browser without a chromedp context (unit fakes): the probe has nothing to check — alive")
	}

	bctx, cancel := chromedp.NewContext(context.Background())
	defer cancel()
	withCtx := testBrowser(1)
	withCtx.ctx = bctx
	if !withCtx.probeConn() {
		t.Error("a context without a browser session (Browser == nil before Run): the probe does not fail")
	}
}

// TestBrowserRSSCacheTriage — the boundaries of the RSS cache (T-229):
// only positive measurements are cached — a zero one (the root
// disappeared from the snapshot) and a negative one (/proc
// unavailable) are neither read as a value nor written (a fresh
// snapshot on every call); a cache with the same root, a zero/negative
// value and a fresh label — not a cache; a boundary-small positive
// measurement (1 B) is a valid value and is both written and read from
// the cache.
func TestBrowserRSSCacheTriage(t *testing.T) {
	oldProc, oldNow := procRSS, timeNow
	fakeNow := time.Now()
	var rss int64
	procRSS = func(int) int64 { return rss }
	timeNow = func() time.Time { return fakeNow }
	t.Cleanup(func() { procRSS, timeNow = oldProc, oldNow })

	// The zero and negative cache of the same root with a fresh label
	// — not values: the call goes to a measurement.
	rss = 777
	zero := &Browser{rootPID: 1234, rssPID: 1234, rss: 0, rssAt: fakeNow}
	if got := zero.RSS(); got != 777 {
		t.Fatalf("a zero cache: RSS = %d, want 777 (a fresh snapshot)", got)
	}
	negative := &Browser{rootPID: 1234, rssPID: 1234, rss: -1, rssAt: fakeNow}
	if got := negative.RSS(); got != 777 {
		t.Fatalf("a negative cache: RSS = %d, want 777 (a fresh snapshot)", got)
	}

	// A measurement of 0 (the root disappeared) and -1 (/proc
	// unavailable) are not values but their absence: not cached (the
	// label is not written).
	rss = 0
	gone := &Browser{rootPID: 4242424}
	if got := gone.RSS(); got != 0 {
		t.Fatalf("a vanished root: RSS = %d, want 0", got)
	}
	if !gone.rssAt.IsZero() || gone.rssPID != 0 {
		t.Fatalf("a zero snapshot is cached (rssAt=%v, rssPID=%d) — only positives are cached",
			gone.rssAt, gone.rssPID)
	}
	rss = -1
	noProc := &Browser{rootPID: 4242424}
	if got := noProc.RSS(); got != -1 {
		t.Fatalf("an unavailable /proc: RSS = %d, want -1", got)
	}
	if !noProc.rssAt.IsZero() {
		t.Fatalf("the -1 snapshot is cached (rssAt=%v) — only positives are cached", noProc.rssAt)
	}

	// The boundary positive 1 B: written to the cache and read from it
	// (the second call does not measure again).
	rss = 1
	one := &Browser{rootPID: 1234}
	if got := one.RSS(); got != 1 {
		t.Fatalf("the first read: RSS = %d, want 1", got)
	}
	rss = 2
	if got := one.RSS(); got != 1 {
		t.Fatalf("the second read: RSS = %d, want 1 from the cache", got)
	}
}

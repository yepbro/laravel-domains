// T-049 limits tests. Levels:
//
//   - clean unit tests: the anti-tarpit deadline math, the token bucket
//     (rate), the per-IP and global semaphores (queue, cancellation, map
//     cleanup), the options from the configuration and the defaults; the
//     per-IP override limits by prefixes and the slot-wait observation
//     (T-165);
//   - httptest servers on the loopback: the body cap (exactly at the
//     limit / over — truncated, including chunked), the body tarpit (a
//     speed violation after the first byte), the rate without false
//     triggers, the connection limit per IP (the third request waits in
//     the queue), cancellation in the queue, the RoundTrip rate,
//     keep-alive (the connection is not reused);
//   - the helpers of transport_test.go are reused: loopbackAddr, hostURL,
//     stallServer.

package httpfetch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crawler/internal/config"
)

// --- options and math --------------------------------------------------------

// TestLimitOptionsFromConfigAndDefaults: carrying over the fields of the
// http section, the spec 5.5 limits table defaults, enabling limits in
// NewOptions.
func TestLimitOptionsFromConfigAndDefaults(t *testing.T) {
	cfg := config.HTTP{
		Concurrency:    777,
		PerIPLimit:     3,
		PerIPOverrides: map[string]int{"195.208.9.9/16": 12},
		MaxBodyBytes:   config.ByteSize(3 << 20),
		MinBodySpeed:   config.ByteSize(4096),
		MaxRPS:         500,
		MaxRedirects:   8,
		UserAgent:      "t",
		ConnectTimeout: config.Duration(time.Second),
	}
	lo := NewLimitOptions(cfg)
	if lo.MaxBodyBytes != 3<<20 || lo.MinBodySpeed != 4096 || lo.PerIPConns != 3 ||
		lo.GlobalConns != 777 || lo.MaxRPS != 500 {
		t.Errorf("the carry-over from the http section: %+v", lo)
	}
	// The prefix map (T-165) is parsed into override limits (masking
	// normalization); junk in manual assembly disables the whole list (the
	// valid path cuts junk off earlier).
	if len(lo.PerIPOverrides) != 1 {
		t.Fatalf("the override limits: %v, want one prefix", lo.PerIPOverrides)
	}
	if o := lo.PerIPOverrides[0]; o.Prefix.String() != "195.208.0.0/16" || o.Limit != 12 {
		t.Errorf("override = %s:%d, want 195.208.0.0/16:12", o.Prefix, o.Limit)
	}
	bad := NewLimitOptions(config.HTTP{PerIPLimit: 3, PerIPOverrides: map[string]int{"junk": 8}})
	if bad.PerIPOverrides != nil {
		t.Errorf("a junk map: %v, want nil (the feature off as a whole)", bad.PerIPOverrides)
	}

	// NewOptions always enables limits (the production transport comes
	// with limits).
	o := NewOptions(cfg)
	if o.Limits == nil {
		t.Fatal("NewOptions: the limits are not enabled")
	}
	if o.Limits.MaxBodyBytes != 3<<20 || o.Limits.MaxRPS != 500 {
		t.Errorf("the NewOptions limits: %+v", o.Limits)
	}

	// Defaults: the spec 5.5 limits table + the derived ones (RPS, the
	// speed window).
	d := LimitOptions{}.withDefaults()
	if d.MaxBodyBytes != DefaultLimitBodyBytes || d.MinBodySpeed != DefaultLimitBodySpeed ||
		d.MinSpeedWindow != DefaultLimitSpeedWindow || d.PerIPConns != DefaultLimitPerIPConns ||
		d.GlobalConns != DefaultLimitGlobalConns || d.MaxRPS != DefaultLimitRPS {
		t.Errorf("the limit defaults: %+v", d)
	}
	if d.Burst != d.GlobalConns {
		t.Errorf("the burst default: %d, GlobalConns=%d expected", d.Burst, d.GlobalConns)
	}
}

// TestBodyLimiterDeadline: the deadline for finishing the remainder is
// now+remain/speed but never shorter than the window floor; speed off —
// no deadline.
func TestBodyLimiterDeadline(t *testing.T) {
	b := &BodyLimiter{speed: 2048, window: 500 * time.Millisecond}
	if d := b.deadlineLocked(4096); d != 2*time.Second {
		t.Errorf("the deadline of a 4096-byte remainder at 2048 bytes/s: %s, 2s expected", d)
	}
	if d := b.deadlineLocked(100); d != 500*time.Millisecond {
		t.Errorf("a short remainder without the window floor: %s, 500ms expected", d)
	}
	b.speed = 0
	if d := b.deadlineLocked(4096); d != 0 {
		t.Errorf("the anti-tarpit off must not produce a deadline: %s", d)
	}
	// A large remainder at 1 byte/s does not overflow time.Duration.
	b.speed = 1
	if d := b.deadlineLocked(2 << 20); d <= 0 {
		t.Errorf("a large remainder: an unexpected deadline %s", d)
	}
}

// TestBodyLimiterClosedRead: reading after Close is an error.
func TestBodyLimiterClosedRead(t *testing.T) {
	b := newBodyLimiter(io.NopCloser(strings.NewReader("abc")), func() {},
		LimitOptions{MaxBodyBytes: 10, MinBodySpeed: 1, MinSpeedWindow: time.Hour})
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := b.Read(make([]byte, 4)); !errors.Is(err, errLimitBodyClosed) {
		t.Errorf("reading a closed body: %v", err)
	}
}

// readCountingRC is a source that counts Read calls (zero-length buffers
// and repeated reads must not touch the source).
type readCountingRC struct {
	io.Reader
	reads *int
}

func (r *readCountingRC) Read(p []byte) (int, error) {
	*r.reads++
	return r.Reader.Read(p)
}

func (r *readCountingRC) Close() error { return nil }

// TestBodyLimiterZeroBufAndRepeatRead: a zero-length buffer does not read
// the source (the io.Reader contract); after the tail beyond the cap has
// been classified, repeated reads return the remembered verdict (io.EOF or
// an error) without re-reading the probe byte.
func TestBodyLimiterZeroBufAndRepeatRead(t *testing.T) {
	run := func(t *testing.T, size, limit int, wantTrunc bool) {
		t.Helper()
		var reads int
		bl := newBodyLimiter(&readCountingRC{Reader: bytes.NewReader(bytes.Repeat([]byte("a"), size)), reads: &reads},
			func() {}, LimitOptions{MaxBodyBytes: int64(limit)}) // speed 0 — the watchdog is off

		if n, err := bl.Read(nil); n != 0 || err != nil {
			t.Errorf("Read(nil) = (%d, %v), expected (0, nil)", n, err)
		}
		if n, err := bl.Read(make([]byte, 0)); n != 0 || err != nil {
			t.Errorf("Read(an empty buffer) = (%d, %v), expected (0, nil)", n, err)
		}
		if reads != 0 {
			t.Fatalf("zero buffers read the source: %d reads", reads)
		}

		buf := make([]byte, size+8)
		got := 0
		for got < limit && got < size {
			n, err := bl.Read(buf[got:])
			if err != nil {
				t.Fatalf("reading up to the cap: %v", err)
			}
			got += n
		}
		if _, err := bl.Read(buf); err != io.EOF {
			t.Fatalf("the tail probe: %v, io.EOF expected", err)
		}
		if bl.Truncated() != wantTrunc {
			t.Errorf("the truncation flag: %v, expected %v", bl.Truncated(), wantTrunc)
		}
		probes := reads
		for i := 0; i < 3; i++ {
			if n, err := bl.Read(buf); n != 0 || err != io.EOF {
				t.Errorf("re-read #%d: (%d, %v), expected (0, io.EOF)", i+1, n, err)
			}
		}
		if reads != probes {
			t.Errorf("re-reads touched the source: %d → %d reads", probes, reads)
		}
		_ = bl.Close()
	}
	t.Run("over the limit", func(t *testing.T) { run(t, 10, 5, true) })
	t.Run("exactly at the limit", func(t *testing.T) { run(t, 5, 5, false) })
}

// --- the rate token bucket ---------------------------------------------------

// TestLimitBucketPacing: a burst up to burst goes out at once, further
// tokens at the rate pace; the wait is interrupted by a ctx cancellation.
func TestLimitBucketPacing(t *testing.T) {
	b := newLimitBucket(25, 2) // 25 tokens/s, burst 2
	start := time.Now()
	for i := 0; i < 5; i++ {
		if err := b.take(context.Background()); err != nil {
			t.Fatalf("take #%d: %v", i+1, err)
		}
	}
	elapsed := time.Since(start)
	// 2 instantly + 3 × 40 ms ≈ 120 ms; the lower bound with a margin.
	if elapsed < 100*time.Millisecond {
		t.Errorf("the bucket rate is not held: 5 tokens in %s (≥100ms expected)", elapsed)
	}
	if elapsed >= 2*time.Second {
		t.Errorf("the bucket rate is stretched: %s", elapsed)
	}

	// An empty bucket: the wait is interrupted by a ctx cancellation.
	slow := newLimitBucket(0.001, 1)
	if err := slow.take(context.Background()); err != nil {
		t.Fatalf("the burst token: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := slow.take(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("the token wait is not interrupted by the cancellation: %v", err)
	}
}

// --- connection semaphores ---------------------------------------------------

// TestLimitIPMapQueueAndCancel: the per-IP cap, the queue is woken by a
// release, a cancelled waiter takes no slot, an emptied entry is removed
// from the map.
func TestLimitIPMapQueueAndCancel(t *testing.T) {
	m := limitIPMap{limit: 2, entries: make(map[netip.Addr]*limitIPEntry)}
	ip := netip.MustParseAddr("192.0.2.1")
	ctx := context.Background()

	r1, err := m.acquire(ctx, ip)
	if err != nil {
		t.Fatalf("slot 1: %v", err)
	}
	r2, err := m.acquire(ctx, ip)
	if err != nil {
		t.Fatalf("slot 2: %v", err)
	}

	// The third one queues (does not return while the slots are taken).
	queued := make(chan error, 1)
	var release3 func()
	go func() {
		rel, err := m.acquire(ctx, ip)
		if err == nil {
			release3 = rel
		}
		queued <- err
	}()
	select {
	case err := <-queued:
		t.Fatalf("a third slot taken at the full cap: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	// A release wakes the waiter.
	r1()
	select {
	case err := <-queued:
		if err != nil {
			t.Fatalf("the waiter did not get a slot: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("the queue did not wake on the slot release")
	}

	// A cancelled waiter gets an error and takes no slot.
	cctx, cancel := context.WithCancel(ctx)
	cancelled := make(chan error, 1)
	go func() {
		_, err := m.acquire(cctx, ip)
		cancelled <- err
	}()
	// The slots are full again (r2 + release3): the waiter blocks.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-cancelled:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("the queue cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("the cancellation did not wake the waiter")
	}

	// The emptied map is cleaned: no per-IP entries.
	r2()
	release3()
	m.mu.Lock()
	entries := len(m.entries)
	m.mu.Unlock()
	if entries != 0 {
		t.Errorf("the per-IP entries are not cleaned up: %d", entries)
	}
}

// TestLimitStateAcquireDial: the per-IP → global order; the global cap
// queues; a cancellation in the queue is an error; a release wakes.
func TestLimitStateAcquireDial(t *testing.T) {
	st := newLimitState(LimitOptions{
		PerIPConns: 4, GlobalConns: 2, MaxRPS: 1000, Burst: 10,
		MaxBodyBytes: 1024, MinBodySpeed: 1024, MinSpeedWindow: time.Second,
	})
	ip1 := netip.MustParseAddr("192.0.2.1")
	ip2 := netip.MustParseAddr("192.0.2.2")
	ip3 := netip.MustParseAddr("192.0.2.3")
	ctx := context.Background()

	r1, err := st.acquireDial(ctx, ip1)
	if err != nil {
		t.Fatalf("connection 1: %v", err)
	}
	r2, err := st.acquireDial(ctx, ip2)
	if err != nil {
		t.Fatalf("connection 2: %v", err)
	}

	// The global cap is exhausted (per-IP is not): the third one queues.
	queued := make(chan struct{ rel func() }, 1)
	go func() {
		rel, err := st.acquireDial(ctx, ip3)
		if err != nil {
			t.Errorf("connection 3: %v", err)
			rel = func() {}
		}
		queued <- struct{ rel func() }{rel}
	}()
	select {
	case got := <-queued:
		got.rel()
		t.Fatal("a third connection passed the full global cap")
	case <-time.After(100 * time.Millisecond):
	}

	// A cancelled waiter gets an error and takes no slots.
	cctx, cancel := context.WithCancel(ctx)
	cancelled := make(chan error, 1)
	go func() {
		_, err := st.acquireDial(cctx, ip3)
		cancelled <- err
	}()
	cancel()
	select {
	case err := <-cancelled:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("the dial queue cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("the cancellation did not wake the connection waiter")
	}

	// Releasing a global slot wakes the queue.
	r1()
	select {
	case got := <-queued:
		defer got.rel()
	case <-time.After(time.Second):
		t.Fatal("the connection queue did not wake")
	}
	r2()
}

// --- per-IP override limits and wait observation (T-165) ---------------------

// TestParsePerIPOverrides: parsing the "CIDR → limit" map — masking
// normalization ("195.208.9.9/16" is the network 195.208.0.0/16), IPv4 and
// IPv6; errors: a junk CIDR, a limit < 1, a duplicate network after
// masking.
func TestParsePerIPOverrides(t *testing.T) {
	overrides, err := parsePerIPOverrides(map[string]int{
		"195.208.9.9/16": 12,
		"2a02:6b8::/32":  8,
		"192.0.2.0/24":   2,
	})
	if err != nil {
		t.Fatalf("parsing the map: %v", err)
	}
	want := map[string]int{"195.208.0.0/16": 12, "2a02:6b8::/32": 8, "192.0.2.0/24": 2}
	if len(overrides) != len(want) {
		t.Fatalf("%d overrides expected, got %v", len(want), overrides)
	}
	for _, o := range overrides {
		if o.Limit != want[o.Prefix.String()] {
			t.Errorf("override %s = %d, want %d", o.Prefix, o.Limit, want[o.Prefix.String()])
		}
	}

	for name, m := range map[string]map[string]int{
		"a junk CIDR":         {"195.208.0.0.1/16": 8},
		"a limit below 1":     {"195.208.0.0/16": 0},
		"a duplicate network": {"195.208.0.0/16": 8, "195.208.9.9/16": 12},
	} {
		if _, err := parsePerIPOverrides(m); err == nil {
			t.Errorf("%s: a parse error expected", name)
		}
	}
	if got, err := parsePerIPOverrides(nil); err != nil || got != nil {
		t.Errorf("an empty map: %v, %v — nil, nil expected", got, err)
	}
}

// TestLimitIPMapLimitFor: the effective IP limit is the longest matching
// prefix (longest-prefix, regardless of the map order); outside the
// prefixes — the common one; IPv4-in-IPv6 is unmapped, the families do not
// overlap.
func TestLimitIPMapLimitFor(t *testing.T) {
	overrides, err := parsePerIPOverrides(map[string]int{
		"195.208.0.0/16": 5,
		"195.208.7.0/24": 3,
		"2a02:6b8::/32":  4,
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	st := newLimitState(LimitOptions{
		PerIPConns: 2, PerIPOverrides: overrides,
		MaxBodyBytes: 1024, MinBodySpeed: 1024, MinSpeedWindow: time.Second,
	})

	for _, tc := range []struct {
		ip   string
		want int
	}{
		{"195.208.7.1", 3},        // /24 is longer than /16
		{"195.208.9.9", 5},        // /16
		{"195.209.0.1", 2},        // outside the prefixes — the common limit
		{"2a02:6b8::1", 4},        // an IPv6 prefix
		{"2001:db8::1", 2},        // outside the IPv6 prefixes
		{"::ffff:195.208.7.1", 3}, // IPv4-in-IPv6 — Unmap, semantically IPv4
	} {
		if got := st.ips.limitFor(netip.MustParseAddr(tc.ip)); got != tc.want {
			t.Errorf("limitFor(%s) = %d, want %d", tc.ip, got, tc.want)
		}
	}
}

// TestLimitIPMapOverrideCapacity: an IP semaphore inside a prefix holds
// the prefix limit (the fifth connection waits), an IP outside — the
// common limit (the third one waits).
func TestLimitIPMapOverrideCapacity(t *testing.T) {
	st := newLimitState(LimitOptions{
		PerIPConns: 2, GlobalConns: 64, MaxRPS: 1000, Burst: 64,
		PerIPOverrides: []PerIPOverride{{Prefix: netip.MustParsePrefix("195.208.0.0/16"), Limit: 4}},
		MaxBodyBytes:   1024, MinBodySpeed: 1024, MinSpeedWindow: time.Second,
	})
	ctx := context.Background()

	// A parker IP: 4 slots of the prefix limit.
	park := netip.MustParseAddr("195.208.0.1")
	parkRel := make([]func(), 0, 4)
	for i := 0; i < 4; i++ {
		r, err := st.ips.acquire(ctx, park)
		if err != nil {
			t.Fatalf("slot %d in the prefix: %v", i+1, err)
		}
		parkRel = append(parkRel, r)
	}
	parkQueued := make(chan func(), 1)
	go func() {
		rel, err := st.ips.acquire(ctx, park)
		if err != nil {
			t.Errorf("the prefix fifth connection: %v", err)
			rel = func() {}
		}
		parkQueued <- rel
	}()
	select {
	case <-parkQueued:
		t.Fatal("the prefix fifth slot taken at the limit 4")
	case <-time.After(100 * time.Millisecond):
	}

	// An ordinary IP: the common limit of 2.
	plain := netip.MustParseAddr("192.0.2.7")
	r1, err := st.ips.acquire(ctx, plain)
	if err != nil {
		t.Fatalf("slot 1 outside the prefix: %v", err)
	}
	r2, err := st.ips.acquire(ctx, plain)
	if err != nil {
		t.Fatalf("slot 2 outside the prefix: %v", err)
	}
	plainQueued := make(chan func(), 1)
	go func() {
		rel, err := st.ips.acquire(ctx, plain)
		if err != nil {
			t.Errorf("a third connection outside the prefix: %v", err)
			rel = func() {}
		}
		plainQueued <- rel
	}()
	select {
	case <-plainQueued:
		t.Fatal("a third slot outside the prefix taken at the shared limit 2")
	case <-time.After(100 * time.Millisecond):
	}

	// A release wakes the matching queue (the cross queues stay silent).
	parkRel[0]()
	select {
	case rel := <-parkQueued:
		rel()
	case <-time.After(time.Second):
		t.Fatal("the prefix queue did not wake")
	}
	r1()
	select {
	case rel := <-plainQueued:
		rel()
	case <-time.After(time.Second):
		t.Fatal("the outside-the-prefix queue did not wake")
	}
	for _, r := range parkRel[1:] {
		r()
	}
	r2()
}

// TestLimitIPMapWaitObserved: a slot wait is observed by onWait after the
// grant (an instant slot too, ≈0); a cancelled wait does not become an
// observation; a waiter behind a holder gets a non-zero wait.
func TestLimitIPMapWaitObserved(t *testing.T) {
	var mu sync.Mutex
	waits := make([]time.Duration, 0, 2)
	m := limitIPMap{
		limit:   1,
		onWait:  func(d time.Duration) { mu.Lock(); waits = append(waits, d); mu.Unlock() },
		entries: make(map[netip.Addr]*limitIPEntry),
	}
	ip := netip.MustParseAddr("192.0.2.1")
	ctx := context.Background()

	r1, err := m.acquire(ctx, ip)
	if err != nil {
		t.Fatalf("slot 1: %v", err)
	}

	// A waiter behind a holder: does not return while the slot is taken.
	got := make(chan func(), 1)
	go func() {
		rel, err := m.acquire(ctx, ip)
		if err != nil {
			t.Errorf("the waiter: %v", err)
			rel = func() {}
		}
		got <- rel
	}()
	select {
	case <-got:
		t.Fatal("a second slot taken at the full cap")
	case <-time.After(100 * time.Millisecond):
	}

	// A cancelled wait does not become an observation (the stage error
	// classification is done by the consumer).
	cctx, cancel := context.WithCancel(ctx)
	cancelled := make(chan error, 1)
	go func() {
		_, err := m.acquire(cctx, ip)
		cancelled <- err
	}()
	cancel()
	select {
	case err := <-cancelled:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("the queue cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("the cancellation did not wake the waiter")
	}
	mu.Lock()
	n := len(waits)
	mu.Unlock()
	if n != 1 {
		t.Fatalf("observations after the cancellation %d, want 1 (only the instant slot)", n)
	}

	// The grant is woken by a release; the queue wait is observed.
	r1()
	select {
	case rel := <-got:
		rel()
	case <-time.After(time.Second):
		t.Fatal("the queue did not wake on the release")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(waits) != 2 {
		t.Fatalf("observations %d, want 2", len(waits))
	}
	if waits[1] < 20*time.Millisecond {
		t.Errorf("the queue wait %s, want ≥20ms (the waiter stood ≥100ms)", waits[1])
	}
	if waits[0] >= waits[1] {
		t.Errorf("the instant slot %s is not less than the queue wait %s", waits[0], waits[1])
	}
}

// holdServer is a server whose handlers live until release: occupied
// connections hold the limit slots, and the caller can see how many
// entered.
func holdServer(t *testing.T) (srv *httptest.Server, entered func() int64, release func()) {
	t.Helper()
	var n atomic.Int64
	ch := make(chan struct{})
	var once sync.Once
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-ch:
		case <-time.After(10 * time.Second): // a guard against hanging forever
		}
		_, _ = fmt.Fprint(w, "done")
	}))
	return srv, n.Load, func() { once.Do(func() { close(ch) }) }
}

// waitCount waits for the counter value (a dialog with the test server).
func waitCount(t *testing.T, entered func() int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if entered() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the entered counter %d, %d expected", entered(), want)
}

// smallLimits is the limits for the integration tests: the connection caps
// are below the request count, the rate does not interfere.
func smallLimits(perIP, global int) *LimitOptions {
	return &LimitOptions{
		PerIPConns:     perIP,
		GlobalConns:    global,
		MaxRPS:         10000,
		MaxBodyBytes:   1 << 20,
		MinBodySpeed:   1 << 20,
		MinSpeedWindow: 100 * time.Millisecond,
	}
}

// TestFetcherPerIPLimitQueue: two connections to one IP hold slots, the
// third request waits in the per-IP queue and passes after a release.
func TestFetcherPerIPLimitQueue(t *testing.T) {
	srv, entered, release := holdServer(t)
	defer srv.Close() // LIFO: the release below closes the handlers earlier
	defer release()

	f := NewFetcher(Options{
		UserAgent:     "t",
		HeaderTimeout: 5 * time.Second,
		TotalTimeout:  10 * time.Second,
		Limits:        smallLimits(2, 8),
	})
	u := hostURL(srv.URL, "perip.example.net")
	do := func(ctx context.Context) (*FetchResult, error) {
		return f.Fetch(ctx, FetchRequest{URL: u, Addrs: loopbackAddr()})
	}

	res1, err := do(context.Background())
	if err != nil {
		t.Fatalf("Fetch 1: %v", err)
	}
	res2, err := do(context.Background())
	if err != nil {
		t.Fatalf("Fetch 2: %v", err)
	}
	defer func() { _ = res2.Body.Close() }()
	waitCount(t, entered, 2)

	type result struct {
		res *FetchResult
		err error
	}
	third := make(chan result, 1)
	go func() {
		res, err := do(context.Background())
		third <- result{res, err}
	}()

	// The slots are taken: the third does not dial — still two handlers.
	time.Sleep(300 * time.Millisecond)
	if got := entered(); got != 2 {
		t.Fatalf("the per-IP limit passed: %d handlers entered (2 expected)", got)
	}

	// Release: the bodies are read out, the connections close, the slots
	// go to the waiting third request.
	release()
	if _, err := io.ReadAll(res1.Body); err != nil {
		t.Fatalf("body 1: %v", err)
	}
	_ = res1.Body.Close()
	if _, err := io.ReadAll(res2.Body); err != nil {
		t.Fatalf("body 2: %v", err)
	}

	select {
	case got := <-third:
		if got.err != nil {
			t.Fatalf("Fetch 3 after the release: %v", got.err)
		}
		defer func() { _ = got.res.Body.Close() }()
		if got.res.StatusCode != http.StatusOK {
			t.Errorf("status 3: %d", got.res.StatusCode)
		}
		if _, err := io.ReadAll(got.res.Body); err != nil {
			t.Errorf("body 3: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the third request did not pass after the slots were released")
	}
	waitCount(t, entered, 3)
}

// TestFetcherPerIPQueueCancelled: a request waiting in the per-IP queue is
// interrupted by a ctx cancellation (the caller's timeout).
func TestFetcherPerIPQueueCancelled(t *testing.T) {
	srv, entered, release := holdServer(t)
	defer srv.Close()
	defer release()

	f := NewFetcher(Options{
		UserAgent:     "t",
		HeaderTimeout: 5 * time.Second,
		TotalTimeout:  10 * time.Second,
		Limits:        smallLimits(2, 8),
	})
	u := hostURL(srv.URL, "cancel-queue.example.net")
	fetch := func() (*FetchResult, error) {
		return f.Fetch(context.Background(), FetchRequest{URL: u, Addrs: loopbackAddr()})
	}

	res1, err := fetch()
	if err != nil {
		t.Fatalf("Fetch 1: %v", err)
	}
	defer func() { _ = res1.Body.Close() }()
	res2, err := fetch()
	if err != nil {
		t.Fatalf("Fetch 2: %v", err)
	}
	defer func() { _ = res2.Body.Close() }()
	waitCount(t, entered, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = f.Fetch(ctx, FetchRequest{URL: u, Addrs: loopbackAddr()})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("a request-cancellation error in the per-IP queue expected")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("the queue error does not propagate DeadlineExceeded: %v", err)
	}
	if elapsed >= 3*time.Second {
		t.Errorf("the queue cancellation fired late: %s", elapsed)
	}
}

// --- the body cap ------------------------------------------------------------

// TestFetchBodyLimitExactAndTruncated: a body exactly at the limit is read
// in full without the truncation flag; over the limit — truncated with the
// Truncated flag (not a network error), including responses without
// Content-Length.
func TestFetchBodyLimitExactAndTruncated(t *testing.T) {
	body := func(size int, contentLength bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if contentLength {
				w.Header().Set("Content-Length", fmt.Sprint(size))
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(bytes.Repeat([]byte("x"), size))
			if !contentLength {
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
		}
	}

	run := func(t *testing.T, h http.HandlerFunc, size, limit int, wantTrunc bool) {
		t.Helper()
		srv := httptest.NewServer(h)
		defer srv.Close()
		f := NewFetcher(Options{
			UserAgent:    "t",
			TotalTimeout: 5 * time.Second,
			Limits: &LimitOptions{
				PerIPConns: 4, GlobalConns: 8, MaxRPS: 10000,
				MaxBodyBytes: int64(limit), MinBodySpeed: 1 << 20, MinSpeedWindow: 100 * time.Millisecond,
			},
		})
		res, err := f.Fetch(context.Background(), FetchRequest{URL: hostURL(srv.URL, "body.example.net"), Addrs: loopbackAddr()})
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		got, readErr := io.ReadAll(res.Body)
		_ = res.Body.Close()
		if readErr != nil {
			t.Fatalf("truncation is not a read error: %v", readErr)
		}
		if len(got) != min(size, limit) {
			t.Errorf("%d bytes read, %d expected", len(got), min(size, limit))
		}
		if res.BodyLimiter == nil {
			t.Fatal("BodyLimiter is not set")
		}
		if res.BodyLimiter.Truncated() != wantTrunc {
			t.Errorf("the truncation flag: %v, expected %v", res.BodyLimiter.Truncated(), wantTrunc)
		}
	}

	t.Run("exactly at the limit", func(t *testing.T) { run(t, body(1024, true), 1024, 1024, false) })
	t.Run("over the limit", func(t *testing.T) { run(t, body(4096, true), 4096, 1024, true) })
	t.Run("over the limit chunked", func(t *testing.T) { run(t, body(4096, false), 4096, 1024, true) })
}

// --- the anti-tarpit ---------------------------------------------------------

// TestFetchBodyTarpitSpeed: the headers and the first body chunk arrived,
// the tail is silent — the deadline for finishing the remainder (renewed
// as reading progresses) expires, the read is interrupted with a
// timeout-classified error, the connection closes.
func TestFetchBodyTarpitSpeed(t *testing.T) {
	srv, done := stallServer(t, func(w http.ResponseWriter, stall <-chan struct{}) {
		w.Header().Set("Content-Length", "8192")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bytes.Repeat([]byte("x"), 1024))
		if f, ok := w.(http.Flusher); ok {
			f.Flush() // the first byte is at the client — the tarpit starts
		}
		select {
		case <-stall:
		case <-time.After(5 * time.Second):
		}
		_, _ = w.Write(bytes.Repeat([]byte("x"), 7168))
	})
	defer done()

	f := NewFetcher(Options{
		UserAgent:    "t",
		TotalTimeout: 10 * time.Second,
		Limits: &LimitOptions{
			PerIPConns: 4, GlobalConns: 16, MaxRPS: 10000,
			MaxBodyBytes: 8192, MinBodySpeed: 1 << 20, MinSpeedWindow: 200 * time.Millisecond,
		},
	})
	res, err := f.Fetch(context.Background(), FetchRequest{URL: hostURL(srv.URL, "tarpit-body.example.net"), Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("the headers must make it in time: %v", err)
	}
	start := time.Now()
	_, readErr := io.ReadAll(res.Body)
	elapsed := time.Since(start)
	if readErr == nil {
		t.Fatal("a body tarpit must be interrupted by the speed violation")
	}
	if !IsTimeout(readErr) {
		t.Errorf("a speed violation is timeout-classified, got: %v", readErr)
	}
	if !strings.Contains(readErr.Error(), "tarpit") {
		t.Errorf("the error without the reason: %v", readErr)
	}
	if elapsed < 150*time.Millisecond || elapsed >= 2*time.Second {
		t.Errorf("the speed violation fired outside the window bounds: %s", elapsed)
	}
	// The connection is closed: a repeated read gets the same error, not a
	// block.
	if _, err := res.Body.Read(make([]byte, 16)); err == nil || !IsTimeout(err) {
		t.Errorf("a read after the violation: %v", err)
	}
	_ = res.Body.Close()
}

// TestFetchBodySpeedPacedOK: the body trickles faster than the minimum
// speed — the renewable deadline never fires, the body is read in full.
func TestFetchBodySpeedPacedOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "4096")
		w.WriteHeader(http.StatusOK)
		for i := 0; i < 4; i++ {
			_, _ = w.Write(bytes.Repeat([]byte("x"), 1024))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(60 * time.Millisecond) // ~17 KiB/s > the minimum of 4 KiB/s
		}
	}))
	defer srv.Close()

	f := NewFetcher(Options{
		UserAgent:    "t",
		TotalTimeout: 10 * time.Second,
		Limits: &LimitOptions{
			PerIPConns: 4, GlobalConns: 16, MaxRPS: 10000,
			MaxBodyBytes: 4096, MinBodySpeed: 4096, MinSpeedWindow: 150 * time.Millisecond,
		},
	})
	res, err := f.Fetch(context.Background(), FetchRequest{URL: hostURL(srv.URL, "paced.example.net"), Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	got, readErr := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if readErr != nil {
		t.Fatalf("a false anti-tarpit trigger: %v", readErr)
	}
	if len(got) != 4096 {
		t.Errorf("%d bytes read, 4096 expected", len(got))
	}
	if res.BodyLimiter.Truncated() {
		t.Error("a body within the limit must not get the truncation flag")
	}
}

// --- the request rate and keep-alive -----------------------------------------

// TestRoundTripRPSPacing: the rate token bucket in RoundTrip — requests
// beyond the burst keep the rate interval.
func TestRoundTripRPSPacing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	f := NewFetcher(Options{
		UserAgent: "t",
		Limits: &LimitOptions{
			PerIPConns: 4, GlobalConns: 8, MaxRPS: 20, Burst: 1,
			MaxBodyBytes: 1 << 20, MinBodySpeed: 1 << 20, MinSpeedWindow: 100 * time.Millisecond,
		},
	})
	start := time.Now()
	for i := 0; i < 3; i++ {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		resp, err := f.RoundTrip(req)
		if err != nil {
			t.Fatalf("RoundTrip #%d: %v", i+1, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	elapsed := time.Since(start)
	// Burst 1 + 2 tokens of 50 ms ≈ 100 ms.
	if elapsed < 90*time.Millisecond {
		t.Errorf("the RPS rate is not held: 3 requests in %s (≥90ms expected)", elapsed)
	}
	if elapsed >= 2*time.Second {
		t.Errorf("the RPS rate is stretched: %s", elapsed)
	}
}

// TestKeepAliveConnectionNotReused: keep-alive is disabled (spec 5.5:
// "disabled or a very short idle"; the T-047/T-049 choice) — two
// sequential requests to one host go over different connections, the
// client sends Connection: close.
func TestKeepAliveConnectionNotReused(t *testing.T) {
	var mu sync.Mutex
	remotes := make(map[string]bool)
	closeHeader := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		remotes[r.RemoteAddr] = true
		if r.Close {
			closeHeader++
		}
		mu.Unlock()
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	f := NewFetcher(Options{UserAgent: "t"})
	for i := 0; i < 2; i++ {
		res, err := f.Fetch(context.Background(), FetchRequest{URL: hostURL(srv.URL, "keepalive.example.net"), Addrs: loopbackAddr()})
		if err != nil {
			t.Fatalf("Fetch #%d: %v", i+1, err)
		}
		if _, err := io.ReadAll(res.Body); err != nil {
			t.Fatalf("body #%d: %v", i+1, err)
		}
		_ = res.Body.Close()
	}
	mu.Lock()
	defer mu.Unlock()
	if len(remotes) != 2 {
		t.Errorf("2 different connections expected (keep-alive off), connections: %d", len(remotes))
	}
	if closeHeader != 2 {
		t.Errorf("Connection: close in both requests: the header in %d of 2", closeHeader)
	}
}

// TestFetchWithoutLimitsNoLimiter: without Options.Limits the T-047
// behavior is preserved — BodyLimiter is nil, the body has no wrapper.
func TestFetchWithoutLimitsNoLimiter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "plain")
	}))
	defer srv.Close()

	f := NewFetcher(Options{UserAgent: "t"})
	res, err := f.Fetch(context.Background(), FetchRequest{URL: hostURL(srv.URL, "plain.example.net"), Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.BodyLimiter != nil {
		t.Error("without Options.Limits BodyLimiter must be nil")
	}
	body, err := io.ReadAll(res.Body)
	if err != nil || string(body) != "plain" {
		t.Errorf("the body without limits: %q, %v", body, err)
	}
}

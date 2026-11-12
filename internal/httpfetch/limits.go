// Limits and protections of the HTTP stage (spec 5.5, the limits table;
// spec 8: "tarpits, endless redirects, huge responses, zip bombs —
// interrupted by the 5.5 limits"). Five mechanisms:
//
//   - the body cap MaxBodyBytes ("max readable body size | 2 MB"):
//     exceeding it is not a network error: the body is truncated, the
//     "data is partial" flag is set in BodyLimiter.Truncated — the
//     decision belongs to the consumer (T-058), the transport only
//     signals;
//   - the anti-tarpit ("min speed of 2 KB/s after the first byte"): a
//     watchdog with a renewable deadline — before each body read, the
//     deadline for finishing the remainder is recomputed as
//     now + remain/speed; the invariant: the total reading time never
//     exceeds the initial remainder/speed, that is, the average speed
//     from any moment to the end is never below the minimum. A violation
//     means timeout classification (limitBodySpeedError → net.Error) and
//     closing the connection by canceling the request context; the
//     deadline is never shorter than MinSpeedWindow — the floor absorbs
//     scheduler jitter and the RTT of the last segment;
//   - connections per IP ("concurrent connections per IP | 3–4"): a
//     semaphore map keyed by the dialed IP (the DNS stage address or the
//     redirect resolve, not the URL host) — limitIPMap; waiters queue
//     with ctx cancellation. Parker prefixes get a raised limit
//     (PerIPOverrides, T-165), the queue wait is observable
//     (OnPerIPWait, T-165 — a :9104 histogram);
//   - the global connection count and request rate ("outbound load
//     limited by the global connection count and RPS"): a counting
//     semaphore for the whole connection and a token bucket per request —
//     limitBucket, the mechanics and rationale are the same as
//     dnsclient.limitBucket (a unified limits-package style): an average
//     rate of max_rps with a short burst up to burst — "a batch goes out
//     at once, the rate evens out"; a fixed window would cut the
//     pipeline's natural bursts.
//
// Integration goes through Options.Limits (nil — a transport without
// limits: the isolated layer tests of T-047; NewOptions always enables
// the limits from the http config section). Connection slots are taken in
// dialLimited before the connect timeout starts: waiting in the queue is
// not part of establishing the connection; the slots are released by
// closing the connection (limitConn). Keep-alive stays disabled (the
// T-047 choice from the "disabled or a very short idle" option of
// spec 5.5, pinned in T-049: one main page per site — a per-host pool
// would ride out a re-resolve and hold thousands of idle connections at
// concurrency 2000).

package httpfetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/netip"
	"sort"
	"sync"
	"time"

	"crawler/internal/config"
)

// The defaults — the spec 5.5 limits table ("all are configurable") and
// spec 5.10. Numbers without a spec entry (RPS, the speed window) —
// defaults derived from the stage's properties; overridden in LimitOptions
// and the configuration.
const (
	// DefaultLimitBodyBytes is the readable body cap ("max readable body
	// size | 2 MB", spec 5.5; http.max_body_bytes).
	DefaultLimitBodyBytes = int64(2 << 20)
	// DefaultLimitBodySpeed is the min body reading speed after the first
	// byte ("tarpit protection | min speed 2 KB/s", spec 5.5;
	// http.min_body_speed), bytes/s.
	DefaultLimitBodySpeed = int64(2 << 10)
	// DefaultLimitSpeedWindow is the floor of the anti-tarpit deadline:
	// the deadline for finishing the remainder is never shorter than this
	// window. Goroutine scheduling jitter and the network RTT of the last
	// segment must not cause false triggers at servers that hold the
	// speed; the protection is unaffected: a stalled body is caught by
	// the window instead of a zero deadline.
	DefaultLimitSpeedWindow = 500 * time.Millisecond
	// DefaultLimitPerIPConns — concurrent connections per IP
	// (spec 5.5 recommends 3–4; http.per_ip_limit): the top of the range.
	DefaultLimitPerIPConns = 4
	// DefaultLimitGlobalConns is the global connection cap
	// (http.concurrency, spec 5.10): a connection counter — the same
	// quantity as the planned worker parallelism.
	DefaultLimitGlobalConns = 2000
	// DefaultLimitRPS is the average request rate (http.max_rps). The spec
	// sets no number: a page takes ~1–2 s, so at concurrency 2000 the
	// natural rate is ~1000–2000 requests/s — the default of 1000 keeps
	// the order of the outbound load of a single IP (network tuning for
	// it is spec 10.2); lowering it further is a configuration matter.
	DefaultLimitRPS = 1000
)

// PerIPOverride is the per-IP semaphore limit for a prefix (T-165, part
// 2b): connections to an IP inside Prefix run with the Limit of
// concurrent connections instead of PerIPConns. The source is the
// config's "CIDR → limit" map (http.per_ip_overrides, parker prefixes);
// the effective IP limit is by the longest matching prefix. An override
// may also be below the default (a soft hoster) — the semantics are
// uniform.
type PerIPOverride struct {
	Prefix netip.Prefix
	Limit  int
}

// LimitOptions is the T-049 limits parameters. Zero values are replaced
// with defaults (withDefaults, the Options style: "0 means the default").
type LimitOptions struct {
	// MaxBodyBytes is the readable body cap (http.max_body_bytes).
	// Exceeding it means truncation and the BodyLimiter.Truncated flag,
	// not a network error (spec 8: the decision belongs to the consumer,
	// T-058).
	MaxBodyBytes int64
	// MinBodySpeed is the min body reading speed after the first byte,
	// bytes/s (http.min_body_speed). A violation is a timeout, the
	// connection is closed.
	MinBodySpeed int64
	// MinSpeedWindow is the floor of the anti-tarpit deadline (see
	// DefaultLimitSpeedWindow).
	MinSpeedWindow time.Duration
	// PerIPConns is the number of concurrent connections per dialed IP
	// (http.per_ip_limit).
	PerIPConns int
	// PerIPOverrides are per-IP override limits by parker prefixes
	// (T-165, http.per_ip_overrides): an IP inside a prefix gets the
	// prefix's limit instead of PerIPConns — the longest matching prefix
	// (longest-prefix; one network listed twice is invalid — the
	// configuration rejects duplicates after masking). Empty — one limit
	// for all IPs (the previous behavior).
	PerIPOverrides []PerIPOverride
	// GlobalConns is the global cap on concurrent connections
	// (http.concurrency).
	GlobalConns int
	// MaxRPS is the average request rate, a token bucket (http.max_rps).
	MaxRPS int
	// Burst is the token bucket capacity: requests in a row without
	// waiting (a short pipeline burst). The default is GlobalConns — a
	// "full wave" of all workers at once; more is pointless: no more
	// connections fly at once.
	Burst int
	// OnPerIPWait observes the wait for a per-IP semaphore slot (T-165,
	// part 1): called after each successful slot grant with the pure
	// queue wait time (a free slot is microseconds). A ctx-cancelled wait
	// does not become an observation — the stage error classification
	// already accounts for it. nil — no observations (a transport outside
	// the http-worker: the CLI, isolated tests). The service assembly
	// wires it to the crawler_http_per_ip_wait_seconds histogram (:9104)
	// — the share of waits > 2 s is read from the buckets (the T-165
	// criterion: < 5% on the production mix).
	OnPerIPWait func(wait time.Duration)
}

// NewLimitOptions assembles the limits from the http section: the body cap
// and per-IP exist since T-006, body speed and RPS are T-049 fields; the
// global connection cap is http.concurrency; parker prefixes (the
// "CIDR → limit" map) are T-165. A map parse error is impossible after
// config.Validate (Load rejects a broken config); in manual assembly a
// junk entry disables overrides entirely — safer than a silent partial
// application: all IPs stay on PerIPLimit.
func NewLimitOptions(cfg config.HTTP) LimitOptions {
	overrides, err := parsePerIPOverrides(cfg.PerIPOverrides)
	if err != nil {
		overrides = nil
	}
	return LimitOptions{
		MaxBodyBytes:   cfg.MaxBodyBytes.B(),
		MinBodySpeed:   cfg.MinBodySpeed.B(),
		PerIPConns:     cfg.PerIPLimit,
		PerIPOverrides: overrides,
		GlobalConns:    cfg.Concurrency,
		MaxRPS:         cfg.MaxRPS,
	}
}

// parsePerIPOverrides parses the "CIDR → limit" map (the
// http.per_ip_overrides format): an IPv4/IPv6 CIDR, a limit >= 1, one
// network (after masking) never appears twice — a duplicate would make
// the limit choice ambiguous (longest-prefix cannot tell equal lengths
// apart). The result order is undefined (a map) — newLimitState does the
// sorting.
func parsePerIPOverrides(m map[string]int) ([]PerIPOverride, error) {
	if len(m) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(m))
	out := make([]PerIPOverride, 0, len(m))
	for cidr, limit := range m {
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil {
			return nil, fmt.Errorf("httpfetch: a per-IP override %q: an IPv4/IPv6 CIDR is expected", cidr)
		}
		if limit < 1 {
			return nil, fmt.Errorf("httpfetch: a per-IP override %s: the limit %d is below 1", prefix, limit)
		}
		masked := prefix.Masked()
		key := masked.String()
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf("httpfetch: a per-IP override: the network %s is listed twice", key)
		}
		seen[key] = struct{}{}
		out = append(out, PerIPOverride{Prefix: masked, Limit: limit})
	}
	return out, nil
}

// withDefaults replaces zero and negative values with defaults.
func (o LimitOptions) withDefaults() LimitOptions {
	if o.MaxBodyBytes <= 0 {
		o.MaxBodyBytes = DefaultLimitBodyBytes
	}
	if o.MinBodySpeed <= 0 {
		o.MinBodySpeed = DefaultLimitBodySpeed
	}
	if o.MinSpeedWindow <= 0 {
		o.MinSpeedWindow = DefaultLimitSpeedWindow
	}
	if o.PerIPConns <= 0 {
		o.PerIPConns = DefaultLimitPerIPConns
	}
	if o.GlobalConns <= 0 {
		o.GlobalConns = DefaultLimitGlobalConns
	}
	if o.MaxRPS <= 0 {
		o.MaxRPS = DefaultLimitRPS
	}
	if o.Burst <= 0 {
		o.Burst = o.GlobalConns
	}
	return o
}

// limitState is the active limits of one Fetcher: the global connection
// semaphore, the rate token bucket and the per-IP semaphores.
// Thread-safe, outlives all transport requests.
type limitState struct {
	opt    LimitOptions
	global chan struct{}
	rps    *limitBucket
	ips    limitIPMap
}

// newLimitState builds the state from the options (the caller has already
// applied the defaults). The per-IP override prefixes (T-165) are sorted
// by descending length: limitFor takes the first match — which is the
// longest one (longest-prefix); equal lengths do not overlap after the
// duplicate-network ban (parsePerIPOverrides/config validation), and the
// order within one length is kept by the stable sort (determinism for
// manually assembled lists too).
func newLimitState(opt LimitOptions) *limitState {
	overrides := make([]PerIPOverride, len(opt.PerIPOverrides))
	copy(overrides, opt.PerIPOverrides)
	sort.SliceStable(overrides, func(i, j int) bool {
		return overrides[i].Prefix.Bits() > overrides[j].Prefix.Bits()
	})
	return &limitState{
		opt:    opt,
		global: make(chan struct{}, opt.GlobalConns),
		rps:    newLimitBucket(float64(opt.MaxRPS), opt.Burst),
		ips: limitIPMap{
			limit:     opt.PerIPConns,
			overrides: overrides,
			onWait:    opt.OnPerIPWait,
			entries:   make(map[netip.Addr]*limitIPEntry),
		},
	}
}

// acquireDial takes the slots of one connection to addr: first per-IP
// (the narrow resource — there are only 3–4), then the global one — the
// order rules out a global slot idling while waiting for a narrow one.
// The queue waits for a release or a ctx cancellation; a cancelled
// caller takes no slots.
func (l *limitState) acquireDial(ctx context.Context, addr netip.Addr) (func(), error) {
	releaseIP, err := l.ips.acquire(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := limitChanAcquire(ctx, l.global); err != nil {
		releaseIP()
		return nil, err
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			limitChanRelease(l.global)
			releaseIP()
		})
	}, nil
}

// limitChanAcquire takes a slot of a channel semaphore; the wait is
// interrupted by a ctx cancellation. The channel wakes waiters in arrival
// order (FIFO) — the queue is fair with no extra code.
func limitChanAcquire(ctx context.Context, ch chan struct{}) error {
	select {
	case ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// limitChanRelease returns a slot of the channel semaphore.
func limitChanRelease(ch chan struct{}) { <-ch }

// limitIPMap is the "connections per IP" semaphores (spec 5.5): an entry
// per IP is created on demand and removed when nobody holds it and nobody
// waits for it (refs) — the map does not grow with the number of checked
// IPs; its size is bounded by transport activity (concurrency).
//
// Override prefixes (T-165): an entry is created with the limit of the
// prefix containing the IP (limitFor, longest-prefix) instead of the
// common limit — the queue wait for parking IPs is observable (onWait,
// T-165 part 1).
type limitIPMap struct {
	limit     int
	overrides []PerIPOverride // prefix length descending (newLimitState)
	onWait    func(wait time.Duration)
	mu        sync.Mutex
	entries   map[netip.Addr]*limitIPEntry
}

// limitIPEntry is a single-IP semaphore and an interest counter (slot
// holders and queue waiters): every acquire increments refs; a slot
// release or a cancelled wait decrements it; at zero the entry leaves the
// map.
type limitIPEntry struct {
	sem  chan struct{}
	refs int
}

// limitFor is the effective connection limit for an IP (T-165): the limit
// of the longest prefix containing the IP, otherwise the common m.limit.
// The dialed address is unmapped (Unmap): an IPv4-in-IPv6 address is
// semantically IPv4 and IPv4 prefixes must match; prefixes of different
// families do not overlap (netip.Contains across families is false), so
// scanning until the first match in descending length order is
// unambiguous.
func (m *limitIPMap) limitFor(ip netip.Addr) int {
	a := ip.Unmap()
	for _, o := range m.overrides {
		if o.Prefix.Contains(a) {
			return o.Limit
		}
	}
	return m.limit
}

// acquire takes a connection slot for an IP; when exhausted — a queue
// until a release or a ctx cancellation. The returned function is
// thread-safe and idempotent (calling it twice breaks nothing). The queue
// wait (including an instant one) is observed by onWait after a
// successful slot grant (T-165): a cancelled wait does not become an
// observation.
func (m *limitIPMap) acquire(ctx context.Context, ip netip.Addr) (func(), error) {
	m.mu.Lock()
	e, ok := m.entries[ip]
	if !ok {
		e = &limitIPEntry{sem: make(chan struct{}, m.limitFor(ip))}
		m.entries[ip] = e
	}
	e.refs++
	m.mu.Unlock()

	start := time.Now()
	select {
	case e.sem <- struct{}{}:
		if m.onWait != nil {
			m.onWait(time.Since(start))
		}
		var once sync.Once
		return func() {
			once.Do(func() {
				<-e.sem
				m.drop(ip, e)
			})
		}, nil
	case <-ctx.Done():
		m.drop(ip, e)
		return nil, ctx.Err()
	}
}

// drop decrements the entry's interest counter and removes the emptied
// entry from the map. Removal is by entry identity: between refs hitting
// zero and the removal, a new entry for the same IP could have been
// created.
func (m *limitIPMap) drop(ip netip.Addr, e *limitIPEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e.refs--
	if e.refs == 0 && m.entries[ip] == e {
		delete(m.entries, ip)
	}
}

// limitBucket is a request-rate token bucket with lazy refills: tokens
// are credited at access time for (now−last)×rate, with no background
// goroutines. The choice against a fixed "N per second" window is the
// same as dnsclient.limitBucket (a unified style): the bucket holds the
// average rate and allows a short burst up to burst, while a window would
// cut subdomain batches and add jitter at the boundaries.
type limitBucket struct {
	mu     sync.Mutex
	rate   float64 // tokens per second (the effective RPS)
	burst  float64 // the bucket capacity
	tokens float64
	last   time.Time // the moment of the last credit
}

// newLimitBucket creates a full bucket (burst tokens): the planned startup
// wave of the pipeline goes out without waiting.
func newLimitBucket(rps float64, burst int) *limitBucket {
	return &limitBucket{
		rate:   rps,
		burst:  float64(burst),
		tokens: float64(burst),
		last:   time.Now(),
	}
}

// take takes a token; with an empty bucket it waits for the next credit
// or a ctx cancellation (a waiter does not hold a token forever).
func (b *limitBucket) take(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		wait, ok := b.tryTake()
		if ok {
			return nil
		}
		if err := limitSleep(ctx, wait); err != nil {
			return err
		}
	}
}

// tryTake takes a token without waiting; on shortage it returns the time
// until the next token.
func (b *limitBucket) tryTake() (time.Duration, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	if b.tokens >= 1 {
		b.tokens--
		return 0, true
	}
	// (1 − tokens)/rate seconds until a whole token; a minimum of 1 ns —
	// protection against a zero wait from rounding (otherwise take would
	// spin idly).
	wait := time.Duration((1 - b.tokens) / b.rate * float64(time.Second))
	if wait < 1 {
		wait = 1
	}
	return wait, false
}

// refill credits tokens for the elapsed time; the caller holds b.mu.
func (b *limitBucket) refill() {
	if now := time.Now(); now.After(b.last) {
		b.tokens = math.Min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
		b.last = now
	}
}

// limitSleep is a pause interruptible by a ctx cancellation.
func limitSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// limitConn is a connection with limit slots (per-IP and global): the
// slots live exactly as long as the connection does, Close returns them.
// The slots of one connection have a single owner; a repeated Close (the
// transport may call it) does not release twice.
type limitConn struct {
	net.Conn
	release func()
	once    sync.Once
}

func (c *limitConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

// dialLimited is a connection attempt under the T-049 connection limits:
// the per-IP slots and the global one are taken before the connect
// timeout starts (a queue wait is not part of establishing the
// connection); a successful connection is wrapped in limitConn. Without
// Options.Limits — a direct pass to dialTimed (a transport without
// limits, T-047).
func (f *Fetcher) dialLimited(ctx context.Context, addr netip.Addr, port string) (net.Conn, error) {
	if f.lim == nil {
		return f.dialTimed(ctx, addr, port)
	}
	release, err := f.lim.acquireDial(ctx, addr)
	if err != nil {
		return nil, err
	}
	conn, err := f.dialTimed(ctx, addr, port)
	if err != nil {
		release()
		return nil, err
	}
	return &limitConn{Conn: conn, release: release}, nil
}

// errLimitBodyClosed — reading after the body was closed.
var errLimitBodyClosed = errors.New("httpfetch: reading a closed body")

// BodyLimiter is a response body under the T-049 limits: the MaxBodyBytes
// cap (truncation with the Truncated flag, not a network error) and the
// anti-tarpit (a min speed after the first byte). It implements
// io.ReadCloser; in FetchResult it is duplicated by the BodyLimiter field
// — the truncation flag is visible to the consumer (T-058) without tying
// to the concrete Body type.
//
// Anti-tarpit: before each read a watchdog is armed with the deadline for
// finishing the remainder of now + remain/speed (renewed as reading
// progresses — the invariant "the average speed to the end is never below
// the minimum"); a violation cancels the request context: the connection
// closes, the blocked read returns, and Read yields limitBodySpeedError
// (timeout classification via IsTimeout). The watchdog is time.AfterFunc
// with no waiting goroutines: disarmed by reading, no leaks.
type BodyLimiter struct {
	rc     io.ReadCloser
	cancel context.CancelFunc

	speed    int64         // bytes/s; 0 — the anti-tarpit is off
	window   time.Duration // the deadline floor (MinSpeedWindow)
	remain   int64         // remaining until the MaxBodyBytes cap
	trunc    bool          // the body is longer than the cap — the data is partial
	probed   bool          // the tail beyond the cap has been classified already
	probeErr error         // the tail classification verdict (repeated reads do not touch the source)

	mu     sync.Mutex
	timer  *time.Timer
	fired  bool // the watchdog fired — a speed violation
	closed bool
}

// newBodyLimiter wraps rc (a body with the shared request cancellation —
// cancelBody); cancel closes the connection on a speed violation.
func newBodyLimiter(rc io.ReadCloser, cancel context.CancelFunc, opt LimitOptions) *BodyLimiter {
	return &BodyLimiter{
		rc:     rc,
		cancel: cancel,
		speed:  opt.MinBodySpeed,
		window: opt.MinSpeedWindow,
		remain: opt.MaxBodyBytes,
	}
}

// Read reads the body no further than the MaxBodyBytes cap. Once the cap
// has been given out, a single probe byte decides whether the body is
// longer than the limit: a byte exists — truncated=true and io.EOF
// (truncation, not an error), an immediate io.EOF — the body is exactly
// at the limit; the probe verdict is remembered — repeated reads return
// it without touching the source. A zero-length buffer does not read the
// source at all (the io.Reader contract). The probe runs under the same
// speed watchdog: a silent tail is the same rate violation.
func (b *BodyLimiter) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, errLimitBodyClosed
	}
	if b.fired {
		b.mu.Unlock()
		return 0, b.speedError()
	}
	if b.remain <= 0 {
		if b.probed {
			probeErr := b.probeErr
			b.mu.Unlock()
			return 0, probeErr
		}
		b.mu.Unlock()
		return b.probeTail()
	}
	if int64(len(p)) > b.remain {
		p = p[:int(b.remain)] // never read beyond the cap
	}
	b.armLocked()
	b.mu.Unlock()

	n, err := b.rc.Read(p)

	b.mu.Lock()
	b.disarmLocked()
	fired := b.fired
	if n > 0 {
		b.remain -= int64(n)
	}
	b.mu.Unlock()

	if fired {
		return n, b.speedError()
	}
	return n, err
}

// probeTail classifies the tail after the cap is exhausted (see Read);
// performed once, the verdict is remembered (probed/probeErr).
func (b *BodyLimiter) probeTail() (int, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, errLimitBodyClosed
	}
	b.armLocked() // remain = 0: the deadline is only the window floor
	b.mu.Unlock()

	var one [1]byte
	n, err := b.rc.Read(one[:])

	b.mu.Lock()
	b.disarmLocked()
	fired := b.fired
	b.probed = true
	if n > 0 {
		b.trunc = true
		b.probeErr = io.EOF // classified as truncation
	} else {
		b.probeErr = err // io.EOF — exactly at the limit; a network error — as is
	}
	b.mu.Unlock()

	if fired {
		return 0, b.speedError()
	}
	if n > 0 {
		return 0, io.EOF // the body was truncated by the cap
	}
	return 0, err // io.EOF — exactly at the limit; a network error — as is
}

// Close stops the watchdog and closes the body.
func (b *BodyLimiter) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.disarmLocked()
	b.mu.Unlock()
	return b.rc.Close()
}

// Truncated — the body exceeded the MaxBodyBytes cap and was truncated:
// the data is partial, not a network error (spec 8). The flag is set by
// reading by the time Body returns io.EOF.
func (b *BodyLimiter) Truncated() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.trunc
}

// armLocked arms the anti-tarpit watchdog: the deadline for finishing the
// remainder of remain/speed from the current moment (spec 5.5: a min
// speed after the first byte). The caller holds b.mu.
func (b *BodyLimiter) armLocked() {
	if b.speed <= 0 {
		return
	}
	d := b.deadlineLocked(b.remain)
	if b.timer == nil {
		b.timer = time.AfterFunc(d, b.onDeadline)
		return
	}
	b.timer.Reset(d)
}

// deadlineLocked is the deadline for finishing the remainder; never
// shorter than the window floor (scheduler jitter, the RTT of the last
// segment).
func (b *BodyLimiter) deadlineLocked(remain int64) time.Duration {
	if b.speed <= 0 {
		return 0
	}
	d := time.Duration(float64(remain) / float64(b.speed) * float64(time.Second))
	if d < b.window {
		d = b.window
	}
	return d
}

// disarmLocked disarms the watchdog (the read returned). The caller holds
// b.mu.
func (b *BodyLimiter) disarmLocked() {
	if b.timer != nil {
		b.timer.Stop()
	}
}

// onDeadline — the deadline expired: the speed is violated. Canceling the
// request context closes the connection — the blocked read returns with
// an error and the connection limit slots are freed (spec 5.5: "on a
// violation the connection is closed").
func (b *BodyLimiter) onDeadline() {
	b.mu.Lock()
	b.fired = true
	b.mu.Unlock()
	b.cancel()
}

// speedError is a speed violation error with the parameters at the
// moment.
func (b *BodyLimiter) speedError() error {
	b.mu.Lock()
	speed, remain := b.speed, b.remain
	b.mu.Unlock()
	return &limitBodySpeedError{speed: speed, remain: remain}
}

// limitBodySpeedError is a violation of the min body reading speed (a
// tarpit, spec 5.5). A net.Error with Timeout()=true: the consumer
// classifies it as a timeout (temp_error, like the other layers —
// IsTimeout), not "no site".
type limitBodySpeedError struct {
	speed  int64 // the setting, bytes/s
	remain int64 // the bytes that were left to read
}

func (e *limitBodySpeedError) Error() string {
	return fmt.Sprintf("httpfetch: the body is served slower than %d bytes/s (%d bytes remain) — a tarpit", e.speed, e.remain)
}

func (e *limitBodySpeedError) Timeout() bool   { return true }
func (e *limitBodySpeedError) Temporary() bool { return true }

// compile-time check: *limitBodySpeedError is a net.Error (IsTimeout).
var _ net.Error = (*limitBodySpeedError)(nil)

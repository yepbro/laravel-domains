// DNS stage limits (spec 5.4: "Concurrency and QPS limits are configurable;
// adaptive shedding when the timeout share grows", "If the server has no
// IPv6 — AAAA queries are disabled by config") — the LimitResolver
// decorator over the Resolver interface — the extension point left by
// T-034. The Exchange contract does not change: the consumer (T-036…T-039)
// gets the same Result and errors as without the wrapper; an RCODE is still not an error.
//
// Three mechanisms:
//
//   - concurrency — the limitSemaphore semaphore with a mutable cap: the
//     adaptive lowers the cap for new exchanges without taking slots from
//     in-flight ones (the shedding is soft: the queue drains at the natural pace of responses);
//   - rate — the limitBucket token bucket with the average rate dns.max_qps
//     and short bursts up to burst (the rationale is at limitBucket);
//   - adaptive — the share of timeouts (IsTimeout) in the sliding window of
//     recent exchanges; above the threshold both limits go down, below the
//     recovery threshold they grow (hysteresis; the mechanics and directions are at limitAdaptive).
//
// The waiting order in Exchange: the semaphore first, then the token. The
// token is taken right before the send: if goroutines took tokens in
// advance and then queued for slots, the accumulated bucket would allow a
// burst past the lowered QPS. The reverse price (a slot held while waiting
// for the token) is not scary: when the rate is QPS-bound, the actual exchange parallelism is below the cap anyway.
//
// AAAA disabling (dns.ipv6=false) — a local empty NOERROR response without
// going to the resolver and without the limits and the adaptive window:
// the resolver is not loaded and the timeout share is not diluted with bogus successes.
//
// The semaphore guards against overspend, it is not a queue for thousands
// of goroutines: the T-036+ consumer keeps no more than concurrency
// workers, so limitSemaphore uses broadcast wakeup (rare state changes),
// not clever "wake one" schemes.

package dnsclient

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miekg/dns"

	"crawler/internal/config"
)

// Default values. The planned limits are from spec 5.10 (dns.concurrency
// 2000, dns.max_qps 10000); the adaptive mechanics are not numerically set
// in the spec — the defaults are derived from the stage properties and can be overridden in LimitOptions.
const (
	// DefaultLimitConcurrency is the planned concurrency of the DNS stage
	// (dns.concurrency, spec 5.10). It matches DefaultUDPPoolSize: the
	// socket pool must not cut the planned concurrency (T-034).
	DefaultLimitConcurrency = 2000
	// DefaultLimitQPS is the planned exchange rate (dns.max_qps, spec 5.10).
	DefaultLimitQPS = 10000
	// DefaultLimitWindow is the timeout share window: 200 exchanges. At the
	// planned 10000 QPS the window fills in ~20 ms — faster than one retry
	// loop (2s × 3 attempts), while single timeouts do not swing the decision.
	DefaultLimitWindow = 200
	// DefaultLimitHighRatio is the shedding threshold: the timeout share ≥ 10%.
	// For a healthy recursive resolver with a warm cache (spec 5.4) timeouts
	// are fractions of a percent; 10% is unambiguously an anomaly.
	DefaultLimitHighRatio = 0.10
	// DefaultLimitLowRatio is the recovery threshold: the share ≤ 2% (a
	// hysteresis with a 2…10% deadband — a steady "slightly bad" does not
	// swing the limits back and forth).
	DefaultLimitLowRatio = 0.02
	// DefaultLimitDownFactor is the shedding multiplier ×0.5: while the
	// resolver times out, every exchange burns the attempt budget and holds a
	// slot — shedding must be decisive (multiplicative, like an AIMD decrease).
	DefaultLimitDownFactor = 0.5
	// DefaultLimitUpFactor is the recovery multiplier ×1.25: raising goes
	// slower than shedding — the resolver has just shown overload, a fast
	// return gives "shed → restored → overload again" oscillations.
	DefaultLimitUpFactor = 1.25
	// DefaultLimitFloorDivisor is the limit floor: base/divisor, but at
	// least 1 (for 2000/10000 — 20/100): at the floor the pipeline keeps
	// running; a full stop (a 1/1 limit) would cut the throughput for the
	// sake of a resolver that is down anyway.
	DefaultLimitFloorDivisor = 100
)

// LimitOptions holds the LimitResolver parameters. Zero values are
// replaced with the defaults (withDefaults, the client Options style).
type LimitOptions struct {
	// Concurrency is the cap on concurrent Exchange calls to the lower
	// resolver (dns.concurrency). Defaults to 2000.
	Concurrency int
	// MaxQPS is the average exchange rate per second (dns.max_qps).
	// Defaults to 10000.
	MaxQPS int
	// Burst is the token bucket capacity: how many exchanges may go in a row
	// without waiting (short pipeline bursts). It defaults to Concurrency —
	// a "full wave" of all slots at once; more is pointless: no more than
	// Concurrency exchanges fly at once. The effective capacity is further
	// capped by one second of rate (see newLimitBucket): a burst longer
	// than the max_qps second budget is resolver overload, not a burst
	// (DNS-10).
	Burst int
	// DisableAAAA — answer AAAA questions with a local empty NOERROR
	// without going to the resolver. Set from dns.ipv6=false (NewLimitOptions):
	// "If the server has no IPv6 — AAAA queries are disabled by config"
	// (spec 5.4); without IPv6 the crawler does not check AAAA addresses over HTTP either.
	DisableAAAA bool

	// AdaptiveWindow is the timeout share window, in exchanges. Defaults to 200.
	AdaptiveWindow int
	// AdaptiveHighRatio is the shedding threshold (share ∈ (0, 1]). Defaults to 0.10.
	AdaptiveHighRatio float64
	// AdaptiveLowRatio is the recovery threshold (share ∈ (0, high)).
	// Defaults to 0.02.
	AdaptiveLowRatio float64
	// AdaptiveDownFactor is the shedding multiplier (∈ (0, 1)). Defaults to 0.5.
	AdaptiveDownFactor float64
	// AdaptiveUpFactor is the recovery multiplier (∈ (1, 10]).
	// Defaults to 1.25.
	AdaptiveUpFactor float64
	// AdaptiveFloorDivisor is the limit floor: base/divisor, at least 1.
	// Defaults to 100.
	AdaptiveFloorDivisor int
}

// NewLimitOptions assembles the parameters from the dns section:
// concurrency and max_qps are the planned limits (spec 5.10, present since
// T-006), ipv6=false disables AAAA questions. The adaptive mechanics are the LimitOptions defaults (constants).
func NewLimitOptions(cfg config.DNS) LimitOptions {
	return LimitOptions{
		Concurrency: cfg.Concurrency,
		MaxQPS:      cfg.MaxQPS,
		DisableAAAA: !cfg.IPv6,
	}
}

// withDefaults replaces zero and negative values with the defaults
// (the Options.withDefaults style: "0 means the default value").
func (o LimitOptions) withDefaults() LimitOptions {
	if o.Concurrency <= 0 {
		o.Concurrency = DefaultLimitConcurrency
	}
	if o.MaxQPS <= 0 {
		o.MaxQPS = DefaultLimitQPS
	}
	if o.Burst <= 0 {
		o.Burst = o.Concurrency
	}
	if o.AdaptiveWindow <= 0 {
		o.AdaptiveWindow = DefaultLimitWindow
	}
	if o.AdaptiveHighRatio <= 0 {
		o.AdaptiveHighRatio = DefaultLimitHighRatio
	}
	if o.AdaptiveLowRatio <= 0 {
		o.AdaptiveLowRatio = DefaultLimitLowRatio
	}
	if o.AdaptiveDownFactor <= 0 {
		o.AdaptiveDownFactor = DefaultLimitDownFactor
	}
	if o.AdaptiveUpFactor <= 0 {
		o.AdaptiveUpFactor = DefaultLimitUpFactor
	}
	if o.AdaptiveFloorDivisor <= 0 {
		o.AdaptiveFloorDivisor = DefaultLimitFloorDivisor
	}
	return o
}

// validate checks the values after the defaults are applied. It catches
// what normalization cannot sensibly fix: inverted hysteresis,
// non-multiplicative factors.
func (o LimitOptions) validate() error {
	if o.Concurrency < 1 {
		return fmt.Errorf("dnsclient: лимит concurrency %d: ожидается ≥ 1", o.Concurrency)
	}
	if o.MaxQPS < 1 {
		return fmt.Errorf("dnsclient: лимит max_qps %d: ожидается ≥ 1", o.MaxQPS)
	}
	if o.Burst < 1 {
		return fmt.Errorf("dnsclient: лимит burst %d: ожидается ≥ 1", o.Burst)
	}
	if o.AdaptiveWindow < 2 {
		return fmt.Errorf("dnsclient: окно адаптива %d: ожидается ≥ 2", o.AdaptiveWindow)
	}
	if o.AdaptiveHighRatio <= 0 || o.AdaptiveHighRatio > 1 {
		return fmt.Errorf("dnsclient: порог снижения %v: ожидается (0, 1]", o.AdaptiveHighRatio)
	}
	if o.AdaptiveLowRatio <= 0 || o.AdaptiveLowRatio >= o.AdaptiveHighRatio {
		return fmt.Errorf("dnsclient: порог восстановления %v: ожидается (0, %v)", o.AdaptiveLowRatio, o.AdaptiveHighRatio)
	}
	if o.AdaptiveDownFactor <= 0 || o.AdaptiveDownFactor >= 1 {
		return fmt.Errorf("dnsclient: фактор снижения %v: ожидается (0, 1)", o.AdaptiveDownFactor)
	}
	if o.AdaptiveUpFactor <= 1 || o.AdaptiveUpFactor > 10 {
		return fmt.Errorf("dnsclient: фактор восстановления %v: ожидается (1, 10]", o.AdaptiveUpFactor)
	}
	if o.AdaptiveFloorDivisor < 1 {
		return fmt.Errorf("dnsclient: делитель пола лимитов %d: ожидается ≥ 1", o.AdaptiveFloorDivisor)
	}
	return nil
}

// limitClock is time for the limiter: production uses the real clock and
// timers, unit tests inject fakes (a deterministic rate without real
// pauses). The same trick as the client's exchangeFunc: a seam with no
// dependency on interfaces outside the package.
type limitClock struct {
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error
}

// limitRealClock is the real clock: sleep is interrupted by ctx cancellation.
func limitRealClock() limitClock {
	return limitClock{
		now: func() time.Time { return time.Now() },
		sleep: func(ctx context.Context, d time.Duration) error {
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
		},
	}
}

// limitSemaphore is a counting concurrency semaphore with a mutable cap
// (the adaptive lowers the cap). Waiters are woken by closing the
// generation channel ("wake everyone"): see the package comment — no mass
// queues are expected here, and broadcast wakeup loses no events.
type limitSemaphore struct {
	mu      sync.Mutex
	cur     int // slots issued
	max     int // current cap
	waiters int // acquire waiters (to avoid pointless wakeups)
	change  chan struct{}
}

// newLimitSemaphore creates a semaphore with the max cap.
func newLimitSemaphore(max int) *limitSemaphore {
	return &limitSemaphore{max: max, change: make(chan struct{})}
}

// acquire takes a slot; when the cap is exhausted it waits for a release,
// a cap change or ctx cancellation (a cancelled caller takes no slot —
// "never lose a slot forever").
func (s *limitSemaphore) acquire(ctx context.Context) error {
	for {
		s.mu.Lock()
		if s.cur < s.max {
			s.cur++
			s.mu.Unlock()
			return nil
		}
		s.waiters++
		ch := s.change
		s.mu.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			s.mu.Lock()
			s.waiters--
			s.mu.Unlock()
			return ctx.Err()
		}

		s.mu.Lock()
		s.waiters--
		s.mu.Unlock()
	}
}

// release returns the slot and wakes waiters.
func (s *limitSemaphore) release() {
	s.mu.Lock()
	// A guard against an unpaired release: a negative cur would break the
	// cur ≤ max invariant (the cap would stop bounding).
	if s.cur > 0 {
		s.cur--
	}
	s.wake()
	s.mu.Unlock()
}

// setMax changes the cap: raising and lowering both wake waiters (on
// lowering, in-flight exchanges finish as they are — slots are not taken away).
func (s *limitSemaphore) setMax(max int) {
	s.mu.Lock()
	if max != s.max {
		s.max = max
		s.wake()
	}
	s.mu.Unlock()
}

// state returns the current cur and max (a snapshot for Stats).
func (s *limitSemaphore) state() (cur, max int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur, s.max
}

// wake closes the generation channel; the caller must hold s.mu.
func (s *limitSemaphore) wake() {
	if s.waiters > 0 {
		ch := s.change
		s.change = make(chan struct{})
		close(ch)
	}
}

// limitBucket is a QPS-rate token bucket with lazy refill: tokens are
// accrued at access time for (now−last)×rate, with no background goroutines.
//
// The choice against alternatives: a fixed "N per second" window cuts the
// natural pipeline bursts (a domain's chunk of subdomains) and adds
// jitter at window boundaries; a sliding window is fairer but needs memory
// for timestamps and gives the same rigidity. A token bucket holds the
// average max_qps rate and allows a short burst up to burst — exactly the
// "the chunk goes out at once, then the rate evens out" mode.
//
// Waiting for a token is reservation, not polling: a shortage is charged
// to the balance right away (the balance goes negative — a debt to the
// bucket), the waiter computes its own issue moment once under the mutex
// and sleeps until it with no wakeups. Polling "everyone wakes up for the
// token and recomputes the pause" would cost ~N wakeups per issued token
// (N is the waiter queue, up to concurrency=2000): at 10000 QPS that is
// millions of empty timers per second plus issue jitter (a newcomer
// steals the token from the woken one).
type limitBucket struct {
	mu       sync.Mutex
	clock    limitClock
	rate     float64 // tokens per second (the effective QPS)
	burst    float64 // current bucket capacity (follows the rate, ≤ maxBurst)
	maxBurst float64 // original capacity (the recovery cap)
	tokens   float64
	last     time.Time // moment of the last accrual
}

// newLimitBucket creates a full bucket (the token capacity): the planned
// pipeline startup wave goes out without waiting. The capacity is no
// higher than the one-second rate budget: with the typical tuning for a
// weak resolver (max_qps ≪ concurrency), a "full wave" of slots would be
// seconds of exchanges fired past the configured rate at once (DNS-10).
func newLimitBucket(qps float64, burst int, clock limitClock) *limitBucket {
	cap := math.Min(float64(burst), qps)
	if cap < 1 {
		cap = 1
	}
	return &limitBucket{
		clock:    clock,
		rate:     qps,
		burst:    cap,
		maxBurst: cap,
		tokens:   cap,
		last:     clock.now(),
	}
}

// take takes a token; with an empty bucket it waits for its own token to
// be accrued or for ctx cancellation (a token waiter does not hold the
// token forever — the semaphore is released by the caller on error). The
// issue moment is reserved before the wait (see the limitBucket comment):
// every waiter sleeps exactly until its mark and wakes once. Cancellation
// returns the reservation — a cancelled wait spends no capacity and does
// not lengthen the wait of the queue behind it.
func (b *limitBucket) take(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	wait, ok := b.reserve()
	if ok {
		return nil
	}
	if err := b.clock.sleep(ctx, wait); err != nil {
		b.unreserve()
		return err
	}
	// By the wakeup the own debt is paid off by accrual (the issue moment
	// was computed under the mutex from the balance including all previous
	// reservations): the waiter passes without re-checking the common
	// balance — it still holds the queue debts behind it, and such a check
	// would send it to the end of the common wait, collapsing the chunk into
	// one burst. The rate lowered by the adaptive during the wait gives a
	// one-time allowance to one waiter — cheaper than repeated wakeups.
	return nil
}

// reserve charges a token without waiting: on shortage the balance goes
// negative (a reservation of a future accrual); ok=false — no token, wait
// is the time until its own token accrues at the current rate (the
// reservations of everyone queued earlier are accounted — issue in arrival
// order, no token stealing by newcomers).
func (b *limitBucket) reserve() (time.Duration, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	b.tokens--
	if b.tokens >= 0 {
		return 0, true
	}
	// (−tokens)/rate seconds until its own token accrues; a minimum of 1 ns —
	// a guard against zero wait from rounding (otherwise take would spin
	// idly).
	wait := time.Duration(-b.tokens / b.rate * float64(time.Second))
	if wait < 1 {
		wait = 1
	}
	return wait, false
}

// unreserve returns the reservation of a cancelled wait: the balance grows
// by one token (no higher than the bucket capacity) — cancellation leaves
// no debt that would otherwise lengthen the queue wait behind the cancelled one.
func (b *limitBucket) unreserve() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	b.tokens = math.Min(b.burst, b.tokens+1)
}

// setRate changes the rate (the adaptive): it first accrues tokens at the
// old rate. The capacity follows the rate — no higher than the original
// (maxBurst): a burst after an idle must fit into the per-second budget
// of the active QPS, otherwise the burst accumulated before the shedding
// would fire past the lowered rate at once (DNS-10); the accrued tokens
// are trimmed to the new capacity. Rate recovery returns the capacity to the original.
func (b *limitBucket) setRate(qps float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	b.rate = qps
	b.burst = math.Min(b.maxBurst, qps)
	if b.burst < 1 {
		b.burst = 1
	}
	b.tokens = math.Min(b.tokens, b.burst)
}

// refill accrues tokens for the elapsed time; the caller holds b.mu.
func (b *limitBucket) refill() {
	if now := b.clock.now(); now.After(b.last) {
		b.tokens = math.Min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
		b.last = now
	}
}

// limitAdaptive is adaptive load shedding by the timeout share (spec 5.4)
// in the sliding window of the last AdaptiveWindow exchanges.
//
// Mechanics: a ring buffer of outcomes (timeout/not) with counters — the
// share is computed in O(1). The decision is made after every exchange,
// but only when the window is full (otherwise the first timeout at startup
// would cut the limits based on a one-event statistic).
//
// Directions (rationale): shedding is multiplicative and decisive (×0.5)
// — while the resolver times out, every exchange burns the attempt budget
// and holds a slot, a sluggish shed stretches the incident; recovery is
// more careful (×1.25) — the resolver has just been overloaded, a fast
// return gives oscillation. Between the thresholds is a deadband (hysteresis).
//
// The floor: base/DefaultLimitFloorDivisor (at least 1) — at the floor the
// pipeline keeps running while the resolver recovers.
//
// Only IsTimeout counts as a timeout: protocol/connection refusals are a
// different semantics (the resolver is not "slow" but unreachable/broken);
// spec 5.4 ties the shedding specifically to the timeout share.
type limitAdaptive struct {
	mu              sync.Mutex
	window          []bool // the outcome ring: true — a timeout
	next            int    // position of the oldest record
	filled          bool   // the window is filled completely
	timeouts, total int

	baseConc int
	minConc  int
	conc     int
	baseQPS  float64
	minQPS   float64
	qps      float64

	high, low float64
	down, up  float64
}

// newLimitAdaptive prepares the adaptive with base limits (the floor is base/div).
func newLimitAdaptive(o LimitOptions) *limitAdaptive {
	minConc := o.Concurrency / o.AdaptiveFloorDivisor
	if minConc < 1 {
		minConc = 1
	}
	minQPS := float64(o.MaxQPS) / float64(o.AdaptiveFloorDivisor)
	if minQPS < 1 {
		minQPS = 1
	}
	return &limitAdaptive{
		window:   make([]bool, o.AdaptiveWindow),
		baseConc: o.Concurrency,
		minConc:  minConc,
		conc:     o.Concurrency,
		baseQPS:  float64(o.MaxQPS),
		minQPS:   minQPS,
		qps:      float64(o.MaxQPS),
		high:     o.AdaptiveHighRatio,
		low:      o.AdaptiveLowRatio,
		down:     o.AdaptiveDownFactor,
		up:       o.AdaptiveUpFactor,
	}
}

// record adds an exchange outcome and returns the effective limits.
// Changed limits are applied by the apply callback under the same mutex
// (DNS-11): applying outside the lock let parallel window observers apply
// the values in arbitrary order — a stale larger limit applied last
// would, for one step, hand the load back to the resolver right in the
// middle of the incident. The lock order a.mu → semaphore/bucket does not
// matter on the reverse paths: those know nothing about the adaptive.
func (a *limitAdaptive) record(timedOut bool, apply func(conc int, qps float64)) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.filled {
		// The window is full: evict the oldest record.
		if a.window[a.next] {
			a.timeouts--
		}
	} else {
		a.total++
	}
	a.window[a.next] = timedOut
	if timedOut {
		a.timeouts++
	}
	a.next++
	if a.next == len(a.window) {
		a.next = 0
	}
	if !a.filled && a.total == len(a.window) {
		a.filled = true
	}
	if !a.filled {
		return
	}

	ratio := float64(a.timeouts) / float64(a.total)
	switch {
	case ratio >= a.high:
		// Shedding: down, rounded toward zero; the floor holds.
		newConc := max(a.minConc, int(float64(a.conc)*a.down))
		newQPS := math.Max(a.minQPS, a.qps*a.down)
		if newConc != a.conc || newQPS != a.qps {
			a.conc, a.qps = newConc, newQPS
			if apply != nil {
				apply(a.conc, a.qps)
			}
		}
	case ratio <= a.low && (a.conc < a.baseConc || a.qps < a.baseQPS):
		// Recovery: up with rounding up (ceil gives growth even from 1), the
		// cap is the configured limits.
		newConc := min(a.baseConc, int(math.Ceil(float64(a.conc)*a.up)))
		newQPS := math.Min(a.baseQPS, a.qps*a.up)
		if newConc != a.conc || newQPS != a.qps {
			a.conc, a.qps = newConc, newQPS
			if apply != nil {
				apply(a.conc, a.qps)
			}
		}
	}
}

// snapshot returns the current state (Stats): the effective and base
// limits, the window state.
func (a *limitAdaptive) snapshot() (conc, baseConc int, qps, baseQPS float64, windowSize, windowTimeouts int, filled bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.conc, a.baseConc, a.qps, a.baseQPS, a.total, a.timeouts, a.filled
}

// LimitStats is a snapshot of the limiter state for monitoring (metric
// export — T-077; only data here, no dependency on internal/metrics).
type LimitStats struct {
	Concurrency    int     // the effective concurrency cap (after the adaptive)
	ConcurrencyCfg int     // the configured cap (dns.concurrency)
	QPS            float64 // the effective rate (after the adaptive)
	QPSCfg         float64 // the configured rate (dns.max_qps)
	InFlight       int     // exchanges in the lower resolver right now
	WindowFilled   bool    // the adaptive window is full
	WindowSize     int     // exchanges in the window (fewer while filling)
	WindowTimeouts int     // timeouts in the window
	Exchanges      int64   // exchanges completed through the limiter
	Timeouts       int64   // of them — timeouts
	Errors         int64   // of them — other errors
	AAAAIgnored    int64   // AAAA questions answered locally (no resolver)
}

// LimitResolver is a Resolver decorator with concurrency/QPS limits,
// adaptive shedding by the timeout share and AAAA disabling (spec 5.4,
// dns.concurrency/max_qps/ipv6 from spec 5.10). Safe for concurrent use
// from goroutines.
type LimitResolver struct {
	next     Resolver
	opt      LimitOptions
	sem      *limitSemaphore
	bucket   *limitBucket
	adaptive *limitAdaptive

	exchanges   atomic.Int64
	timeouts    atomic.Int64
	errors      atomic.Int64
	aaaaIgnored atomic.Int64
}

// compile-time check: *LimitResolver — Resolver.
var _ Resolver = (*LimitResolver)(nil)

// NewLimitResolver wraps next with the limits from opts (see
// NewLimitOptions for assembling from the configuration). An error is a
// nil next or meaningless parameters (after the defaults are applied).
func NewLimitResolver(next Resolver, opts LimitOptions) (*LimitResolver, error) {
	return newLimitResolver(next, opts, limitRealClock())
}

// newLimitResolver is the common part with clock injection (tests).
func newLimitResolver(next Resolver, opts LimitOptions, clock limitClock) (*LimitResolver, error) {
	if next == nil {
		return nil, errors.New("dnsclient: NewLimitResolver: нижний резолвер не задан")
	}
	opts = opts.withDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}
	return &LimitResolver{
		next:     next,
		opt:      opts,
		sem:      newLimitSemaphore(opts.Concurrency),
		bucket:   newLimitBucket(float64(opts.MaxQPS), opts.Burst, clock),
		adaptive: newLimitAdaptive(opts),
	}, nil
}

// Exchange performs the question through the limits: a concurrency slot →
// a rate token → the lower resolver. A ctx cancellation aborts the wait in
// any queue; slots held by then are returned (defer), the token is not queued.
//
// AAAA with DisableAAAA — a local empty NOERROR (NODATA: "the name exists,
// no records of this type"): no trip to the network, no limits, no
// adaptive window. Attempts 0, TCP false, Elapsed 0 — no exchange happened.
func (r *LimitResolver) Exchange(ctx context.Context, q Question) (*Result, error) {
	if r.opt.DisableAAAA && q.Type == dns.TypeAAAA {
		r.aaaaIgnored.Add(1)
		return &Result{RCode: RCodeNOERROR}, nil
	}
	if err := r.sem.acquire(ctx); err != nil {
		return nil, err
	}
	defer r.sem.release()
	if err := r.bucket.take(ctx); err != nil {
		return nil, err
	}
	res, err := r.next.Exchange(ctx, q)
	r.observe(err)
	return res, err
}

// observe accounts for the exchange outcome: the counters and the adaptive;
// changed effective limits are applied to the semaphore and the bucket
// under the adaptive lock (see limitAdaptive.record — DNS-11).
func (r *LimitResolver) observe(err error) {
	r.exchanges.Add(1)
	timedOut := IsTimeout(err)
	if err != nil {
		if timedOut {
			r.timeouts.Add(1)
		} else {
			r.errors.Add(1)
		}
	}
	r.adaptive.record(timedOut, func(conc int, qps float64) {
		r.sem.setMax(conc)
		r.bucket.setRate(qps)
	})
}

// Stats returns a state snapshot (monitoring points for T-077: the
// effective limits, the adaptive window, the counters).
func (r *LimitResolver) Stats() LimitStats {
	conc, baseConc, qps, baseQPS, windowSize, windowTimeouts, filled := r.adaptive.snapshot()
	cur, _ := r.sem.state()
	return LimitStats{
		Concurrency:    conc,
		ConcurrencyCfg: baseConc,
		QPS:            qps,
		QPSCfg:         baseQPS,
		InFlight:       cur,
		WindowFilled:   filled,
		WindowSize:     windowSize,
		WindowTimeouts: windowTimeouts,
		Exchanges:      r.exchanges.Load(),
		Timeouts:       r.timeouts.Load(),
		Errors:         r.errors.Load(),
		AAAAIgnored:    r.aaaaIgnored.Load(),
	}
}

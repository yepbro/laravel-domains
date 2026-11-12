// Tests of the DNS stage limiter (T-035). No network: the lower resolver
// is a fake, the clock is fake (the QPS rate is deterministic without real
// pauses). Each mechanism is checked separately (the semaphore, the
// bucket, the adaptive) and the end-to-end Exchange decorator over the fake.

package dnsclient

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miekg/dns"

	"crawler/internal/config"
)

// --- helper fakes ----------------------------------------------------------

// limitFakeResolver is the lower resolver: it counts calls and
// parallelism and returns the given result/error; when hold is set, every
// exchange waits for it to close (parallelism control in tests).
type limitFakeResolver struct {
	mu          sync.Mutex
	calls       int
	inflight    int
	maxInflight int
	res         *Result
	err         error
	hold        chan struct{}
}

func (f *limitFakeResolver) Exchange(ctx context.Context, q Question) (*Result, error) {
	f.mu.Lock()
	f.calls++
	f.inflight++
	if f.inflight > f.maxInflight {
		f.maxInflight = f.inflight
	}
	hold, res, err := f.hold, f.res, f.err
	f.mu.Unlock()

	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			f.mu.Lock()
			f.inflight--
			f.mu.Unlock()
			return nil, ctx.Err()
		}
	}

	f.mu.Lock()
	f.inflight--
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if res != nil {
		return res, nil
	}
	return &Result{
		RCode:  RCodeNOERROR,
		Answer: []dns.RR{aRR(dns.Fqdn(q.Name), "192.0.2.1")},
	}, nil
}

func (f *limitFakeResolver) setErr(err error) {
	f.mu.Lock()
	f.err = err
	f.mu.Unlock()
}

func (f *limitFakeResolver) setHold(ch chan struct{}) {
	f.mu.Lock()
	f.hold = ch
	f.mu.Unlock()
}

func (f *limitFakeResolver) stats() (calls, maxInflight, inflight int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.maxInflight, f.inflight
}

// limitManualClock is a clock with no motion of its own: time moves only
// via Sleep (instant, recording the pause) and advance. The QPS rate is
// checked from the recorded pauses — without real waiting.
type limitManualClock struct {
	mu     sync.Mutex
	now    time.Time
	sleeps []time.Duration
}

func newLimitManualClock() *limitManualClock {
	return &limitManualClock{now: time.Unix(1700000000, 0)}
}

func (c *limitManualClock) clock() limitClock {
	return limitClock{now: c.Now, sleep: c.Sleep}
}

func (c *limitManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *limitManualClock) Sleep(_ context.Context, d time.Duration) error {
	c.mu.Lock()
	c.sleeps = append(c.sleeps, d)
	c.now = c.now.Add(d)
	c.mu.Unlock()
	return nil
}

func (c *limitManualClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func (c *limitManualClock) slept() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.sleeps...)
}

// limitGatedClock — Sleep hangs until the gate is closed (advancing the
// time by d) or ctx cancellation: cancellation in the token queue with no time motion.
type limitGatedClock struct {
	sleeps atomic.Int64
	mu     sync.Mutex
	now    time.Time
	gate   chan struct{}
}

func (c *limitGatedClock) clock() limitClock {
	return limitClock{now: c.Now, sleep: c.Sleep}
}

func (c *limitGatedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *limitGatedClock) Sleep(ctx context.Context, d time.Duration) error {
	c.sleeps.Add(1)
	select {
	case <-c.gate:
		// The time advanced exactly by the wait — the token accrues.
		c.mu.Lock()
		c.now = c.now.Add(d)
		c.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// limitCountingClock is a real clock with a Sleep call counter: reserving
// a token must give exactly one sleep per waiter (polling "wake up and
// recompute" would breed dozens of wakeups per chunk).
type limitCountingClock struct {
	sleeps atomic.Int64
}

func (c *limitCountingClock) clock() limitClock {
	return limitClock{
		now: time.Now,
		sleep: func(ctx context.Context, d time.Duration) error {
			c.sleeps.Add(1)
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

// newTestLimit is a limiter over a fake with a fake clock.
func newTestLimit(t *testing.T, modify func(*LimitOptions), clock limitClock, next Resolver) *LimitResolver {
	t.Helper()
	var opts LimitOptions
	if modify != nil {
		modify(&opts)
	}
	r, err := newLimitResolver(next, opts, clock)
	if err != nil {
		t.Fatalf("newLimitResolver: %v", err)
	}
	return r
}

// limitTimeoutErr is a timeout error (IsTimeout is true, like the
// client's error when attempts run out on a deadline).
var limitTimeoutErr = fakeNetErr{timeout: true}

// limitPollCond waits for a condition with a time limit (goroutines and
// channels in tests are asynchronous, but must keep up within seconds).
func limitPollCond(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("не дождались: " + msg)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// --- options ----------------------------------------------------------------

func TestLimitOptionsDefaults(t *testing.T) {
	got := LimitOptions{}.withDefaults()
	want := LimitOptions{
		Concurrency:          DefaultLimitConcurrency,
		MaxQPS:               DefaultLimitQPS,
		Burst:                DefaultLimitConcurrency,
		AdaptiveWindow:       DefaultLimitWindow,
		AdaptiveHighRatio:    DefaultLimitHighRatio,
		AdaptiveLowRatio:     DefaultLimitLowRatio,
		AdaptiveDownFactor:   DefaultLimitDownFactor,
		AdaptiveUpFactor:     DefaultLimitUpFactor,
		AdaptiveFloorDivisor: DefaultLimitFloorDivisor,
	}
	if got != want {
		t.Fatalf("withDefaults() = %+v, want %+v", got, want)
	}

	// Explicit values are not overwritten; the default burst follows
	// concurrency when not set separately.
	got = LimitOptions{Concurrency: 42, MaxQPS: 4200}.withDefaults()
	if got.Burst != 42 {
		t.Fatalf("burst = %d, want 42 (= concurrency)", got.Burst)
	}
	got = LimitOptions{Concurrency: 42, Burst: 7}.withDefaults()
	if got.Burst != 7 {
		t.Fatalf("burst = %d, want 7 (задан явно)", got.Burst)
	}
}

func TestLimitOptionsValidate(t *testing.T) {
	cases := []struct {
		name   string
		modify func(*LimitOptions)
	}{
		{"window 1", func(o *LimitOptions) { o.AdaptiveWindow = 1 }},
		{"high 0", func(o *LimitOptions) { o.AdaptiveHighRatio = 0 }},
		{"high >1", func(o *LimitOptions) { o.AdaptiveHighRatio = 1.5 }},
		{"low >= high", func(o *LimitOptions) { o.AdaptiveHighRatio = 0.1; o.AdaptiveLowRatio = 0.1 }},
		{"down 1", func(o *LimitOptions) { o.AdaptiveDownFactor = 1 }},
		{"up 1", func(o *LimitOptions) { o.AdaptiveUpFactor = 1 }},
		{"up >10", func(o *LimitOptions) { o.AdaptiveUpFactor = 11 }},
		{"floor divisor 0", func(o *LimitOptions) { o.AdaptiveFloorDivisor = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := LimitOptions{}.withDefaults()
			tc.modify(&opts)
			if err := opts.validate(); err == nil {
				t.Fatal("ожидается ошибка валидации")
			}
		})
	}
	if err := (LimitOptions{}).withDefaults().validate(); err != nil {
		t.Fatalf("дефолты валидны: %v", err)
	}
}

func TestNewLimitOptions(t *testing.T) {
	cfg := config.DNS{Concurrency: 42, MaxQPS: 4200, IPv6: false}
	if got := NewLimitOptions(cfg); !got.DisableAAAA || got.Concurrency != 42 || got.MaxQPS != 4200 {
		t.Fatalf("NewLimitOptions(ipv6=false) = %+v", got)
	}
	cfg.IPv6 = true
	if got := NewLimitOptions(cfg); got.DisableAAAA {
		t.Fatalf("NewLimitOptions(ipv6=true) = %+v: AAAA не отключаются", got)
	}
}

func TestNewLimitResolverNilNext(t *testing.T) {
	if _, err := NewLimitResolver(nil, LimitOptions{}); err == nil {
		t.Fatal("nil-резолвер: ожидается ошибка")
	}
}

// --- semaphore -------------------------------------------------------------

func TestLimitSemaphore(t *testing.T) {
	s := newLimitSemaphore(2)
	bg := context.Background()
	if err := s.acquire(bg); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := s.acquire(bg); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// The cap is exhausted: a waiting acquire is interrupted by the ctx deadline.
	ctx, cancel := context.WithTimeout(bg, 40*time.Millisecond)
	defer cancel()
	if err := s.acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquire за потолком: %v, want DeadlineExceeded", err)
	}

	// The freed slot goes to the next waiter.
	s.release()
	if err := s.acquire(bg); err != nil {
		t.Fatalf("acquire после release: %v", err)
	}
	if cur, max := s.state(); cur != 2 || max != 2 {
		t.Fatalf("state = (%d, %d), want (2, 2)", cur, max)
	}
}

func TestLimitSemaphoreShrink(t *testing.T) {
	s := newLimitSemaphore(2)
	bg := context.Background()
	_ = s.acquire(bg)
	_ = s.acquire(bg)

	// The cap is lowered below the taken level: in-flight ones are not taken away, new ones wait.
	s.setMax(1)

	ctx, cancel := context.WithCancel(bg)
	done := make(chan error, 1)
	go func() { done <- s.acquire(ctx) }()
	time.Sleep(30 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("ждущий.acquire при cur=max: вернулся до освобождения: %v", err)
	default:
	}

	s.release() // cur 1 == max 1: the waiter is still standing
	time.Sleep(30 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("ждущий.acquire при cur=max=1: вернулся: %v", err)
	default:
	}

	s.release() // cur 0 < max 1: the waiter goes through
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("acquire не прошёл после освобождения слота")
	}
	cancel()

	// Raising the cap: new slots are handed out without waiting.
	s.setMax(3)
	if err := s.acquire(bg); err != nil {
		t.Fatalf("acquire после роста потолка: %v", err)
	}
}

// --- token bucket ----------------------------------------------------------

func TestLimitBucket(t *testing.T) {
	fc := newLimitManualClock()
	b := newLimitBucket(10, 5, fc.clock()) // 10 QPS, burst 5
	bg := context.Background()

	// Burst: burst exchanges in a row without waiting.
	for i := 0; i < 5; i++ {
		if err := b.take(bg); err != nil {
			t.Fatalf("take %d: %v", i+1, err)
		}
	}
	if sleeps := fc.slept(); len(sleeps) != 0 {
		t.Fatalf("в пределах burst не ждём, паузы: %v", sleeps)
	}

	// Then the rate: 100 ms per token (1/10 s).
	for i := 0; i < 2; i++ {
		if err := b.take(bg); err != nil {
			t.Fatalf("take %d: %v", i+3, err)
		}
	}
	want := []time.Duration{100 * time.Millisecond, 100 * time.Millisecond}
	if sleeps := fc.slept(); !equalDurations(sleeps, want) {
		t.Fatalf("паузы: %v, want %v", sleeps, want)
	}

	// A long idle does not overflow the bucket: the refill is capped at burst.
	fc.advance(10 * time.Second)
	for i := 0; i < 5; i++ {
		if err := b.take(bg); err != nil {
			t.Fatalf("take после простоя %d: %v", i+1, err)
		}
	}
	if err := b.take(bg); err != nil {
		t.Fatalf("take сверх burst: %v", err)
	}
	if sleeps := fc.slept(); len(sleeps) != 3 {
		t.Fatalf("паузы: %v, want 3 × 100ms", sleeps)
	}
}

func TestLimitBucketRateChange(t *testing.T) {
	fc := newLimitManualClock()
	b := newLimitBucket(10, 2, fc.clock())
	bg := context.Background()

	_ = b.take(bg)
	_ = b.take(bg) // the bucket is empty
	b.setRate(2)   // the adaptive lowered the rate from 10 to 2 QPS — 500 ms per token
	if err := b.take(bg); err != nil {
		t.Fatal(err)
	}
	if sleeps := fc.slept(); !equalDurations(sleeps, []time.Duration{500 * time.Millisecond}) {
		t.Fatalf("паузы: %v, want [500ms]", sleeps)
	}
}

// TestLimitBucketSingleWakePerToken — reservation instead of polling: the
// waiter queue disperses by personal marks, each waiter sleeps once.
// Polling would wake everyone per token: with 9 waiters at a rate of
// 100 QPS — 45 pauses instead of 9 (DNS-01).
func TestLimitBucketSingleWakePerToken(t *testing.T) {
	cc := &limitCountingClock{}
	b := newLimitBucket(100, 1, cc.clock()) // burst 1: one goes at once, all others wait
	const total = 10
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.take(context.Background()); err != nil {
				t.Errorf("take: %v", err)
			}
		}()
	}
	wg.Wait()
	if n := cc.sleeps.Load(); n != total-1 {
		t.Fatalf("пауз %d, want %d (по одной на ждущего — шторма пробуждений нет)", n, total-1)
	}
	// The last waiter entered the queue ~right after the start: its mark is
	// 9/100 s. Real timers sleep no less than requested, so the lower
	// bound is reliable.
	if since := time.Since(start); since < 9*10*time.Millisecond {
		t.Fatalf("итоговое время %s < 90 мс — темп очереди не выдержан", since)
	}
}

// TestLimitBucketUnreserve — cancelling a waiter returns the reservation: a
// cancelled wait leaves no debt that would lengthen the queue wait behind it.
func TestLimitBucketUnreserve(t *testing.T) {
	fc := newLimitManualClock()
	b := newLimitBucket(1, 1, fc.clock())

	if _, ok := b.reserve(); !ok {
		t.Fatal("первый токен обязан пройти без ожидания")
	}
	wait, ok := b.reserve() // balance −1: waiting 1s
	if ok || wait != time.Second {
		t.Fatalf("reserve = (%v, %v), want (1s, false)", wait, ok)
	}
	b.unreserve() // the waiter is cancelled: the debt is removed
	wait, ok = b.reserve()
	if ok || wait != time.Second {
		t.Fatalf("reserve после unreserve = (%v, %v), want (1s, false) — долг не накопился", wait, ok)
	}

	// The debt of two waiters without cancellation — the third one's mark is farther: 3s, not 2s.
	b.unreserve()
	_, _ = b.reserve()
	_, _ = b.reserve()
	wait, ok = b.reserve()
	if ok || wait != 3*time.Second {
		t.Fatalf("reserve третьего в очереди = (%v, %v), want (3s, false)", wait, ok)
	}
}

// TestLimitBucketBurstCappedByQPS — the typical tuning for a weak resolver
// (max_qps ≪ concurrency): the initial capacity is no higher than a second
// of rate — a "full wave" of slots does not fire a second of budget at once (DNS-10).
func TestLimitBucketBurstCappedByQPS(t *testing.T) {
	fc := newLimitManualClock()
	b := newLimitBucket(50, 2000, fc.clock()) // rate 50/s, 2000 thrown in
	took := 0
	for i := 0; i < 60; i++ {
		if _, ok := b.reserve(); ok {
			took++
		} else {
			break
		}
	}
	if took != 50 {
		t.Fatalf("мгновенно взято %d токенов, want 50 (секунда темпа, не 2000)", took)
	}
}

// TestLimitBucketBurstFollowsRate — the capacity follows the adaptive rate:
// after a shed the burst is bounded by a second of the new rate (and the
// accrued tokens are trimmed); rate recovery returns the capacity to the
// original (DNS-10).
func TestLimitBucketBurstFollowsRate(t *testing.T) {
	fc := newLimitManualClock()
	b := newLimitBucket(1000, 100, fc.clock())

	// The bucket is full (100); the rate was dropped by two orders of magnitude.
	fc.advance(time.Second)
	b.setRate(10)
	took := 0
	for i := 0; i < 12; i++ {
		if _, ok := b.reserve(); ok {
			took++
		} else {
			break
		}
	}
	if took != 10 {
		t.Fatalf("мгновенно взято %d токенов после снижения, want 10 (секунда нового темпа)", took)
	}

	// The rate is restored — the capacity is back to the original.
	b.setRate(1000)
	fc.advance(time.Second)
	took = 0
	for i := 0; i < 102; i++ {
		if _, ok := b.reserve(); ok {
			took++
		} else {
			break
		}
	}
	if took != 100 {
		t.Fatalf("мгновенно взято %d токенов после восстановления, want 100", took)
	}
}

// --- decorator: semaphore --------------------------------------------------

func TestLimitResolverConcurrency(t *testing.T) {
	next := &limitFakeResolver{}
	hold := make(chan struct{})
	next.setHold(hold)
	r := newTestLimit(t, func(o *LimitOptions) {
		o.Concurrency = 3
		o.MaxQPS = 1000
		o.AdaptiveWindow = 100 // never fills — the adaptive stays out of the way
	}, newLimitManualClock().clock(), next)

	const total = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make([]error, total)
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
		}(i)
	}
	close(start)

	// Exactly cap exchanges got through to the resolver: 3.
	limitPollCond(t, func() bool { c, _, _ := next.stats(); return c == 3 }, "3 обмена до резолвера")
	time.Sleep(30 * time.Millisecond)
	if c, _, _ := next.stats(); c != 3 {
		t.Fatalf("обменов к резолверу: %d, want 3 (семафор)", c)
	}

	close(hold)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Exchange %d: %v", i, err)
		}
	}
	calls, maxInflight, inflight := next.stats()
	if calls != total {
		t.Fatalf("обменов: %d, want %d", calls, total)
	}
	if maxInflight != 3 {
		t.Fatalf("максимальная параллельность: %d, want 3", maxInflight)
	}
	if inflight != 0 {
		t.Fatalf("осталось в полёте: %d", inflight)
	}
	if st := r.Stats(); st.InFlight != 0 || st.Concurrency != 3 || st.ConcurrencyCfg != 3 {
		t.Fatalf("Stats: %+v", st)
	}
}

// --- decorator: QPS rate ---------------------------------------------------

func TestLimitResolverQPSPacing(t *testing.T) {
	next := &limitFakeResolver{}
	fc := newLimitManualClock()
	r := newTestLimit(t, func(o *LimitOptions) {
		o.Concurrency = 10
		o.MaxQPS = 5 // 200 ms per exchange
		o.Burst = 2
		o.AdaptiveWindow = 100
	}, fc.clock(), next)

	// Sequential exchanges: 2 at once (burst), then 200 ms each.
	q := Question{Name: "example.com", Type: dns.TypeA}
	for i := 0; i < 6; i++ {
		if _, err := r.Exchange(context.Background(), q); err != nil {
			t.Fatalf("Exchange %d: %v", i+1, err)
		}
	}
	want := []time.Duration{
		200 * time.Millisecond, 200 * time.Millisecond,
		200 * time.Millisecond, 200 * time.Millisecond,
	}
	if sleeps := fc.slept(); !equalDurations(sleeps, want) {
		t.Fatalf("паузы: %v, want %v", sleeps, want)
	}
	if st := r.Stats(); st.Exchanges != 6 {
		t.Fatalf("Stats.Exchanges = %d, want 6", st.Exchanges)
	}
}

func equalDurations(a, b []time.Duration) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- decorator: adaptive ---------------------------------------------------

func TestLimitResolverAdaptiveDecrease(t *testing.T) {
	next := &limitFakeResolver{err: limitTimeoutErr}
	r := newTestLimit(t, func(o *LimitOptions) {
		o.Concurrency = 100
		o.MaxQPS = 100
		o.AdaptiveWindow = 10
		o.AdaptiveHighRatio = 0.5
	}, newLimitManualClock().clock(), next)
	q := Question{Name: "example.com", Type: dns.TypeA}

	// The window is not full yet — the limits do not move.
	for i := 0; i < 9; i++ {
		if _, err := r.Exchange(context.Background(), q); !errors.Is(err, limitTimeoutErr) {
			t.Fatalf("Exchange %d: %v", i+1, err)
		}
	}
	if st := r.Stats(); st.Concurrency != 100 || st.WindowFilled {
		t.Fatalf("до заполнения окна: %+v", st)
	}

	// The 10th timeout fills the window (share 1.0 ≥ 0.5): the first shed ×0.5.
	if _, err := r.Exchange(context.Background(), q); !errors.Is(err, limitTimeoutErr) {
		t.Fatal(err)
	}
	if st := r.Stats(); st.Concurrency != 50 || st.QPS != 50 || !st.WindowFilled || st.WindowTimeouts != 10 {
		t.Fatalf("после заполнения: %+v", st)
	}

	// Then down to the floor: base/100 = 1 (the floor holds both concurrency and QPS).
	for i := 0; i < 10; i++ {
		if _, err := r.Exchange(context.Background(), q); !errors.Is(err, limitTimeoutErr) {
			t.Fatal(err)
		}
	}
	if st := r.Stats(); st.Concurrency != 1 || st.QPS != 1 {
		t.Fatalf("пол: %+v, want Concurrency=1 QPS=1", st)
	}
	if st := r.Stats(); st.Timeouts != 20 || st.Errors != 0 || st.Exchanges != 20 {
		t.Fatalf("счётчики: %+v", st)
	}
}

func TestLimitResolverAdaptiveRecover(t *testing.T) {
	next := &limitFakeResolver{err: limitTimeoutErr}
	r := newTestLimit(t, func(o *LimitOptions) {
		o.Concurrency = 100
		o.MaxQPS = 100
		o.AdaptiveWindow = 10
		o.AdaptiveHighRatio = 0.5
	}, newLimitManualClock().clock(), next)
	q := Question{Name: "example.com", Type: dns.TypeA}
	bg := context.Background()

	// Shed down to the floor.
	for i := 0; i < 20; i++ {
		_, _ = r.Exchange(bg, q)
	}
	if st := r.Stats(); st.Concurrency != 1 {
		t.Fatalf("после снижения: %+v", st)
	}

	// The resolver came back: the window clears of timeouts, the share
	// reaches 0 — recovery starts (×1.25 per exchange, the cap is base).
	next.setErr(nil)
	for i := 0; i < 10; i++ {
		if _, err := r.Exchange(bg, q); err != nil {
			t.Fatal(err)
		}
	}
	if st := r.Stats(); st.Concurrency != 2 || st.QPS != 1.25 {
		t.Fatalf("первый шаг восстановления: %+v, want Concurrency=2 QPS=1.25", st)
	}

	for i := 0; i < 40; i++ {
		if _, err := r.Exchange(bg, q); err != nil {
			t.Fatal(err)
		}
	}
	st := r.Stats()
	if st.Concurrency != 100 || st.QPS != 100 {
		t.Fatalf("восстановление до base: %+v", st)
	}
	if st.WindowTimeouts != 0 || st.Errors != 0 || st.Timeouts != 20 {
		t.Fatalf("счётчики: %+v", st)
	}
}

func TestLimitResolverAdaptiveIgnoresOtherErrors(t *testing.T) {
	// Non-timeout errors (connection/protocol refusal) do not trigger
	// shedding: spec 5.4 ties it to the timeout share.
	next := &limitFakeResolver{err: fakeNetErr{timeout: false}}
	r := newTestLimit(t, func(o *LimitOptions) {
		o.Concurrency = 100
		o.MaxQPS = 100
		o.AdaptiveWindow = 10
		o.AdaptiveHighRatio = 0.5
	}, newLimitManualClock().clock(), next)
	q := Question{Name: "example.com", Type: dns.TypeA}
	for i := 0; i < 20; i++ {
		_, _ = r.Exchange(context.Background(), q)
	}
	if st := r.Stats(); st.Concurrency != 100 || st.QPS != 100 || st.Errors != 20 {
		t.Fatalf("не-таймаутные ошибки: %+v", st)
	}
}

// TestLimitAdaptiveAppliesUnderLock — applying the changed limits happens
// under the adaptive mutex (DNS-11): parallel window observations cannot
// apply a stale larger limit last. The detector is TryLock from apply:
// under the write lock it must refuse.
func TestLimitAdaptiveAppliesUnderLock(t *testing.T) {
	a := newLimitAdaptive(LimitOptions{
		Concurrency: 100, MaxQPS: 100, AdaptiveWindow: 2,
		AdaptiveHighRatio: 0.5, AdaptiveLowRatio: 0.01,
		AdaptiveDownFactor: 0.5, AdaptiveUpFactor: 1.25,
		AdaptiveFloorDivisor: 100,
	}.withDefaults())

	applied, inLock := 0, true
	apply := func(conc int, qps float64) {
		applied++
		if a.mu.TryLock() {
			a.mu.Unlock()
			inLock = false // applied outside the adaptive lock
		}
	}

	a.record(true, apply) // the window is not full yet — no changes
	if applied != 0 {
		t.Fatalf("применений до заполнения окна: %d, want 0", applied)
	}
	a.record(true, apply) // the window is full of timeouts: shed ×0.5
	if applied != 1 {
		t.Fatalf("применений: %d, want 1", applied)
	}
	if !inLock {
		t.Fatal("лимиты применены вне блокировки адаптива")
	}
	if conc, _, _, _, _, _, _ := a.snapshot(); conc != 50 {
		t.Fatalf("conc = %d, want 50 (первое снижение)", conc)
	}
}

func TestLimitResolverAdaptiveShrinksLiveSemaphore(t *testing.T) {
	// The shed must take effect, not just sit in Stats: after the cap is
	// cut, no more than the new cap gets through to the resolver.
	next := &limitFakeResolver{err: limitTimeoutErr}
	r := newTestLimit(t, func(o *LimitOptions) {
		o.Concurrency = 4
		o.MaxQPS = 1000
		o.AdaptiveWindow = 10
		o.AdaptiveHighRatio = 0.5
	}, newLimitManualClock().clock(), next)
	q := Question{Name: "example.com", Type: dns.TypeA}
	bg := context.Background()

	// 10 timeouts: 4 → 2.
	for i := 0; i < 10; i++ {
		_, _ = r.Exchange(bg, q)
	}
	if st := r.Stats(); st.Concurrency != 2 {
		t.Fatalf("снижение: %+v", st)
	}

	hold := make(chan struct{})
	next.setHold(hold)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = r.Exchange(bg, q)
		}()
	}
	limitPollCond(t, func() bool { c, _, _ := next.stats(); return c == 12 }, "2 обмена после снижения")
	time.Sleep(30 * time.Millisecond)
	if c, _, _ := next.stats(); c != 12 {
		t.Fatalf("обменов: %d, want 12 (новый потолок 2)", c)
	}
	if st := r.Stats(); st.InFlight != 2 {
		t.Fatalf("InFlight = %d, want 2", st.InFlight)
	}

	close(hold)
	wg.Wait()
	if _, maxInflight, inflight := next.stats(); maxInflight != 2 || inflight != 0 {
		t.Fatalf("параллельность после снижения: max=%d inflight=%d, want 2/0", maxInflight, inflight)
	}
}

// --- decorator: AAAA disabling ---------------------------------------------

func TestLimitResolverDisableAAAA(t *testing.T) {
	next := &limitFakeResolver{}
	fc := newLimitManualClock()
	r := newTestLimit(t, func(o *LimitOptions) {
		o.Concurrency = 1
		o.MaxQPS = 1
		o.DisableAAAA = true
	}, fc.clock(), next)

	// AAAA — local: an empty NOERROR (NODATA), no resolver, no limits.
	aaaa := Question{Name: "example.com", Type: dns.TypeAAAA}
	for i := 0; i < 3; i++ {
		res, err := r.Exchange(context.Background(), aaaa)
		if err != nil {
			t.Fatalf("Exchange AAAA %d: %v", i+1, err)
		}
		if res.RCode != RCodeNOERROR || len(res.Answer) != 0 || len(res.Ns) != 0 {
			t.Fatalf("Exchange AAAA %d: %+v, want пустой NOERROR", i+1, res)
		}
		if len(res.RecordsOf(dns.TypeAAAA)) != 0 {
			t.Fatalf("записей AAAA: %d, want 0", len(res.RecordsOf(dns.TypeAAAA)))
		}
		if res.Attempts != 0 || res.TCP || res.Elapsed != 0 {
			t.Fatalf("локальный ответ без обмена: %+v", res)
		}
	}
	if c, _, _ := next.stats(); c != 0 {
		t.Fatalf("нижний резолвер вызван %d раз, want 0", c)
	}
	if sleeps := fc.slept(); len(sleeps) != 0 {
		t.Fatalf("AAAA не тратит лимиты темпа, паузы: %v", sleeps)
	}
	if st := r.Stats(); st.AAAAIgnored != 3 || st.Exchanges != 0 {
		t.Fatalf("Stats: %+v", st)
	}

	// Real questions go as usual: through the resolver and with limits.
	a := Question{Name: "example.com", Type: dns.TypeA}
	if _, err := r.Exchange(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if c, _, _ := next.stats(); c != 1 {
		t.Fatalf("обменов A: %d, want 1", c)
	}
	// A rate of 1 QPS with burst 1: the second exchange waits a second — the
	// AAAA exchanges did not dilute the bucket.
	if _, err := r.Exchange(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if sleeps := fc.slept(); !equalDurations(sleeps, []time.Duration{time.Second}) {
		t.Fatalf("паузы: %v, want [1s]", sleeps)
	}

	// Without AAAA disabling it goes to the resolver as an ordinary question.
	next2 := &limitFakeResolver{}
	r2 := newTestLimit(t, func(o *LimitOptions) { o.DisableAAAA = false }, newLimitManualClock().clock(), next2)
	if _, err := r2.Exchange(context.Background(), aaaa); err != nil {
		t.Fatal(err)
	}
	if c, _, _ := next2.stats(); c != 1 {
		t.Fatalf("обменов AAAA без отключения: %d, want 1", c)
	}
}

// --- decorator: ctx cancellation in queues ---------------------------------

func TestLimitResolverCancelWaitingSlot(t *testing.T) {
	next := &limitFakeResolver{}
	hold := make(chan struct{})
	next.setHold(hold)
	r := newTestLimit(t, func(o *LimitOptions) {
		o.Concurrency = 1
		o.MaxQPS = 1000
		o.AdaptiveWindow = 100
	}, newLimitManualClock().clock(), next)
	q := Question{Name: "example.com", Type: dns.TypeA}
	bg := context.Background()

	// The first exchange took the only slot and hangs in the resolver.
	done1 := make(chan error, 1)
	go func() {
		_, err := r.Exchange(bg, q)
		done1 <- err
	}()
	limitPollCond(t, func() bool { c, _, _ := next.stats(); return c == 1 }, "первый обмен дошёл до резолвера")

	// The second waits for a slot; the cancellation removes it from the semaphore queue.
	ctx, cancel := context.WithCancel(bg)
	done2 := make(chan error, 1)
	go func() {
		_, err := r.Exchange(ctx, q)
		done2 <- err
	}()
	time.Sleep(30 * time.Millisecond) // entered the queue
	cancel()
	select {
	case err := <-done2:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("отмена в очереди слотов: %v, want Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("отмена не вынула из очереди слотов")
	}
	if c, _, _ := next.stats(); c != 1 {
		t.Fatalf("обменов: %d, want 1 (отменённый не дошёл)", c)
	}

	// The slot is not lost: the first one finishes, the next one goes through.
	close(hold)
	if err := <-done1; err != nil {
		t.Fatal(err)
	}
	if _, err := r.Exchange(bg, q); err != nil {
		t.Fatalf("Exchange после разблокировки: %v", err)
	}
	if st := r.Stats(); st.InFlight != 0 {
		t.Fatalf("InFlight = %d, want 0", st.InFlight)
	}
}

func TestLimitResolverCancelWaitingToken(t *testing.T) {
	next := &limitFakeResolver{}
	gc := &limitGatedClock{gate: make(chan struct{})}
	r := newTestLimit(t, func(o *LimitOptions) {
		o.Concurrency = 2
		o.MaxQPS = 1
		o.Burst = 1
		o.AdaptiveWindow = 100
	}, gc.clock(), next)
	q := Question{Name: "example.com", Type: dns.TypeA}
	bg := context.Background()

	// The first exchange took the only token.
	if _, err := r.Exchange(bg, q); err != nil {
		t.Fatal(err)
	}

	// The second waits for the token (time is not moving); the cancellation interrupts the wait.
	ctx, cancel := context.WithCancel(bg)
	done := make(chan error, 1)
	go func() {
		_, err := r.Exchange(ctx, q)
		done <- err
	}()
	limitPollCond(t, func() bool { return gc.sleeps.Load() > 0 }, "второй обмен ждёт токен")
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("отмена в очереди токенов: %v, want Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("отмена не вынула из очереди токенов")
	}
	// The concurrency slot is not lost (returned on the token error).
	if st := r.Stats(); st.InFlight != 0 {
		t.Fatalf("InFlight = %d, want 0 (слот возвращён)", st.InFlight)
	}

	// Time is moving — exchanges continue, the bucket is not "broken".
	close(gc.gate)
	if _, err := r.Exchange(bg, q); err != nil {
		t.Fatalf("Exchange после открытия gate: %v", err)
	}
	if c, _, _ := next.stats(); c != 2 {
		t.Fatalf("обменов: %d, want 2", c)
	}
}

// --- decorator: end-to-end pass --------------------------------------------

func TestLimitResolverPassThrough(t *testing.T) {
	want := &Result{
		RCode:    RCodeNXDOMAIN,
		Answer:   []dns.RR{cnameRR("www.example.com.", "example.com.")},
		Ns:       []dns.RR{nsRR("example.com.", "a.iana-servers.net.")},
		Elapsed:  5 * time.Millisecond,
		Attempts: 2,
		TCP:      true,
	}
	next := &limitFakeResolver{res: want}
	r := newTestLimit(t, nil, newLimitManualClock().clock(), next)

	got, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeCNAME})
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if got.RCode != want.RCode || got.Elapsed != want.Elapsed || got.Attempts != want.Attempts ||
		got.TCP != want.TCP || len(got.Answer) != 1 || len(got.Ns) != 1 {
		t.Fatalf("результат изменён декоратором: %+v, want %+v", got, want)
	}

	// The lower resolver's error is returned as is.
	sentinel := errors.New("dnsclient: сеть отвалилась")
	next.setErr(sentinel)
	if _, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA}); !errors.Is(err, sentinel) {
		t.Fatalf("ошибка: %v, want исходная", err)
	}
}

// --- real sleep ------------------------------------------------------------

func TestLimitSleepRealClock(t *testing.T) {
	bg := context.Background()
	if err := limitRealClock().sleep(bg, time.Millisecond); err != nil {
		t.Fatalf("sleep: %v", err)
	}
	ctx, cancel := context.WithCancel(bg)
	cancel()
	if err := limitRealClock().sleep(ctx, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("sleep после отмены: %v, want Canceled", err)
	}
}

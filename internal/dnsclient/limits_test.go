// Тесты лимитёра DNS-этапа (T-035). Сети нет: нижний резолвер — фейк,
// часы — фальшивые (темп QPS детерминирован без реальных пауз). Проверяется
// каждый механизм по отдельности (семафор, ведро, адаптив) и сквозной
// декоратор Exchange над фейком.

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

// --- вспомогательные фейки ---------------------------------------------------

// limitFakeResolver — нижний резолвер: считает вызовы и параллельность,
// возвращает заданный результат/ошибку; при заданном hold каждый обмен
// ждёт его закрытия (управление параллельностью в тестах).
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

// limitManualClock — часы без собственного хода: время двигают только
// Sleep (мгновенно, с записью паузы) и advance. Темп QPS проверяется по
// записанным паузам — без реального ожидания.
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

// limitGatedClock — Sleep висит до закрытия gate (продвигая время на d)
// или отмены ctx: отмена в очереди токенов без хода времени.
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
		// Время продвинулось ровно на ожидание — токен начисляется.
		c.mu.Lock()
		c.now = c.now.Add(d)
		c.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// limitCountingClock — реальные часы со счётчиком вызовов Sleep: резервирование
// токена обязано давать ровно один sleep на ждущего (поллинг «проснуться и
// пересчитать» плодил бы десятки пробуждений на пачку).
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

// newTestLimit — лимитёр над фейком с фальшивыми часами.
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

// limitTimeoutErr — ошибка-таймаут (IsTimeout истинен, как у клиента
// при исчерпании попыток на дедлайне).
var limitTimeoutErr = fakeNetErr{timeout: true}

// limitPollCond ждёт условия с пределом по времени (горутины и каналы в
// тестах асинхронны, но обязаны успевать за секунды).
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

// --- опции --------------------------------------------------------------------

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

	// Явные значения не затираются; burst по умолчанию следует за
	// concurrency, если не задан отдельно.
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

// --- семафор ------------------------------------------------------------------

func TestLimitSemaphore(t *testing.T) {
	s := newLimitSemaphore(2)
	bg := context.Background()
	if err := s.acquire(bg); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := s.acquire(bg); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// Потолок исчерпан: ждущий acquire прерывается дедлайном ctx.
	ctx, cancel := context.WithTimeout(bg, 40*time.Millisecond)
	defer cancel()
	if err := s.acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquire за потолком: %v, want DeadlineExceeded", err)
	}

	// Освободившийся слот получает следующий ждущий.
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

	// Потолок опущен ниже занятого: летящие не отбираются, новые ждут.
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

	s.release() // cur 1 == max 1: ждущий всё ещё стоит
	time.Sleep(30 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("ждущий.acquire при cur=max=1: вернулся: %v", err)
	default:
	}

	s.release() // cur 0 < max 1: ждущий проходит
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("acquire не прошёл после освобождения слота")
	}
	cancel()

	// Рост потолка: новые слоты раздаются без ожидания.
	s.setMax(3)
	if err := s.acquire(bg); err != nil {
		t.Fatalf("acquire после роста потолка: %v", err)
	}
}

// --- токен-ведро ----------------------------------------------------------------

func TestLimitBucket(t *testing.T) {
	fc := newLimitManualClock()
	b := newLimitBucket(10, 5, fc.clock()) // 10 QPS, burst 5
	bg := context.Background()

	// Всплеск: burst обменов подряд без ожидания.
	for i := 0; i < 5; i++ {
		if err := b.take(bg); err != nil {
			t.Fatalf("take %d: %v", i+1, err)
		}
	}
	if sleeps := fc.slept(); len(sleeps) != 0 {
		t.Fatalf("в пределах burst не ждём, паузы: %v", sleeps)
	}

	// Дальше — темп: по 100 мс на токен (1/10 с).
	for i := 0; i < 2; i++ {
		if err := b.take(bg); err != nil {
			t.Fatalf("take %d: %v", i+3, err)
		}
	}
	want := []time.Duration{100 * time.Millisecond, 100 * time.Millisecond}
	if sleeps := fc.slept(); !equalDurations(sleeps, want) {
		t.Fatalf("паузы: %v, want %v", sleeps, want)
	}

	// Долгий простой не переполняет ведро: пополнение капится burst.
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
	_ = b.take(bg) // ведро пусто
	b.setRate(2)   // адаптив опустил темп с 10 до 2 QPS — ждём 500 мс на токен
	if err := b.take(bg); err != nil {
		t.Fatal(err)
	}
	if sleeps := fc.slept(); !equalDurations(sleeps, []time.Duration{500 * time.Millisecond}) {
		t.Fatalf("паузы: %v, want [500ms]", sleeps)
	}
}

// TestLimitBucketSingleWakePerToken — резервирование вместо поллинга:
// очередь ждущих расходится по персональным меткам, каждый ждущий спит
// один раз. Поллинг будил бы всех к каждому токену: при 9 ждущих и темпе
// 100 QPS — 45 пауз вместо 9 (DNS-01).
func TestLimitBucketSingleWakePerToken(t *testing.T) {
	cc := &limitCountingClock{}
	b := newLimitBucket(100, 1, cc.clock()) // burst 1: один сразу, ждут все прочие
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
	// Последний ждущий встал в очередь ~сразу после старта: его метка —
	// 9/100 с. Реальные таймеры спят не меньше запроса, нижняя граница
	// надёжна.
	if since := time.Since(start); since < 9*10*time.Millisecond {
		t.Fatalf("итоговое время %s < 90 мс — темп очереди не выдержан", since)
	}
}

// TestLimitBucketUnreserve — отмена ждущего возвращает резерв: отменённое
// ожидание не оставляет долг, удлиняющий ожидание очереди за ним.
func TestLimitBucketUnreserve(t *testing.T) {
	fc := newLimitManualClock()
	b := newLimitBucket(1, 1, fc.clock())

	if _, ok := b.reserve(); !ok {
		t.Fatal("первый токен обязан пройти без ожидания")
	}
	wait, ok := b.reserve() // баланс −1: ждём 1 с
	if ok || wait != time.Second {
		t.Fatalf("reserve = (%v, %v), want (1s, false)", wait, ok)
	}
	b.unreserve() // отмена ждущего: долг снят
	wait, ok = b.reserve()
	if ok || wait != time.Second {
		t.Fatalf("reserve после unreserve = (%v, %v), want (1s, false) — долг не накопился", wait, ok)
	}

	// Долг двух ждущих без отмены — третий метки дальше: 3 с, не 2 с.
	b.unreserve()
	_, _ = b.reserve()
	_, _ = b.reserve()
	wait, ok = b.reserve()
	if ok || wait != 3*time.Second {
		t.Fatalf("reserve третьего в очереди = (%v, %v), want (3s, false)", wait, ok)
	}
}

// TestLimitBucketBurstCappedByQPS — типовой тюнинг слабого резолвера
// (max_qps ≪ concurrency): начальная ёмкость не выше секунды темпа —
// «полная волна» слотов не уходит залпом секунд бюджета (DNS-10).
func TestLimitBucketBurstCappedByQPS(t *testing.T) {
	fc := newLimitManualClock()
	b := newLimitBucket(50, 2000, fc.clock()) // темп 50/с, затрошено 2000
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

// TestLimitBucketBurstFollowsRate — ёмкость следует за темпом адаптива:
// после снижения всплеск ограничен секундой нового темпа (и накопленные
// токены подрезаны), восстановление темпа возвращает ёмкость до исходной
// (DNS-10).
func TestLimitBucketBurstFollowsRate(t *testing.T) {
	fc := newLimitManualClock()
	b := newLimitBucket(1000, 100, fc.clock())

	// Ведро полно (100), темп упали на два порядка.
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

	// Темп восстановлен — ёмкость вернулась к исходной.
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

// --- декоратор: семафор ----------------------------------------------------------

func TestLimitResolverConcurrency(t *testing.T) {
	next := &limitFakeResolver{}
	hold := make(chan struct{})
	next.setHold(hold)
	r := newTestLimit(t, func(o *LimitOptions) {
		o.Concurrency = 3
		o.MaxQPS = 1000
		o.AdaptiveWindow = 100 // не наполняется — адаптив не мешает
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

	// К резолверу пробились ровно ceiling-обмена: 3.
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

// --- декоратор: темп QPS ---------------------------------------------------------

func TestLimitResolverQPSPacing(t *testing.T) {
	next := &limitFakeResolver{}
	fc := newLimitManualClock()
	r := newTestLimit(t, func(o *LimitOptions) {
		o.Concurrency = 10
		o.MaxQPS = 5 // 200 мс на обмен
		o.Burst = 2
		o.AdaptiveWindow = 100
	}, fc.clock(), next)

	// Последовательные обмены: 2 сразу (burst), дальше по 200 мс.
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

// --- декоратор: адаптив ----------------------------------------------------------

func TestLimitResolverAdaptiveDecrease(t *testing.T) {
	next := &limitFakeResolver{err: limitTimeoutErr}
	r := newTestLimit(t, func(o *LimitOptions) {
		o.Concurrency = 100
		o.MaxQPS = 100
		o.AdaptiveWindow = 10
		o.AdaptiveHighRatio = 0.5
	}, newLimitManualClock().clock(), next)
	q := Question{Name: "example.com", Type: dns.TypeA}

	// Окно ещё не заполнено — лимиты не двигаются.
	for i := 0; i < 9; i++ {
		if _, err := r.Exchange(context.Background(), q); !errors.Is(err, limitTimeoutErr) {
			t.Fatalf("Exchange %d: %v", i+1, err)
		}
	}
	if st := r.Stats(); st.Concurrency != 100 || st.WindowFilled {
		t.Fatalf("до заполнения окна: %+v", st)
	}

	// 10-й таймаут заполняет окно (доля 1.0 ≥ 0.5): первое снижение ×0.5.
	if _, err := r.Exchange(context.Background(), q); !errors.Is(err, limitTimeoutErr) {
		t.Fatal(err)
	}
	if st := r.Stats(); st.Concurrency != 50 || st.QPS != 50 || !st.WindowFilled || st.WindowTimeouts != 10 {
		t.Fatalf("после заполнения: %+v", st)
	}

	// Дальше — до пола: base/100 = 1 (пол держит и concurrency, и QPS).
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

	// Снизили до пола.
	for i := 0; i < 20; i++ {
		_, _ = r.Exchange(bg, q)
	}
	if st := r.Stats(); st.Concurrency != 1 {
		t.Fatalf("после снижения: %+v", st)
	}

	// Резолвер ожил: окно очищается от таймаутов, доля доходит до 0 —
	// начинается восстановление (×1.25 за обмен, потолок — base).
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
	// Не-таймаутные ошибки (отказ соединения/протокола) снижения не
	// запускают: ТЗ 5.4 привязывает его к доле таймаутов.
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

// TestLimitAdaptiveAppliesUnderLock — применение изменившихся лимитов
// происходит под мьютексом адаптива (DNS-11): параллельные наблюдения
// окна не могут наложить устаревший больший лимит последним. Детектор —
// TryLock из apply: под блокировкой записи он обязан отказывать.
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
			inLock = false // применено вне блокировки адаптива
		}
	}

	a.record(true, apply) // окно ещё не заполнено — изменений нет
	if applied != 0 {
		t.Fatalf("применений до заполнения окна: %d, want 0", applied)
	}
	a.record(true, apply) // окно заполнено таймаутами: снижение ×0.5
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
	// Снижение обязано действовать, а не только лежать в Stats: после
	// урезки потолка к резолверу пробивается не больше нового потолка.
	next := &limitFakeResolver{err: limitTimeoutErr}
	r := newTestLimit(t, func(o *LimitOptions) {
		o.Concurrency = 4
		o.MaxQPS = 1000
		o.AdaptiveWindow = 10
		o.AdaptiveHighRatio = 0.5
	}, newLimitManualClock().clock(), next)
	q := Question{Name: "example.com", Type: dns.TypeA}
	bg := context.Background()

	// 10 таймаутов: 4 → 2.
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

// --- декоратор: отключение AAAA ---------------------------------------------------

func TestLimitResolverDisableAAAA(t *testing.T) {
	next := &limitFakeResolver{}
	fc := newLimitManualClock()
	r := newTestLimit(t, func(o *LimitOptions) {
		o.Concurrency = 1
		o.MaxQPS = 1
		o.DisableAAAA = true
	}, fc.clock(), next)

	// AAAA — локально: пустой NOERROR (NODATA), без резолвера, без лимитов.
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

	// Реальные вопросы идут как обычно: через резолвер и с лимитами.
	a := Question{Name: "example.com", Type: dns.TypeA}
	if _, err := r.Exchange(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if c, _, _ := next.stats(); c != 1 {
		t.Fatalf("обменов A: %d, want 1", c)
	}
	// Темп 1 QPS при burst 1: второй обмен ждёт секунду — ведро AAAA
	// не разбавляло.
	if _, err := r.Exchange(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if sleeps := fc.slept(); !equalDurations(sleeps, []time.Duration{time.Second}) {
		t.Fatalf("паузы: %v, want [1s]", sleeps)
	}

	// Без отключения AAAA идёт к резолверу как обычный вопрос.
	next2 := &limitFakeResolver{}
	r2 := newTestLimit(t, func(o *LimitOptions) { o.DisableAAAA = false }, newLimitManualClock().clock(), next2)
	if _, err := r2.Exchange(context.Background(), aaaa); err != nil {
		t.Fatal(err)
	}
	if c, _, _ := next2.stats(); c != 1 {
		t.Fatalf("обменов AAAA без отключения: %d, want 1", c)
	}
}

// --- декоратор: отмена ctx в очередях -------------------------------------------

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

	// Первый обмен занял единственный слот и висит в резолвере.
	done1 := make(chan error, 1)
	go func() {
		_, err := r.Exchange(bg, q)
		done1 <- err
	}()
	limitPollCond(t, func() bool { c, _, _ := next.stats(); return c == 1 }, "первый обмен дошёл до резолвера")

	// Второй ждёт слот; отмена вынимает его из очереди семафора.
	ctx, cancel := context.WithCancel(bg)
	done2 := make(chan error, 1)
	go func() {
		_, err := r.Exchange(ctx, q)
		done2 <- err
	}()
	time.Sleep(30 * time.Millisecond) // встал в очередь
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

	// Слот не потерян: первый завершается, следующий проходит.
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

	// Первый обмен забрал единственный токен.
	if _, err := r.Exchange(bg, q); err != nil {
		t.Fatal(err)
	}

	// Второй ждёт токен (время не идёт); отмена прерывает ожидание.
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
	// Слот параллелизма не потерян (возвращён по ошибке токена).
	if st := r.Stats(); st.InFlight != 0 {
		t.Fatalf("InFlight = %d, want 0 (слот возвращён)", st.InFlight)
	}

	// Время пошло — обмены продолжаются, ведро не «сломано».
	close(gc.gate)
	if _, err := r.Exchange(bg, q); err != nil {
		t.Fatalf("Exchange после открытия gate: %v", err)
	}
	if c, _, _ := next.stats(); c != 2 {
		t.Fatalf("обменов: %d, want 2", c)
	}
}

// --- декоратор: сквозной проход ----------------------------------------------

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

	// Ошибка нижнего резолвера возвращается как есть.
	sentinel := errors.New("dnsclient: сеть отвалилась")
	next.setErr(sentinel)
	if _, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA}); !errors.Is(err, sentinel) {
		t.Fatalf("ошибка: %v, want исходная", err)
	}
}

// --- реальный sleep -----------------------------------------------------------

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

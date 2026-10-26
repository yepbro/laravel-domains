// Юнит-тесты TTL-кэша тяжёлых выборок (T-161) на фейковом пуле
// status_test.go: попадание в окне TTL (без повторного SQL), перечёт по
// истечении (инвалидация по времени на часах фейка), независимые секции
// («ключи» кэша), ошибки не кэшируются, копия значения не портит кэш,
// дефолты опций.
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

// cacheClock — управляемые часы кэша: тест двигает время мимо TTL без
// реальных ожиданий.
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

// countSQL — сколько раз пул видел запрос с подстрокой what.
func countSQL(calls []string, what string) int {
	n := 0
	for _, q := range calls {
		if strings.Contains(q, what) {
			n++
		}
	}
	return n
}

// TestCacheTTLHit — в окне TTL секция отдаётся без обращения к БД;
// по истечении (граница: now == expiresAt — уже просрочено) — повторная
// выборка.
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
		t.Fatalf("зоны = %+v, want одна зона crawl.test", z1)
	}
	// Внутри окна — кэш, не SQL.
	clock.tick(59 * time.Second)
	if _, err := c.ZoneStats(ctx, pool); err != nil {
		t.Fatalf("ZoneStats из кэша: %v", err)
	}
	if n := countSQL(pool.calls(), "FROM zones AS z"); n != 1 {
		t.Errorf("выборок зон = %d, want 1 (внутри TTL)", n)
	}

	// Ровно на границе TTL запись уже не действует (кэш — strictly
	// раньше expiresAt), после — повторная выборка.
	clock.tick(time.Second)
	z2, err := c.ZoneStats(ctx, pool)
	if err != nil {
		t.Fatalf("ZoneStats после TTL: %v", err)
	}
	if n := countSQL(pool.calls(), "FROM zones AS z"); n != 2 {
		t.Errorf("выборок зон = %d, want 2 (TTL истёк — перечёт)", n)
	}
	if !reflect.DeepEqual(z1, z2) {
		t.Errorf("после перечёта зоны = %+v, want как в первый раз %+v", z2, z1)
	}
}

// TestCacheSectionsIndependent — секции («ключи») независимы: своя
// запись и свой момент старта TTL — истёкшая одна не трогает другую.
func TestCacheSectionsIndependent(t *testing.T) {
	pool := newCacheTestPool()
	clock := &cacheClock{now: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)}
	c := NewCache(CacheOptions{TTL: time.Minute, Now: clock.Now})
	ctx := context.Background()

	if _, err := c.ZoneStats(ctx, pool); err != nil {
		t.Fatalf("ZoneStats: %v", err)
	}
	clock.tick(30 * time.Second) // зоны живут до 1:30, темп стартует в 0:30
	if _, err := c.Throughput(ctx, pool); err != nil {
		t.Fatalf("Throughput: %v", err)
	}
	if _, err := c.DomainStatuses(ctx, pool); err != nil {
		t.Fatalf("DomainStatuses: %v", err)
	}
	clock.tick(45 * time.Second) // 1:15: зоны просрочились, темп/статусы живы

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
		t.Errorf("выборок зон = %d, want 2 (первая + после истечения)", n)
	}
	if n := countSQL(calls, "AS done_today"); n != 1 {
		t.Errorf("выборок темпа = %d, want 1 (запись жива)", n)
	}
	// Маркер статусов — LIKE 'status (domainStatusSQL): throughputSQL
	// с T-179 тоже читает crawler_counters (счётчик суточных проверок) —
	// подстрока FROM crawler_counters больше не различает запросы.
	if n := countSQL(calls, "LIKE 'status"); n != 1 {
		t.Errorf("выборок статусов = %d, want 1 (запись жива)", n)
	}
}

// TestCacheErrorsNotCached — ошибка выборки возвращается и не
// кэшируется: следующий вызов повторяет обращение к БД.
func TestCacheErrorsNotCached(t *testing.T) {
	pool := newCacheTestPool()
	clock := &cacheClock{now: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)}
	c := NewCache(CacheOptions{TTL: time.Minute, Now: clock.Now})
	ctx := context.Background()

	boom := errors.New("бд недоступна")
	pool.setQueryErr(boom)
	if _, err := c.ZoneStats(ctx, pool); !errors.Is(err, boom) {
		t.Fatalf("ошибка = %v, want исходная ошибка БД", err)
	}
	pool.setQueryErr(nil)
	z, err := c.ZoneStats(ctx, pool)
	if err != nil {
		t.Fatalf("ZoneStats после восстановления: %v", err)
	}
	if len(z) != 1 {
		t.Errorf("зоны = %+v, want данные восстановившейся БД", z)
	}
	if n := countSQL(pool.calls(), "FROM zones AS z"); n != 2 {
		t.Errorf("выборок зон = %d, want 2 (ошибка не закэширована)", n)
	}
}

// TestCacheCloneOnReturn — возвращаемое значение — копия: мутация
// результата вызова не меняет последующие ответы кэша.
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
		t.Fatalf("ZoneStats из кэша: %v", err)
	}
	if z2[0].DomainsTotal != 100 {
		t.Errorf("мутация результата испортила кэш зон: %+v", z2[0])
	}
	s2, err := c.DomainStatuses(ctx, pool)
	if err != nil {
		t.Fatalf("DomainStatuses из кэша: %v", err)
	}
	if s2["new"] == 999999 {
		t.Errorf("мутация результата испортила кэш статусов: %v", s2)
	}
}

// TestCacheNilPool — nil-пул — ошибка выборки (как FetchZoneStats), не
// паника и не кэширование ошибки.
func TestCacheNilPool(t *testing.T) {
	clock := &cacheClock{now: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)}
	c := NewCache(CacheOptions{TTL: time.Minute, Now: clock.Now})
	if _, err := c.ZoneStats(context.Background(), nil); err == nil ||
		!strings.Contains(err.Error(), "пул не задан") {
		t.Fatalf("nil-пул = %v, want ошибка FetchZoneStats", err)
	}
}

// TestCacheOptionsDefaults — нулевой/отрицательный TTL — дефолт
// (пин-литерал 60s: сравнение с константой мутируется вместе с ней);
// заданные значения не затираются.
func TestCacheOptionsDefaults(t *testing.T) {
	if DefaultCacheTTL != time.Minute {
		t.Errorf("DefaultCacheTTL = %v, want 60s", DefaultCacheTTL)
	}
	if got := (CacheOptions{}).withDefaults().TTL; got != time.Minute {
		t.Errorf("TTL = %v, want дефолт 60s", got)
	}
	if got := (CacheOptions{TTL: -time.Second}).withDefaults().TTL; got != time.Minute {
		t.Errorf("TTL = %v после отрицательного, want дефолт", got)
	}
	if got := (CacheOptions{TTL: 5 * time.Minute}).withDefaults().TTL; got != 5*time.Minute {
		t.Errorf("TTL = %v, want заданный (дефолт не затирает)", got)
	}
}

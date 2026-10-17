package httpfetch

import (
	"context"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"
)

// Resolver — резолв имён хостов, у которых нет адресов DNS-этапа
// (ТЗ 5.5 п. 2: «для редиректов на другие хосты — резолв через тот же
// резолвер с кэшем»). «Тот же резолвер» — резолвер DNS-этапа
// (internal/dnsclient поверх Unbound): адаптер Exchange(A/AAAA) →
// адреса собирается при wiring воркера (T-053), где известен флаг
// dns.ipv6 и политика fallback-резолверов; транспорт зависит от
// интерфейса, а не от DNS-клиента.
//
// IP-литералы сюда не попадают: транспорт обрабатывает их сам (lookup).
type Resolver interface {
	// LookupNetIP возвращает IP-адреса хоста (A/AAAA в порядке ответа
	// резолвера). Возвращённый срез потребитель не изменяет.
	LookupNetIP(ctx context.Context, host string) ([]netip.Addr, error)
}

// ResolverFunc — адаптер функции под Resolver (как http.HandlerFunc):
// заглушки юнит-тестов и адаптеры резолвера этапа.
type ResolverFunc func(ctx context.Context, host string) ([]netip.Addr, error)

// LookupNetIP реализует Resolver.
func (fn ResolverFunc) LookupNetIP(ctx context.Context, host string) ([]netip.Addr, error) {
	return fn(ctx, host)
}

// SystemResolver — системный резолвер (net.DefaultResolver): дефолт
// транспорта, когда резолвер этапа не подключён (локальная разработка,
// юнит-тесты). В боевом воркере заменяется адаптером dnsclient.
var SystemResolver Resolver = ResolverFunc(func(ctx context.Context, host string) ([]netip.Addr, error) {
	return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
})

// Параметры кэша по умолчанию.
const (
	// DefaultCacheTTL — время жизни записи. Не выше типичного
	// минимального TTL публичных зон (60 с): редирект-хост повторно
	// резолвится в пределах цепочки и соседних задач, а изменение
	// адреса подхватывается минутой позже — некритично для краулера
	// (проверки периодические, §3.3). Кэш экономит поход к резолверу,
	// но не подменяет его: сам Unbound кэширует по настоящим TTL.
	DefaultCacheTTL = 60 * time.Second
	// DefaultCacheEntries — предел записей: ~4096 хостов редиректов
	// с срезом адресов — единицы мегабайт памяти воркера (§3.2).
	DefaultCacheEntries = 4096
)

// CacheOptions — параметры CachingResolver. Нулевые значения
// заменяются дефолтами. Параметры кэша в конфигурации не вынесены:
// в таблице лимитов ТЗ 5.5 их нет, это деталь транспорта.
type CacheOptions struct {
	TTL        time.Duration
	MaxEntries int
}

// cacheEntry — запись кэша: адреса и момент истечения.
type cacheEntry struct {
	addrs  []netip.Addr
	expiry time.Time
}

// CachingResolver — обёртка-кэш поверх резолвера: попадание в
// неистёкшую запись не обращается к резолверу.
//
// Кэш — отдельный слой-декоратор над Resolver, а не поле транспорта:
// T-048 резолвит хосты Location и meta refresh тем же кэшем (meta
// refresh — уровень контента, мимо диала транспорта), а транспорт
// остаётся зависящим от одного интерфейса. Ошибки не кэшируются —
// отказ резолвера повторяется реальным запросом (клиппер зацикливания
// «негативный ответ навсегда» не нужен: деградация резолвера —
// сигнал мониторинга, а не штатный режим). Одновременные промахи по
// одному хосту оба идут в резолвер (singleflight не заводился:
// редирект-хосты в пределах задачи почти не пересекаются, экономия
// редкая, а сложность координации — реальная).
type CachingResolver struct {
	inner   Resolver
	ttl     time.Duration
	max     int
	mu      sync.Mutex
	entries map[string]cacheEntry
	now     func() time.Time // для детерминизма тестов TTL
}

// NewCachingResolver оборачивает inner кэшем с TTL и пределом записей.
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

// LookupNetIP резолвит хост с кэшем: hit в пределах TTL — без обращения
// к резолверу; истёкшая запись удаляется, miss идёт в inner и
// успешно сохраняется.
func (c *CachingResolver) LookupNetIP(ctx context.Context, host string) ([]netip.Addr, error) {
	key := strings.ToLower(strings.TrimSpace(host))
	now := c.now()

	c.mu.Lock()
	if e, ok := c.entries[key]; ok {
		if now.Before(e.expiry) {
			c.mu.Unlock()
			return e.addrs, nil
		}
		delete(c.entries, key) // истекла
	}
	c.mu.Unlock()

	addrs, err := c.inner.LookupNetIP(ctx, host)
	if err != nil {
		return nil, err
	}
	c.store(key, addrs, c.now())
	return addrs, nil
}

// store сохраняет запись; при достижении предела сперва вычищаются
// истёкшие, а если место не освободилось — выселяется произвольная
// запись (порядок обхода map в Go случаен). Политике точного
// выселения (LRU) здесь взяты простота и предсказуемая память: адреса
// редирект-хостов дёшевы, вытеснение живой записи стоит одного
// резолва, а не корректности (ТЗ 3.2 — память ограничена).
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

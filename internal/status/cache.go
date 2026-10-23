// TTL-кэш тяжёлых выборок сводки (T-161, замер §12
// docs/load-measurements.md): зоны с живым агрегатом check_state, темп и
// распределение статусов — полные проходы большой domains (индексов по
// фильтрам запросов нет, T-104), на корпусе 47 млн строк — десятки
// секунд CPU postgres. Разовым потребителям (crawlerctl status —
// диагностика по нажатию) кэш не нужен: свежие данные важнее, процесс
// одноразовый — попаданий не было бы. Постоянному потребителю — фоновому
// циклу сборки снимков дашборда (dashboard.refresh, секунды) — кэш
// возвращает последний ответ, пока не истёк TTL, и ходит в БД не чаще
// раза за период: частота полных проходов перестаёт зависеть от частоты
// обновления экрана оператора.
//
// Кэш живёт в общем пакете status (постановка T-161): любой потребитель
// сводки получает тот же механизм; экземпляр — по одному на процесс
// (сервисы проекта однопроцессные, межпроцессного кэша система не
// вводит). Лендинг web внутренний/status не читает (его данные —
// кэширующие счётчики zones, дёшевы по построению, T-122) — сегодня
// кэширует только дашборд. Легкие выборки (последние находки по
// индексу laravel_domains_updated_at_idx, журнал ingest_files) не
// кэшируются: это не проходы больших таблиц, а свежесть «последних
// находок» — видимая оператору часть снимка.
//
// Ошибки выборки не кэшируются: следующий вызов повторит обращение к БД
// (деградация секции остаётся видимой — карта errors снимка, ТЗД §5.9 —
// а не застывает на «пусто»). TTL один на все секции — задан
// постановкой (1–5 мин) и настраивается полем конфигурации потребителя
// (dashboard.status_cache_ttl).
package status

import (
	"context"
	"sync"
	"time"

	"crawler/internal/db"
)

// DefaultCacheTTL — время жизни записи по умолчанию: 60 с, нижняя
// граница коридора постановки T-161 (1–5 мин). При дефолтном
// dashboard.refresh 5 с — один полный проход domains в минуту вместо
// двенадцати; пятиминутный потолок коридора оставлен оператору для
// совсем стеснённых машин (свежесть сводки деградирует до планки,
// которую постановка сочла приемлемой).
const DefaultCacheTTL = time.Minute

// Cache — TTL-кэш тяжёлых агрегатов сводки: зоны (с живым агрегатом
// check_state), темп и распределение статусов. Потокобезопасен;
// выборка выполняется под общим мьютексом (единый периодический
// потребитель — фоновый цикл дашборда — однопоточен, дубли выборок при
// конкурентных вызовах не накапливаются, сериализация проще
// single-fetch-механики на каждый ключ).
type Cache struct {
	opt CacheOptions

	mu       sync.Mutex
	zones    cacheEntry[[]ZoneStats]
	thr      cacheEntry[Throughput]
	statuses cacheEntry[map[string]int64]
}

// cacheEntry — одна кэшированная секция: значение и момент, до которого
// оно действительно.
type cacheEntry[T any] struct {
	value     T
	valid     bool
	expiresAt time.Time
}

// CacheOptions — параметры кэша; нулевые значения заменяются дефолтами
// (стиль monitor.ZoneStatsOptions).
type CacheOptions struct {
	// TTL — время жизни записи (DefaultCacheTTL).
	TTL time.Duration
	// Now — часы момента записи и проверки TTL; nil — time.Now. Тесты
	// подставляют фиксированные (стиль scheduler, T-030).
	Now func() time.Time
}

func (o CacheOptions) withDefaults() CacheOptions {
	if o.TTL <= 0 {
		o.TTL = DefaultCacheTTL
	}
	return o
}

// NewCache собирает кэш (нулевые опции — дефолты).
func NewCache(opts CacheOptions) *Cache {
	o := opts.withDefaults()
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Cache{opt: o}
}

// cached — общая механика записи: живое значение возвращается без
// выборки, просроченное/отсутствующее — выборка и запись до now+TTL.
// Ошибка выборки не кэшируется. Возвращается копия значения — мутация
// результата вызова не портит кэш следующим вызовам.
func cached[T any](c *Cache, e *cacheEntry[T], fetch func() (T, error)) (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.opt.Now()
	if e.valid && now.Before(e.expiresAt) {
		return cloneOf(e.value), nil
	}
	v, err := fetch()
	if err != nil {
		var zero T
		return zero, err
	}
	e.value, e.valid, e.expiresAt = v, true, now.Add(c.opt.TTL)
	return cloneOf(e.value), nil
}

// cloneOf — защитная копия для отдачи (срезы и карты — ссылочные типы;
// типы-значения вроде Throughput копируются присваиванием).
func cloneOf[T any](v T) T {
	switch t := any(v).(type) {
	case []ZoneStats:
		return any(append([]ZoneStats(nil), t...)).(T)
	case map[string]int64:
		m := make(map[string]int64, len(t))
		for k, val := range t {
			m[k] = val
		}
		return any(m).(T)
	default:
		return v
	}
}

// ZoneStats — сводка зон (FetchZoneStats) из кэша: живая запись — без
// обращения к БД, просроченная — выборка и перезапись.
func (c *Cache) ZoneStats(ctx context.Context, pool db.DB) ([]ZoneStats, error) {
	return cached(c, &c.zones, func() ([]ZoneStats, error) {
		return FetchZoneStats(ctx, pool)
	})
}

// Throughput — замеры темпа (FetchThroughput) из кэша.
func (c *Cache) Throughput(ctx context.Context, pool db.DB) (Throughput, error) {
	return cached(c, &c.thr, func() (Throughput, error) {
		return FetchThroughput(ctx, pool)
	})
}

// DomainStatuses — распределение статусов доменов (FetchDomainStatuses)
// из кэша.
func (c *Cache) DomainStatuses(ctx context.Context, pool db.DB) (map[string]int64, error) {
	return cached(c, &c.statuses, func() (map[string]int64, error) {
		return FetchDomainStatuses(ctx, pool)
	})
}

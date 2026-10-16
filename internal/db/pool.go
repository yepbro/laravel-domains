package db

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
)

// compile-time check: *pgxpool.Pool удовлетворяет интерфейсу DB
// (хелперы пакета принимают его напрямую).
var _ DB = (*pgxpool.Pool)(nil)

// Настройки пула по умолчанию. Значения консервативные: соединения в
// dev-окружении и проде живут на одной машине с сервисами, длинные
// простои и «вечные» соединения не нужны, а регулярная пересборка
// соединений страхует от протухания TCP-сессий и накопления состояния.
const (
	// DefaultPoolSize — лимит соединений пула, когда pool_size не задан
	// (совпадает с config.Default().Postgres.PoolSize, ТЗ 5.10).
	DefaultPoolSize = 20
	// DefaultMaxConnLifetime — переустановка соединения вне зависимости
	// от активности (pgxpool default 1h).
	DefaultMaxConnLifetime = 30 * time.Minute
	// DefaultMaxConnIdleTime — закрытие простаивающего соединения
	// (pgxpool default 30m).
	DefaultMaxConnIdleTime = 5 * time.Minute
	// DefaultHealthCheckPeriod — фоновая проверка простаивающих соединений
	// перед выдачей из пула и по расписанию (значение pgxpool).
	DefaultHealthCheckPeriod = time.Minute
	// DefaultConnectTimeout — установка одного соединения; обязан
	// укладываться в стартовый контекст сервиса.
	DefaultConnectTimeout = 10 * time.Second
)

// MaxPoolSize — верхняя граница pool_size одного сервиса: бюджет ТЗ 6.3
// (≤ 20–50 соединений на ВСЕ сервисы суммарно) не имеет смысла отдавать
// одному пулу. config.Validate ограничивает то же поле диапазоном 1..100,
// здесь граница жёстче и защищает вызовы мимо конфига.
const MaxPoolSize = 100

// Options — параметры пула подключений. Нулевые длительности и PoolSize
// заменяются значениями по умолчанию; DSN обязателен.
type Options struct {
	// DSN — строка подключения PostgreSQL (postgres.dsn конфигурации).
	DSN string
	// PoolSize — максимум соединений пула: переопределяет pool_max_conns
	// из DSN (источник истины — конфигурация сервиса). 0 — значение по
	// умолчанию (DefaultPoolSize); лимит на все сервисы — ТЗ 6.3.
	PoolSize int
	// MinPoolSize — соединения, держимые фоном (0 — ленивое открытие:
	// простой не занимает соединений). По умолчанию 0.
	MinPoolSize int
	// MaxConnLifetime — время жизни соединения. По умолчанию 30m.
	MaxConnLifetime time.Duration
	// MaxConnIdleTime — простой соединения до закрытия. По умолчанию 5m.
	MaxConnIdleTime time.Duration
	// HealthCheckPeriod — период фоновой проверки соединений. По умолчанию 1m.
	HealthCheckPeriod time.Duration
	// ConnectTimeout — установка одного соединения. По умолчанию 10s.
	ConnectTimeout time.Duration
}

// NewOptions собирает параметры пула из секции postgres конфигурации
// (T-006): dsn и pool_size; служебные настройки — значения по умолчанию
// (при необходимости задаются поверх явно).
func NewOptions(pg config.Postgres) Options {
	return Options{DSN: pg.DSN, PoolSize: pg.PoolSize}
}

// buildPoolConfig разбирает DSN и накладывает параметры: pool_size задаёт
// максимум соединений (явно перекрывает pool_max_conns из DSN), остальное —
// health-check и время жизни соединений.
func buildPoolConfig(opts Options) (*pgxpool.Config, error) {
	if strings.TrimSpace(opts.DSN) == "" {
		return nil, errors.New("db: пустой DSN PostgreSQL")
	}
	switch {
	case opts.PoolSize == 0:
		opts.PoolSize = DefaultPoolSize
	case opts.PoolSize < 0 || opts.PoolSize > MaxPoolSize:
		return nil, fmt.Errorf("db: pool_size %d вне диапазона 1..%d (бюджет ТЗ 6.3: 20–50 соединений на все сервисы суммарно)", opts.PoolSize, MaxPoolSize)
	}

	cfg, err := pgxpool.ParseConfig(opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("db: разбор DSN: %w", err)
	}

	cfg.MaxConns = int32(opts.PoolSize)
	cfg.MinConns = int32(opts.MinPoolSize)
	cfg.MaxConnLifetime = defaultIfZero(opts.MaxConnLifetime, DefaultMaxConnLifetime)
	cfg.MaxConnIdleTime = defaultIfZero(opts.MaxConnIdleTime, DefaultMaxConnIdleTime)
	cfg.HealthCheckPeriod = defaultIfZero(opts.HealthCheckPeriod, DefaultHealthCheckPeriod)
	cfg.ConnConfig.ConnectTimeout = defaultIfZero(opts.ConnectTimeout, DefaultConnectTimeout)
	return cfg, nil
}

// Open создаёт пул подключений и проверяет доступность БД одним
// подключением (fail-fast: сервис падает при старте с понятной ошибкой,
// а не при первом запросе; проверочное соединение возвращается в пул).
// Контекст ограничивает ожидание установки соединения.
func Open(ctx context.Context, opts Options) (*pgxpool.Pool, error) {
	cfg, err := buildPoolConfig(opts)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("db: создание пула: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db: PostgreSQL недоступен (%s): %w", redactDSN(opts.DSN), err)
	}
	return pool, nil
}

func defaultIfZero(v, def time.Duration) time.Duration {
	if v == 0 {
		return def
	}
	return v
}

// redactDSN скрывает пароль в сообщениях об ошибках.
func redactDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.User == nil {
		return "см. postgres.dsn (конфигурация)"
	}
	// url.UserPassword кодирует символы маски, поэтому пароль просто
	// отбрасывается: важно, чтобы он не попал в лог, а не как выглядит замена.
	u.User = url.User(u.User.Username())
	return u.String()
}

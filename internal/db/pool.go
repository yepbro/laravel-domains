package db

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
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

// AfterConnectFunc — хук настройки нового соединения пула (пробрасывается
// в pgxpool.Config.AfterConnect): выполняется один раз на каждое
// открываемое соединение (включая переоткрытия по MaxConnLifetime),
// до первого использования. Сбой хука — ошибка открытия соединения.
type AfterConnectFunc func(ctx context.Context, conn *pgx.Conn) error

// noParallelGatherSQL — отключение параллельных воркеров запросов сессии.
const noParallelGatherSQL = "SET max_parallel_workers_per_gather = 0"

// NoParallelGather — AfterConnect-хук мониторинговых сервисов (T-171):
// сессионный SET max_parallel_workers_per_gather = 0 — агрегатные проходы
// (пересчёт зон, окна сводки, backlog) не разбирают параллельные воркеры
// PostgreSQL и не забирают сразу все ядра поделимого CPU у конвейера
// краулера (боевой инцидент 23–24.10: 4 ядра, load 21–23, каждый
// параллельный агрегат занимал 3 ядра). Суммарная работа сессии та же,
// wall-время прохода растёт — мониторингу свежесть в десятки секунд не
// критична (порог алерта остановки прогресса — 5 периодов пересчёта зон),
// а воркеры и транспорт получают равные доли CPU вместо всплесков.
//
// Применимо только к пулам мониторинга (monitor, dashboard, коллекторы
// метрик): путь OLTP-воркеров (выборка/запись пачек по индексам) не
// использует параллельные планы, но фоновые seq-проходы того же сервиса
// замедляются — решения по каждому сервису принимаются осознанно.
func NoParallelGather(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, noParallelGatherSQL); err != nil {
		return fmt.Errorf("db: %s: %w", noParallelGatherSQL, err)
	}
	return nil
}

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
	// AfterConnect — настройка каждого нового соединения (nil — нет):
	// см. AfterConnectFunc. Значение применяется и к переоткрытиям
	// по MaxConnLifetime — SET-подобные настройки сессии переживают
	// пересборку соединений.
	AfterConnect AfterConnectFunc
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
	cfg.AfterConnect = opts.AfterConnect
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

// RedactDSN скрывает секреты строки подключения для сообщений об ошибках:
// пароль из userinfo и параметры password/sslpassword из query-строки
// (pgx/pgconn, как libpq, принимает их и в URL-query — parseURLQueryParams
// переносит значения в настройки подключения). Неразбораемый DSN
// заменяется сообщением fallback: не раскрываем даже структуру.
// Единая реализация для internal/db и internal/migrate (раньше это были
// две разошедшиеся копии — в одной query-пароль не вырезался).
func RedactDSN(dsn, fallback string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.User == nil {
		return fallback
	}
	// url.UserPassword кодирует символы маски, поэтому пароль просто
	// отбрасывается: важно, чтобы он не попал в лог, а не как выглядит замена.
	u.User = url.User(u.User.Username())
	// Секретные query-параметры маскируются (маска без спецсимволов —
	// url.Values.Encode экранирует их); остальной запрос (sslmode,
	// pool_max_conns, ...) сохраняется. RawQuery переписывается только
	// при реальной замене — иначе Encode() переупорядочил бы параметры.
	q := u.Query()
	redacted := false
	for _, k := range []string{"password", "sslpassword"} {
		if q.Has(k) {
			q.Set(k, "redacted")
			redacted = true
		}
	}
	if redacted {
		u.RawQuery = q.Encode()
	}
	return u.String()
}

// redactDSN — RedactDSN с fallback-сообщением пакета db.
func redactDSN(dsn string) string {
	return RedactDSN(dsn, "см. postgres.dsn (конфигурация)")
}

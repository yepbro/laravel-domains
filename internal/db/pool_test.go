// Юнит-тесты пула подключений без БД: сборка pgxpool.Config из Options
// (дефолты, приоритет pool_size над DSN, метка application_name,
// валидация), fail-fast Open на недоступном сервере, скрытие пароля в
// ошибках.
package db

import (
	"context"
	"strings"
	"testing"
	"time"

	"crawler/internal/config"
)

const testDSN = "postgres://crawler:secret@127.0.0.1:5432/crawler?sslmode=disable"

func TestNewOptions(t *testing.T) {
	opts := NewOptions(config.Postgres{DSN: testDSN, PoolSize: 7})
	if opts.DSN != testDSN || opts.PoolSize != 7 {
		t.Errorf("NewOptions: %+v, want DSN и pool_size из секции postgres", opts)
	}
}

func TestBuildPoolConfig_Defaults(t *testing.T) {
	cfg, err := buildPoolConfig(Options{DSN: testDSN})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxConns != DefaultPoolSize {
		t.Errorf("MaxConns = %d, want %d (pool_size не задан — значение по умолчанию)", cfg.MaxConns, DefaultPoolSize)
	}
	if cfg.MinConns != 0 {
		t.Errorf("MinConns = %d, want 0 (ленивое открытие соединений)", cfg.MinConns)
	}
	if cfg.MaxConnLifetime != DefaultMaxConnLifetime {
		t.Errorf("MaxConnLifetime = %s, want %s", cfg.MaxConnLifetime, DefaultMaxConnLifetime)
	}
	if cfg.MaxConnIdleTime != DefaultMaxConnIdleTime {
		t.Errorf("MaxConnIdleTime = %s, want %s", cfg.MaxConnIdleTime, DefaultMaxConnIdleTime)
	}
	if cfg.HealthCheckPeriod != DefaultHealthCheckPeriod {
		t.Errorf("HealthCheckPeriod = %s, want %s", cfg.HealthCheckPeriod, DefaultHealthCheckPeriod)
	}
	if cfg.ConnConfig.ConnectTimeout != DefaultConnectTimeout {
		t.Errorf("ConnectTimeout = %s, want %s", cfg.ConnConfig.ConnectTimeout, DefaultConnectTimeout)
	}
}

func TestBuildPoolConfig_Overrides(t *testing.T) {
	// pool_max_conns в DSN перекрывается pool_size: источник истины —
	// конфигурация сервиса (ТЗ 5.10), а не строка подключения.
	dsn := "postgres://crawler:secret@127.0.0.1:5432/crawler?sslmode=disable&pool_max_conns=7&pool_max_conn_lifetime=1h"
	cfg, err := buildPoolConfig(Options{
		DSN:             dsn,
		PoolSize:        5,
		MinPoolSize:     1,
		MaxConnLifetime: time.Hour,
		ConnectTimeout:  3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxConns != 5 {
		t.Errorf("MaxConns = %d, want 5 (pool_size важнее pool_max_conns из DSN)", cfg.MaxConns)
	}
	if cfg.MinConns != 1 {
		t.Errorf("MinConns = %d, want 1", cfg.MinConns)
	}
	if cfg.MaxConnLifetime != time.Hour {
		t.Errorf("MaxConnLifetime = %s, want 1h", cfg.MaxConnLifetime)
	}
	if cfg.ConnConfig.ConnectTimeout != 3*time.Second {
		t.Errorf("ConnectTimeout = %s, want 3s", cfg.ConnConfig.ConnectTimeout)
	}
}

func TestBuildPoolConfig_AppName(t *testing.T) {
	// Метка сервиса: AppName задаёт application_name пула; явная метка
	// в DSN важнее (T-177 — выбор оператора не перетирается).
	cfg, err := buildPoolConfig(Options{DSN: testDSN, AppName: "monitor"})
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.ConnConfig.RuntimeParams["application_name"]; got != "monitor" {
		t.Errorf("application_name = %q, want monitor", got)
	}

	dsn := "postgres://crawler@127.0.0.1:5432/crawler?sslmode=disable&application_name=ops-console"
	cfg, err = buildPoolConfig(Options{DSN: dsn, AppName: "monitor"})
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.ConnConfig.RuntimeParams["application_name"]; got != "ops-console" {
		t.Errorf("application_name = %q, want ops-console (DSN важнее AppName)", got)
	}

	// Пустой AppName — параметр не задаётся вовсе.
	cfg, err = buildPoolConfig(Options{DSN: testDSN})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := cfg.ConnConfig.RuntimeParams["application_name"]; ok {
		t.Errorf("application_name = %q без AppName, want не задан", got)
	}
}

func TestBuildPoolConfig_Validation(t *testing.T) {
	cases := []struct {
		name string
		opts Options
		want string // подстрока ожидаемой ошибки
	}{
		{"пустой DSN", Options{}, "пустой DSN"},
		{"отрицательный pool_size", Options{DSN: testDSN, PoolSize: -1}, "вне диапазона"},
		{"pool_size больше лимита", Options{DSN: testDSN, PoolSize: MaxPoolSize + 1}, "вне диапазона"},
		{"битый DSN", Options{DSN: "://not-a-dsn"}, "разбор DSN"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildPoolConfig(tc.opts)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("ошибка = %v, want содержит %q", err, tc.want)
			}
		})
	}

	// Граница допустимого проходит.
	if _, err := buildPoolConfig(Options{DSN: testDSN, PoolSize: MaxPoolSize}); err != nil {
		t.Errorf("pool_size = %d должен быть допустим: %v", MaxPoolSize, err)
	}
}

func TestOpen_UnreachableFailFast(t *testing.T) {
	// Порт без слушателя: Open обязан упасть быстро с понятной ошибкой,
	// не оставляя пул (соединение с секретом не попадает в сообщение).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := Open(ctx, Options{
		DSN:            "postgres://crawler:secret@127.0.0.1:1/crawler?sslmode=disable",
		ConnectTimeout: 500 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("Open на недоступном PostgreSQL должен вернуть ошибку")
	}
	if !strings.Contains(err.Error(), "недоступен") {
		t.Errorf("ошибка = %v, want «недоступен»", err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Error("пароль DSN попал в сообщение об ошибке")
	}
}

func TestRedactDSN(t *testing.T) {
	cases := []struct {
		name, dsn, want string
	}{
		{
			"пароль в userinfo",
			testDSN,
			"postgres://crawler@127.0.0.1:5432/crawler?sslmode=disable",
		},
		{
			"пароль в query-строке (libpq-форма pgx)",
			"postgres://crawler@127.0.0.1:5432/crawler?password=qwerty&sslmode=disable",
			"postgres://crawler@127.0.0.1:5432/crawler?password=redacted&sslmode=disable",
		},
		{
			"sslpassword в query-строке, остальные параметры целы",
			"postgres://crawler@127.0.0.1:5432/crawler?sslmode=disable&sslpassword=k&pool_max_conns=4",
			// Encode() сортирует параметры по имени
			"postgres://crawler@127.0.0.1:5432/crawler?pool_max_conns=4&sslmode=disable&sslpassword=redacted",
		},
	}
	for _, c := range cases {
		if got := redactDSN(c.dsn); got != c.want {
			t.Errorf("%s: redactDSN = %q, want %q", c.name, got, c.want)
		}
	}
	if got := redactDSN("postgres://localhost/db"); got != "см. postgres.dsn (конфигурация)" {
		t.Errorf("redactDSN без пользователя = %q", got)
	}
}

// Ошибка недоступного сервера — обёрнута, но первопричина доступна через
// errors.Is/errors.As (проверяется в TestOpen_UnreachableFailFast).

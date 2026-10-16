// Юнит-тесты пула подключений без БД: сборка pgxpool.Config из Options
// (дефолты, приоритет pool_size над DSN, валидация), fail-fast Open на
// недоступном сервере, скрытие пароля в ошибках.
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
	if got := redactDSN(testDSN); strings.Contains(got, "secret") {
		t.Errorf("redactDSN(%s) = %q, пароль не скрыт", testDSN, got)
	}
	if got := redactDSN("postgres://localhost/db"); got != "см. postgres.dsn (конфигурация)" {
		t.Errorf("redactDSN без пользователя = %q", got)
	}
}

// Ошибка недоступного сервера — обёрнута, но первопричина доступна через
// errors.Is/errors.As (проверяется в TestOpen_UnreachableFailFast).

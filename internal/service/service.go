// Package service — общий каркас Go-сервисов Domain Crawler (dev-этап).
//
// Пока сервисы проекта находятся в стадии заглушек, каждый из них поднимает
// HTTP-сервер на порту метрик с обязательными эндпоинтами /healthz и /metrics
// (ТЗ 4.2), корректно обрабатывает SIGTERM/SIGINT (graceful shutdown) и
// SIGHUP (перезагрузка правил, ТЗ 5.8 основного ТЗ). По мере реализации
// сервисов каркас дополняется их рабочей логикой.
package service

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"crawler/internal/config"
	"crawler/internal/metrics"
	"crawler/internal/migrate"
)

// Options — параметры запуска сервиса.
type Options struct {
	// Name — имя сервиса (ingest, analyzer, ...), используется в логах и метриках.
	Name string
	// MetricsAddr — адрес HTTP-сервера метрик, например ":9101".
	MetricsAddr string
	// Config — конфигурация сервиса (internal/config, ТЗ 5.10): загружается
	// в main через config.Load (приоритет: env > YAML > defaults). Если
	// задан, DSN PostgreSQL берётся из него.
	Config *config.Config
	// PGDSN — DSN PostgreSQL напрямую; имеет приоритет над Config
	// (используется утилитами, которым конфиг-файл не нужен). Если задан,
	// сервис перед стартом применяет встроенные миграции схемы
	// (internal/migrate): все долгоживущие сервисы проекта работают с БД
	// (ТЗ 6.3), healthcheck отвечает только на актуальной схеме. Пустая
	// строка вместе с пустым Config.Postgres.DSN — сервис без БД.
	PGDSN string
	// OnSIGHUP вызывается при получении SIGHUP (перезагрузка правил Laravel).
	OnSIGHUP func()
	// ExtraMux позволяет зарегистрировать дополнительные хендлеры.
	ExtraMux func(mux *http.ServeMux)
}

// EnvOr возвращает значение переменной окружения или defaultVal.
func EnvOr(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// Run блокирует до получения SIGTERM/SIGINT. Вызывается из main каждого сервиса.
func Run(opts Options) {
	if opts.Name == "" {
		log.Fatal("service: имя сервиса обязательно")
	}
	if opts.MetricsAddr == "" {
		opts.MetricsAddr = ":9090"
	}

	// Схема БД применяется до старта HTTP-сервера: healthcheck отвечает
	// только на актуальной схеме. Параллельный старт нескольких сервисов
	// безопасен (advisory-блокировки в internal/migrate).
	// DSN: явный Options.PGDSN > конфигурация (env-переопределения уже
	// применены в config.Load: CRAWLER_POSTGRES_DSN/APP_PG_DSN > postgres.dsn).
	dsn := opts.PGDSN
	if dsn == "" && opts.Config != nil {
		dsn = opts.Config.Postgres.DSN
	}
	if dsn != "" {
		if err := migrate.Up(context.Background(), dsn); err != nil {
			log.Fatalf("[%s] %v", opts.Name, err)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	// /metrics — реестр Prometheus (internal/metrics): базовые метрики
	// каркаса + коллекторы Go/runtime; метрики сервисов добавляются в него.
	registry := metrics.New(opts.Name)
	mux.Handle("/metrics", metrics.Handler(registry))
	if opts.ExtraMux != nil {
		opts.ExtraMux(mux)
	}

	srv := &http.Server{
		Addr:              opts.MetricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// SIGHUP — перезагрузка правил без рестарта (make rules-reload).
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			log.Printf("[%s] получен SIGHUP", opts.Name)
			if opts.OnSIGHUP != nil {
				opts.OnSIGHUP()
			}
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("[%s] слушаю healthz/metrics на %s (Prometheus)", opts.Name, opts.MetricsAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[%s] HTTP-сервер метрик: %v", opts.Name, err)
		}
	}()

	<-ctx.Done()
	log.Printf("[%s] останавливаюсь (graceful shutdown)", opts.Name)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

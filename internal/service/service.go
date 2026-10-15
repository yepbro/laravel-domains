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
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Options — параметры запуска сервиса.
type Options struct {
	// Name — имя сервиса (ingest, analyzer, ...), используется в логах и метриках.
	Name string
	// MetricsAddr — адрес HTTP-сервера метрик, например ":9101".
	MetricsAddr string
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

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = fmt.Fprintf(w, "# HELP crawler_service_up Сервис запущен и отвечает.\n"+
			"# TYPE crawler_service_up gauge\n"+
			"crawler_service_up{service=%q} 1\n", opts.Name)
		_, _ = fmt.Fprintf(w, "# HELP crawler_build_info Информация о сборке (заглушка dev-каркаса).\n"+
			"# TYPE crawler_build_info gauge\n"+
			"crawler_build_info{service=%q,mode=\"dev-stub\"} 1\n", opts.Name)
	})
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
		log.Printf("[%s] слушаю метрики на %s (заглушка dev-каркаса)", opts.Name, opts.MetricsAddr)
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

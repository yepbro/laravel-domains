// Тесты ограничений «злых» обработчиков mockserver (CLI-09): параметры
// из запроса ограничены сверху (аллокация по ?rate=, удержание горутины
// по ?delay=), /slow слушает отключение клиента.
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTarpitRate(t *testing.T) {
	cases := map[string]int{
		"":             512,           // дефолт — ниже 2 КБ/с основного ТЗ
		"abc":          512,           // мусор — дефолт
		"0":            512,           // неположительное — дефолт
		"-5":           512,           // отрицательное — дефолт
		"4096":         4096,          // штатное значение — как есть
		"2000000000":   tarpitMaxRate, // гигабайт/с — обрезан до предела
		"999999999999": tarpitMaxRate, // ещё больше — обрезан
	}
	for raw, want := range cases {
		if got := tarpitRate(raw); got != want {
			t.Errorf("tarpitRate(%q) = %d, want %d", raw, got, want)
		}
	}
	if tarpitMaxRate <= 0 || tarpitMaxRate > 1<<21 {
		t.Errorf("tarpitMaxRate = %d — предел должен быть мал против гигабайтных заказов", tarpitMaxRate)
	}
}

func TestSlowDelay(t *testing.T) {
	cases := map[string]time.Duration{
		"":         30 * time.Second, // дефолт сценария slow-headers
		"junk":     30 * time.Second,
		"0s":       30 * time.Second,
		"-1m":      30 * time.Second,
		"45s":      45 * time.Second,
		"5m":       5 * time.Minute,
		"9999h":    slowMaxDelay, // сутки ожидания — обрезаны
		"1000000h": slowMaxDelay,
	}
	for raw, want := range cases {
		if got := slowDelay(raw); got != want {
			t.Errorf("slowDelay(%q) = %v, want %v", raw, got, want)
		}
	}
}

// TestSlowHandlerClientGone — отключение клиента (отмена ctx запроса)
// возвращает горутину обработчика немедленно, а не по истечении delay:
// ответ не пишется (тело пусто), ServeHTTP завершается быстро.
func TestSlowHandlerClientGone(t *testing.T) {
	mux := http.NewServeMux()
	registerRoutes(mux)

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/slow?delay=30m", nil)
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		mux.ServeHTTP(rec, req)
		close(done)
	}()
	cancel()

	select {
	case <-done:
		if rec.Body.Len() != 0 {
			t.Errorf("после отмены клиентом тело = %q, хочу ответ не писался", rec.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("обработчик /slow держит горутину после отключения клиента (ctx не учтён)")
	}
}

package render

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"crawler/internal/config"
)

// TestMain глушит служебный лог пакета (Warn пула при повторах) в выводе
// go test.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

// testBrowser — браузер без запуска процесса: только опции и счётчик
// страниц (юнит-тесты пула и классификации; Close/Alive безопасны на нулях).
func testBrowser(contexts int) *Browser {
	return &Browser{opts: Options{
		Contexts:            contexts,
		Timeout:             5 * time.Second,
		RestartEveryPages:   100,
		SessionStartTimeout: time.Second,
	}.withDefaults()}
}

func TestNewOptions(t *testing.T) {
	cfg := config.Default()
	cfg.Render = config.Render{
		RemoteURL:          "ws://chromium:9222",
		Contexts:           3,
		Timeout:            config.Duration(25 * time.Second),
		RestartEveryPages:  500,
		RestartMemoryBytes: config.ByteSize(1536 << 20),
	}
	cfg.HTTP.UserAgent = "Crawler/1.0 (+https://example.example; admin@example.example)"

	got := NewOptions(*cfg)
	want := Options{
		RemoteURL:         "ws://chromium:9222",
		Contexts:          3,
		Timeout:           25 * time.Second,
		RestartEveryPages: 500,
		MemoryThreshold:   1536 << 20,
		UserAgent:         "Crawler/1.0 (+https://example.example; admin@example.example)",
		MaxDOMBytes:       cfg.Content.MaxStoreBytes.B(), // R-02: потолок DOM — лимит хранилища
	}
	if got != want {
		t.Fatalf("NewOptions:\n got %+v\nwant %+v", got, want)
	}
}

func TestOptionsWithDefaults(t *testing.T) {
	got := Options{}.withDefaults()
	if got.DialTimeout != 10*time.Second {
		t.Errorf("DialTimeout = %s, хочу 10s", got.DialTimeout)
	}
	if got.SessionStartTimeout != 15*time.Second {
		t.Errorf("SessionStartTimeout = %s, хочу 15s", got.SessionStartTimeout)
	}
	// Прочие поля — как заданы: скрытые дефолты не маскируют конфигурацию.
	if got.Contexts != 0 || got.Timeout != 0 {
		t.Errorf("withDefaults затёр пустые поля: %+v", got)
	}
}

func TestOptionsValidate(t *testing.T) {
	valid := Options{
		RemoteURL:         "ws://chromium:9222",
		Contexts:          2,
		Timeout:           25 * time.Second,
		RestartEveryPages: 500,
	}
	if err := valid.validate(); err != nil {
		t.Fatalf("валидные опции отклонены: %v", err)
	}
	// RemoteURL не обязателен: пусто — локальный режим.
	local := valid
	local.RemoteURL = ""
	if err := local.validate(); err != nil {
		t.Fatalf("локальный режим отклонён: %v", err)
	}

	cases := []struct {
		name  string
		opts  Options
		field string
	}{
		{"схема remote_url", Options{RemoteURL: "ftp://x:9222", Contexts: 2, Timeout: time.Second, RestartEveryPages: 1}, "remote_url"},
		{"remote_url без хоста", Options{RemoteURL: "ws://", Contexts: 2, Timeout: time.Second, RestartEveryPages: 1}, "remote_url"},
		{"contexts 0", Options{Contexts: 0, Timeout: time.Second, RestartEveryPages: 1}, "contexts"},
		{"contexts 17", Options{Contexts: 17, Timeout: time.Second, RestartEveryPages: 1}, "contexts"},
		{"timeout 0", Options{Contexts: 2, Timeout: 0, RestartEveryPages: 1}, "timeout"},
		{"timeout отрицательный", Options{Contexts: 2, Timeout: -time.Second, RestartEveryPages: 1}, "timeout"},
		{"restart_every_pages 0", Options{Contexts: 2, Timeout: time.Second, RestartEveryPages: 0}, "restart_every_pages"},
		{"memory_threshold отрицательный", Options{Contexts: 2, Timeout: time.Second, RestartEveryPages: 1, MemoryThreshold: -1}, "memory_threshold"},
		{"memory_threshold ниже минимума", Options{Contexts: 2, Timeout: time.Second, RestartEveryPages: 1, MemoryThreshold: 1 << 20}, "memory_threshold"},
		{"memory_threshold выше максимума", Options{Contexts: 2, Timeout: time.Second, RestartEveryPages: 1, MemoryThreshold: 17 << 30}, "memory_threshold"},
		{"max_dom_bytes отрицательный", Options{Contexts: 2, Timeout: time.Second, RestartEveryPages: 1, MaxDOMBytes: -1}, "max_dom_bytes"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opts.validate()
			if err == nil {
				t.Fatalf("ожидана ошибка, её нет")
			}
			if !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("ошибка не называет поле %s: %v", tc.field, err)
			}
		})
	}
	// 0 — контроль памяти отключён (remote-режим и малые машины), не ошибка.
	off := Options{Contexts: 2, Timeout: time.Second, RestartEveryPages: 1}
	if err := off.validate(); err != nil {
		t.Fatalf("нулевой порог памяти должен быть допустим: %v", err)
	}
}

// TestValidateTaskURL — схема задачи рендера (R-09): открываются только
// http/https; file:/data: браузер принял бы, а съём DOM унёс бы содержимое
// локального ресурса в хранилище контента.
func TestValidateTaskURL(t *testing.T) {
	for _, u := range []string{
		"http://example.com/", "https://example.com/deep/path?q=1",
		"https://spa.example:8443/",
	} {
		if err := validateTaskURL(u); err != nil {
			t.Errorf("%s: неожиданная ошибка: %v", u, err)
		}
	}
	for _, u := range []string{
		"file:///etc/passwd", "data:text/html,<html>", "ftp://example.com/",
		"chrome://version", "about:blank", "", "not a url", "/relative/path",
	} {
		err := validateTaskURL(u)
		if err == nil {
			t.Errorf("%s: ожидалась ошибка схемы", u)
			continue
		}
		if ClassOf(err) != ClassTask {
			t.Errorf("%s: класс %v, хочу task (повторная доставка уровня очереди)", u, ClassOf(err))
		}
	}
}

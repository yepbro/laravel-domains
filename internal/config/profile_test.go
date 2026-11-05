// Тест профиля развёртывания воркера B (T-240, ТЗР этапа 35 §4.5):
// боевые YAML-профили подключаются переменной CRAWLER_CONFIG (по образцу
// config.render.yaml — сервис render профиля runtime) и обязаны проходить
// сборку конфигурации целиком: строгий YAML (KnownFields) → окружение →
// нормализация → Validate. Профиль воркера B —
// deploy/docker/config.render-fallback.yaml: движок chromium, собственный
// headless-shell, включённый фолбэк-контур, потолок темпа
// render.fallback.worker_rate (ФТ-13); стрим потребления q:render-fallback
// и группа — переменная сборки RENDER_WORKER_STREAM (cmd/render-worker),
// не поля конфигурации (ТЗ §4.1 п.3, T-236).
package config

import (
	"path/filepath"
	"testing"
)

// TestRenderFallbackProfileValid — профиль воркера B валиден и выражает
// сборку ТЗ §4.5 п.1. DSN приходит окружением, как в развертывании
// (APP_PG_DSN из compose.yaml; обязателен — Validate).
func TestRenderFallbackProfileValid(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "docker", "config.render-fallback.yaml")
	// Окружение детерминировано: кроме DSN — пусто, значения процесса
	// теста профиль не перекрывают (как loadFrom).
	cfg, err := loadPath(path, func(key string) string {
		if key == "CRAWLER_POSTGRES_DSN" {
			return "postgres://crawler:test@127.0.0.1:5432/crawler?sslmode=disable"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("loadPath(config.render-fallback.yaml): %v (профиль обязан проходить валидацию)", err)
	}
	if cfg.Render.Engine != EngineChromium {
		t.Errorf("render.engine = %q, хочу %q (воркер B — всегда chromium, ТЗ §4.5; рекурсивный фолбэк запрещён, ФТ-10)",
			cfg.Render.Engine, EngineChromium)
	}
	if cfg.Render.RemoteURL != "" {
		t.Errorf("render.remote_url = %q, хочу пусто (собственный процесс headless-shell — сегодняшний render-образ без изменений)",
			cfg.Render.RemoteURL)
	}
	if cfg.Render.RestartMemoryBytes == 0 {
		t.Errorf("render.restart_memory_bytes = 0: собственный процесс браузера — RSS-контроль T-065 обязан работать (как у основного render)")
	}
	if !cfg.Render.Fallback.Enabled {
		t.Error("render.fallback.enabled = false: профиль воркера B документирует включённый фолбэк-контур (сборке флаг безразличен — publishFallback не публикует со стрима фолбэка)")
	}
	if cfg.Render.Fallback.WorkerRate != 100 {
		t.Errorf("render.fallback.worker_rate = %d, хочу 100 (дефолт потолка темпа, T-233 — калибруется стендом)", cfg.Render.Fallback.WorkerRate)
	}
}

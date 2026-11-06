// Тесты профилей развёртывания (T-240, ТЗР этапа 35 §4.5): боевые
// YAML-профили подключаются переменной CRAWLER_CONFIG (по образцу
// config.render.yaml — сервис render профиля runtime) и обязаны проходить
// сборку конфигурации целиком: строгий YAML (KnownFields) → окружение →
// нормализация → Validate. Профиль воркера B —
// deploy/docker/config.render-fallback.yaml: движок chromium, собственный
// headless-shell, включённый фолбэк-контур, потолок темпа
// render.fallback.worker_rate (ФТ-13); стрим потребления q:render-fallback
// и группа — переменная сборки RENDER_WORKER_STREAM (cmd/render-worker),
// не поля конфигурации (ТЗ §4.1 п.3, T-236). Профиль dev —
// deploy/docker/config.dev.yaml: документированное переключение движка
// (ФТ-16) правкой engine + remote_url обязано проходить валидацию.
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

// TestDevProfileEngineSwitch — документированное переключение движка
// dev-воркера (ФТ-16, S-P1-2): правка engine + remote_url в
// config.dev.yaml обязана проходить Validate. Профиль держит
// render.restart_memory_bytes: 0 (браузер в dev всегда внешний, RSS
// чужого процесса недоступен); без ключа профиль наследовал бы боевой
// дефолт 1536MiB — и валидация отвергала бы moli/lightpanda ещё до
// старта воркера, как и crawlerctl fixtures export/import/snapshot,
// читающие тот же конфиг.
func TestDevProfileEngineSwitch(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "docker", "config.dev.yaml")
	for _, engine := range []struct{ name, remoteURL string }{
		{EngineMoli, "ws://172.28.100.17:9222"},
		{EngineLightpanda, "ws://172.28.100.16:9222"},
	} {
		cfg, err := loadPath(path, func(key string) string {
			switch key {
			case "CRAWLER_RENDER_ENGINE":
				return engine.name
			case "CRAWLER_RENDER_REMOTE_URL":
				return engine.remoteURL
			}
			return ""
		})
		if err != nil {
			t.Fatalf("loadPath(config.dev.yaml, engine=%s): %v (переключение движка — engine+remote_url, порог в профиле уже 0)", engine.name, err)
		}
		if cfg.Render.RestartMemoryBytes != 0 {
			t.Errorf("engine=%s: render.restart_memory_bytes = %s, хочу 0 (внешний браузер dev-стека)", engine.name, cfg.Render.RestartMemoryBytes)
		}
	}
}

// Tests of the deployment profiles (T-240, the render spec stage 35
// §4.5): the production YAML profiles are connected by the CRAWLER_CONFIG
// variable (following the pattern of config.render.yaml — the render
// service of the runtime profile) and must pass the whole configuration
// build: strict YAML (KnownFields) → environment → normalization →
// Validate. The worker B profile is
// deploy/docker/config.render-fallback.yaml: the chromium engine, its own
// headless-shell, the enabled fallback circuit, the rate ceiling
// render.fallback.worker_rate (FR-13); the consumption stream
// q:render-fallback and the group are the RENDER_WORKER_STREAM build
// variable (cmd/render-worker), not configuration fields (the spec §4.1
// item 3, T-236). The dev profile is deploy/docker/config.dev.yaml: the
// documented engine switch (FR-16) by editing engine + remote_url must
// pass validation.
package config

import (
	"path/filepath"
	"testing"
)

// TestRenderFallbackProfileValid — the worker B profile is valid and
// expresses the build of the spec §4.5 item 1. The DSN comes from the
// environment, as in deployment (APP_PG_DSN from compose.yaml; required —
// Validate).
func TestRenderFallbackProfileValid(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "docker", "config.render-fallback.yaml")
	// The environment is deterministic: except the DSN — empty; the
	// values of the test process do not override the profile (like
	// loadFrom).
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

// TestDevProfileEngineSwitch — the documented engine switch of the dev
// worker (FR-16, S-P1-2): editing engine + remote_url in config.dev.yaml
// must pass Validate. The profile keeps render.restart_memory_bytes: 0
// (the browser in dev is always external, the RSS of a foreign process is
// unavailable); without the key the profile would inherit the production
// default of 1536MiB — and validation would reject moli/lightpanda even
// before the worker starts, as it would crawlerctl fixtures
// export/import/snapshot reading the same config.
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

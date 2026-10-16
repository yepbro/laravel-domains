// Конфигурация сервисов в dev-окружении (T-006): deploy/docker/config.dev.yaml,
// смонтированный в контейнеры как /etc/crawler/config.yaml (CRAWLER_CONFIG),
// вместе с переопределениями из compose (APP_PG_DSN, CRAWLER_HTTP_USER_AGENT).
package integration

import (
	"os"
	"strings"
	"testing"

	"crawler/internal/config"
)

func TestConfigDevEnvironment(t *testing.T) {
	skipUnlessIntegration(t)

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	// DSN приходит из устаревшей APP_PG_DSN (compose), а не из YAML.
	if want := os.Getenv("APP_PG_DSN"); want == "" || cfg.Postgres.DSN != want {
		t.Errorf("postgres.dsn: ожидается DSN из APP_PG_DSN (%q), получен %q", want, cfg.Postgres.DSN)
	}
	// Остальные адреса — из config.dev.yaml (статические IP crawler_net).
	if cfg.Redis.Addr != "172.28.100.3:6379" {
		t.Errorf("redis.addr: ожидается 172.28.100.3:6379, получено %q", cfg.Redis.Addr)
	}
	if got := strings.Join(cfg.DNS.Resolvers, ";"); got != "172.28.100.10:53" {
		t.Errorf("dns.resolvers: ожидается unbound 172.28.100.10:53, получено %q", got)
	}
	if cfg.DNS.IPv6 {
		t.Errorf("dns.ipv6: в docker-сети dev-окружения IPv6 выключен (config.dev.yaml)")
	}
	if len(cfg.DNS.FallbackResolvers) != 0 {
		t.Errorf("dns.fallback_resolvers: dev изолирован от интернета, ожидается пустой список, получено %v", cfg.DNS.FallbackResolvers)
	}
	if cfg.Render.RemoteURL != "ws://172.28.100.14:9222" {
		t.Errorf("render.remote_url: ожидается chromium ws://172.28.100.14:9222, получено %q", cfg.Render.RemoteURL)
	}
	if ua := cfg.HTTP.UserAgent; ua == "" || !strings.Contains(ua, "contact") {
		t.Errorf("http.user_agent: ожидается честный UA с контактами, получено %q", ua)
	}

	// Сам файл compose-окружения валиден и без переопределений окружения
	// (DSN в нём по умолчанию заменяется на APP_PG_DSN выше).
	fileCfg, err := config.LoadFile("/etc/crawler/config.yaml")
	if err != nil {
		t.Fatalf("config.LoadFile(/etc/crawler/config.yaml): %v", err)
	}
	if fileCfg.Redis.Addr != "172.28.100.3:6379" || fileCfg.Monitor.Listen != ":9100" {
		t.Errorf("config.dev.yaml: неожиданные значения: redis=%q monitor=%q", fileCfg.Redis.Addr, fileCfg.Monitor.Listen)
	}

	// Пример из репозитория валиден целиком (config.example.yaml, T-006).
	if _, err := config.LoadFile("/src/config.example.yaml"); err != nil {
		t.Errorf("config.example.yaml должен проходить валидацию: %v", err)
	}
}

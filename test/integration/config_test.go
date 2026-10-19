// Конфигурация сервисов в dev-окружении (T-006): deploy/docker/config.dev.yaml,
// смонтированный в контейнеры как /etc/crawler/config.yaml (CRAWLER_CONFIG),
// вместе с переопределениями из compose (APP_PG_DSN, CRAWLER_HTTP_USER_AGENT).
package integration

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"crawler/internal/config"
	"crawler/internal/dashboard"
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

	// Секция dashboard (T-131): listen — docker-сеть (наружу публикует
	// compose-сервис dashboard на 127.0.0.1:DASHBOARD_PORT хоста), креды —
	// dev admin/admin (docs/dev-environment.md), пробы — имена
	// compose-сервисов с их METRICS_PORT (METRICS_ADDR=:91xx в compose —
	// тот же адрес, что скрейпит Prometheus).
	if fileCfg.Dashboard.Listen != ":8090" {
		t.Errorf("dashboard.listen: ожидается :8090 (интерфейсы docker-сети), получено %q", fileCfg.Dashboard.Listen)
	}
	if fileCfg.Dashboard.Username != "admin" {
		t.Errorf("dashboard.username: dev-креда admin, получено %q", fileCfg.Dashboard.Username)
	}
	if creds, err := dashboard.ParseCredentials(fileCfg); err != nil {
		t.Errorf("dashboard.password_hash (формат ТЗД §7.2): %v", err)
	} else if !creds.Verify("admin") {
		t.Errorf("dashboard.password_hash: dev-пароль admin не проходит сверку (перегенерируйте хэш, docs/dev-environment.md)")
	}
	wantProbes := map[string]string{
		"ingest":        "http://ingest:9101",
		"scheduler":     "http://scheduler:9102",
		"dns-worker":    "http://dns-worker:9103",
		"http-worker":   "http://http-worker:9104",
		"render-worker": "http://render-worker:9105",
		"analyzer":      "http://analyzer:9106",
		"monitor":       "http://monitor:9100",
	}
	if !reflect.DeepEqual(fileCfg.Dashboard.Probes, wantProbes) {
		t.Errorf("dashboard.probes: ожидается карта compose-сервисов с их METRICS_PORT %v, получено %v", wantProbes, fileCfg.Dashboard.Probes)
	}

	// Пример из репозитория валиден целиком (config.example.yaml, T-006).
	if _, err := config.LoadFile("/src/config.example.yaml"); err != nil {
		t.Errorf("config.example.yaml должен проходить валидацию: %v", err)
	}
}

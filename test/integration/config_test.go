// Service configuration in the dev environment (T-006): deploy/docker/config.dev.yaml
// mounted into containers as /etc/crawler/config.yaml (CRAWLER_CONFIG),
// together with overrides from compose (APP_PG_DSN, CRAWLER_HTTP_USER_AGENT).
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

	// The DSN comes from the deprecated APP_PG_DSN (compose), not from YAML.
	if want := os.Getenv("APP_PG_DSN"); want == "" || cfg.Postgres.DSN != want {
		t.Errorf("postgres.dsn: the DSN from APP_PG_DSN expected (%q), got %q", want, cfg.Postgres.DSN)
	}
	// The remaining addresses come from config.dev.yaml (static crawler_net IPs).
	if cfg.Redis.Addr != "172.28.100.3:6379" {
		t.Errorf("redis.addr: 172.28.100.3:6379 expected, got %q", cfg.Redis.Addr)
	}
	if got := strings.Join(cfg.DNS.Resolvers, ";"); got != "172.28.100.10:53" {
		t.Errorf("dns.resolvers: unbound 172.28.100.10:53 expected, got %q", got)
	}
	if cfg.DNS.IPv6 {
		t.Errorf("dns.ipv6: IPv6 disabled in the dev environment's docker network (config.dev.yaml)")
	}
	if len(cfg.DNS.FallbackResolvers) != 0 {
		t.Errorf("dns.fallback_resolvers: the dev is isolated from the internet, an empty list expected, got %v", cfg.DNS.FallbackResolvers)
	}
	if cfg.Render.RemoteURL != "ws://172.28.100.14:9222" {
		t.Errorf("render.remote_url: chromium ws://172.28.100.14:9222 expected, got %q", cfg.Render.RemoteURL)
	}
	if ua := cfg.HTTP.UserAgent; ua == "" || !strings.Contains(ua, "contact") {
		t.Errorf("http.user_agent: an honest UA with contacts expected, got %q", ua)
	}

	// The compose environment file itself is valid even without environment
	// overrides (its default DSN is replaced by APP_PG_DSN above).
	fileCfg, err := config.LoadFile("/etc/crawler/config.yaml")
	if err != nil {
		t.Fatalf("config.LoadFile(/etc/crawler/config.yaml): %v", err)
	}
	if fileCfg.Redis.Addr != "172.28.100.3:6379" || fileCfg.Monitor.Listen != ":9100" {
		t.Errorf("config.dev.yaml: unexpected values: redis=%q monitor=%q", fileCfg.Redis.Addr, fileCfg.Monitor.Listen)
	}

	// The dashboard section (T-131): listen is the docker network (the compose
	// dashboard service publishes it externally at 127.0.0.1:DASHBOARD_PORT on
	// the host), credentials are dev admin/admin (docs/dev-environment.md),
	// probes are compose service names with their METRICS_PORT
	// (METRICS_ADDR=:91xx in compose — the same address Prometheus scrapes).
	if fileCfg.Dashboard.Listen != ":8090" {
		t.Errorf("dashboard.listen: :8090 expected (the docker network interfaces), got %q", fileCfg.Dashboard.Listen)
	}
	if fileCfg.Dashboard.Username != "admin" {
		t.Errorf("dashboard.username: the dev credential admin, got %q", fileCfg.Dashboard.Username)
	}
	if creds, err := dashboard.ParseCredentials(fileCfg); err != nil {
		t.Errorf("dashboard.password_hash (the dashboard spec §7.2 format): %v", err)
	} else if !creds.Verify("admin") {
		t.Errorf("dashboard.password_hash: the dev password admin fails the verification (regenerate the hash, docs/dev-environment.md)")
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
		t.Errorf("dashboard.probes: a map of the compose services with their METRICS_PORT expected %v, got %v", wantProbes, fileCfg.Dashboard.Probes)
	}

	// The repository example is valid as a whole (config.example.yaml, T-006).
	if _, err := config.LoadFile("/src/config.example.yaml"); err != nil {
		t.Errorf("config.example.yaml must pass validation: %v", err)
	}
}

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// loadFrom собирает конфигурацию из YAML-текста и карты окружения
// (в тестах окружение детерминировано и не зависит от процесса).
func loadFrom(t *testing.T, yamlText string, env map[string]string) (*Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(yamlText), 0o644); err != nil {
		t.Fatal(err)
	}
	return loadPath(path, func(k string) string { return env[k] })
}

func mustLoad(t *testing.T, yamlText string, env map[string]string) *Config {
	t.Helper()
	cfg, err := loadFrom(t, yamlText, env)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	return cfg
}

// validDSN — окружение с единственным обязательным значением.
func validDSN() map[string]string {
	return map[string]string{"CRAWLER_POSTGRES_DSN": "postgres://u:p@127.0.0.1:5432/crawler"}
}

func TestDefault(t *testing.T) {
	c := Default()
	if c.Postgres.DSN != "" {
		t.Errorf("postgres.dsn по умолчанию пуст (обязателен из YAML/env), получен %q", c.Postgres.DSN)
	}
	checks := []struct {
		name string
		got  any
		want any
	}{
		{"postgres.pool_size", c.Postgres.PoolSize, 20},
		{"redis.addr", c.Redis.Addr, "127.0.0.1:6379"},
		{"ingest.dir", c.Ingest.Dir, "/data/incoming"},
		{"ingest.batch_size", c.Ingest.BatchSize, 50000},
		{"ingest.stable_seconds", c.Ingest.StableSeconds, 10},
		{"scheduler.recheck_interval", c.Scheduler.RecheckInterval.D(), 4380 * time.Hour},
		{"scheduler.batch_size", c.Scheduler.BatchSize, 5000},
		{"scheduler.queue_high_watermark", c.Scheduler.QueueHighWatermark, 100000},
		{"scheduler.lease", c.Scheduler.Lease.D(), 30 * time.Minute},
		{"scheduler.temp_error_retry_after", c.Scheduler.TempErrorRetryAfter.D(), 24 * time.Hour},
		{"dns.resolvers", strings.Join(c.DNS.Resolvers, ","), "127.0.0.1:53"},
		{"dns.fallback_resolvers", strings.Join(c.DNS.FallbackResolvers, ","), "1.1.1.1:53,9.9.9.9:53"},
		{"dns.concurrency", c.DNS.Concurrency, 2000},
		{"dns.max_qps", c.DNS.MaxQPS, 10000},
		{"dns.ipv6", c.DNS.IPv6, true},
		{"http.concurrency", c.HTTP.Concurrency, 2000},
		{"http.per_ip_limit", c.HTTP.PerIPLimit, 4},
		{"http.connect_timeout", c.HTTP.ConnectTimeout.D(), 5 * time.Second},
		{"http.header_timeout", c.HTTP.HeaderTimeout.D(), 10 * time.Second},
		{"http.total_timeout", c.HTTP.TotalTimeout.D(), 20 * time.Second},
		{"http.max_redirects", c.HTTP.MaxRedirects, 8},
		{"http.max_body_bytes", c.HTTP.MaxBodyBytes.B(), int64(2 << 20)},
		{"http.user_agent", c.HTTP.UserAgent != "", true},
		{"render.remote_url", c.Render.RemoteURL, ""},
		{"render.contexts", c.Render.Contexts, 3},
		{"render.timeout", c.Render.Timeout.D(), 25 * time.Second},
		{"render.restart_every_pages", c.Render.RestartEveryPages, 500},
		{"content.dir", c.Content.Dir, "/data/content"},
		{"content.pack_size", c.Content.PackSize.B(), int64(512 << 20)},
		{"content.max_store_bytes", c.Content.MaxStoreBytes.B(), int64(2 << 20)},
		{"content.zstd_dictionary", c.Content.ZstdDictionary, "/data/content/dict.zst"},
		{"analyzer.rules_dir", c.Analyzer.RulesDir, "/etc/crawler/rules"},
		{"analyzer.laravel_min_score", c.Analyzer.LaravelMinScore, 0.6},
		{"monitor.listen", c.Monitor.Listen, ":9100"},
		{"monitor.zone_stats_interval", c.Monitor.ZoneStatsInterval.D(), 60 * time.Second},
		{"log.level", c.Log.Level, "info"},
	}
	for _, ch := range checks {
		if ch.got != ch.want {
			t.Errorf("%s: по умолчанию ожидается %v, получено %v", ch.name, ch.want, ch.got)
		}
	}
	if want := []string{"", "www", "admin", "crm", "api", "app"}; strings.Join(c.Subdomains, ",") != strings.Join(want, ",") {
		t.Errorf("subdomains: ожидается %v, получено %v", want, c.Subdomains)
	}

	// Все значения по умолчанию проходят валидацию после задания dsn.
	c.Postgres.DSN = "postgres://u:p@127.0.0.1:5432/crawler"
	if err := c.Validate(); err != nil {
		t.Errorf("конфигурация по умолчанию должна быть валидна: %v", err)
	}
}

func TestLoadYAMLFull(t *testing.T) {
	cfg := mustLoad(t, `
postgres:
  dsn: "postgres://crawler:secret@db.local:5432/crawler?sslmode=disable"
  pool_size: 40
redis:
  addr: "10.0.0.1:6379"
ingest:
  dir: "/srv/incoming"
  batch_size: 10000
  stable_seconds: 30
scheduler:
  recheck_interval: "8760h"
  batch_size: 2000
  queue_high_watermark: 50000
  lease: "45m"
  temp_error_retry_after: "12h"
dns:
  resolvers: ["10.0.0.2:53", "10.0.0.3:53"]
  fallback_resolvers: ["8.8.8.8:53"]
  concurrency: 500
  max_qps: 2000
  ipv6: false
http:
  concurrency: 1500
  per_ip_limit: 3
  connect_timeout: "3s"
  header_timeout: "7s"
  total_timeout: "15s"
  max_redirects: 5
  max_body_bytes: 1048576
  user_agent: "TestCrawler/2.0 (contact: test@example.invalid)"
render:
  remote_url: "ws://10.0.0.9:9222"
  contexts: 4
  timeout: "40s"
  restart_every_pages: 200
content:
  dir: "/srv/content"
  pack_size: "1GiB"
  max_store_bytes: "1MiB"
  zstd_dictionary: ""
analyzer:
  rules_dir: "/srv/rules"
  laravel_min_score: 0.75
subdomains:
  - ""
  - "www"
  - "dev"
monitor:
  listen: "127.0.0.1:9110"
  zone_stats_interval: "30s"
log:
  level: "warn"
`, nil)

	type check struct {
		name string
		got  any
		want any
	}
	checks := []check{
		{"postgres.dsn", cfg.Postgres.DSN, "postgres://crawler:secret@db.local:5432/crawler?sslmode=disable"},
		{"postgres.pool_size", cfg.Postgres.PoolSize, 40},
		{"redis.addr", cfg.Redis.Addr, "10.0.0.1:6379"},
		{"ingest.dir", cfg.Ingest.Dir, "/srv/incoming"},
		{"ingest.batch_size", cfg.Ingest.BatchSize, 10000},
		{"ingest.stable_seconds", cfg.Ingest.StableSeconds, 30},
		{"scheduler.recheck_interval", cfg.Scheduler.RecheckInterval.D(), 8760 * time.Hour},
		{"scheduler.batch_size", cfg.Scheduler.BatchSize, 2000},
		{"scheduler.queue_high_watermark", cfg.Scheduler.QueueHighWatermark, 50000},
		{"scheduler.lease", cfg.Scheduler.Lease.D(), 45 * time.Minute},
		{"scheduler.temp_error_retry_after", cfg.Scheduler.TempErrorRetryAfter.D(), 12 * time.Hour},
		{"dns.resolvers", strings.Join(cfg.DNS.Resolvers, ";"), "10.0.0.2:53;10.0.0.3:53"},
		{"dns.fallback_resolvers", strings.Join(cfg.DNS.FallbackResolvers, ";"), "8.8.8.8:53"},
		{"dns.concurrency", cfg.DNS.Concurrency, 500},
		{"dns.max_qps", cfg.DNS.MaxQPS, 2000},
		{"dns.ipv6", cfg.DNS.IPv6, false},
		{"http.concurrency", cfg.HTTP.Concurrency, 1500},
		{"http.per_ip_limit", cfg.HTTP.PerIPLimit, 3},
		{"http.connect_timeout", cfg.HTTP.ConnectTimeout.D(), 3 * time.Second},
		{"http.header_timeout", cfg.HTTP.HeaderTimeout.D(), 7 * time.Second},
		{"http.total_timeout", cfg.HTTP.TotalTimeout.D(), 15 * time.Second},
		{"http.max_redirects", cfg.HTTP.MaxRedirects, 5},
		{"http.max_body_bytes", cfg.HTTP.MaxBodyBytes.B(), int64(1048576)},
		{"http.user_agent", cfg.HTTP.UserAgent, "TestCrawler/2.0 (contact: test@example.invalid)"},
		{"render.remote_url", cfg.Render.RemoteURL, "ws://10.0.0.9:9222"},
		{"render.contexts", cfg.Render.Contexts, 4},
		{"render.timeout", cfg.Render.Timeout.D(), 40 * time.Second},
		{"render.restart_every_pages", cfg.Render.RestartEveryPages, 200},
		{"content.dir", cfg.Content.Dir, "/srv/content"},
		{"content.pack_size", cfg.Content.PackSize.B(), int64(1 << 30)},
		{"content.max_store_bytes", cfg.Content.MaxStoreBytes.B(), int64(1 << 20)},
		{"content.zstd_dictionary", cfg.Content.ZstdDictionary, ""},
		{"analyzer.rules_dir", cfg.Analyzer.RulesDir, "/srv/rules"},
		{"analyzer.laravel_min_score", cfg.Analyzer.LaravelMinScore, 0.75},
		{"monitor.listen", cfg.Monitor.Listen, "127.0.0.1:9110"},
		{"monitor.zone_stats_interval", cfg.Monitor.ZoneStatsInterval.D(), 30 * time.Second},
		{"log.level", cfg.Log.Level, "warn"},
	}
	for _, ch := range checks {
		if ch.got != ch.want {
			t.Errorf("%s: ожидается %v, получено %v", ch.name, ch.want, ch.got)
		}
	}
	if want := []string{"", "www", "dev"}; strings.Join(cfg.Subdomains, ",") != strings.Join(want, ",") {
		t.Errorf("subdomains: ожидается %v, получено %v", want, cfg.Subdomains)
	}
}

// Частичный YAML: отсутствующие ключи сохраняют значения по умолчанию.
func TestLoadYAMLPartial(t *testing.T) {
	cfg := mustLoad(t, `
postgres:
  dsn: "postgres://u:p@127.0.0.1:5432/crawler"
http:
  total_timeout: "45s"
`, nil)
	if cfg.HTTP.TotalTimeout.D() != 45*time.Second {
		t.Errorf("http.total_timeout: ожидается 45s из YAML, получено %s", cfg.HTTP.TotalTimeout)
	}
	if cfg.HTTP.ConnectTimeout.D() != 5*time.Second {
		t.Errorf("http.connect_timeout: ожидается значение по умолчанию 5s, получено %s", cfg.HTTP.ConnectTimeout)
	}
	if cfg.Redis.Addr != "127.0.0.1:6379" {
		t.Errorf("redis.addr: ожидается значение по умолчанию, получено %q", cfg.Redis.Addr)
	}
}

func TestLoadYAMLErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{"неизвестное поле", "postgres:\n  dsn: x\n  unknown_key: 1\n", "unknown_key"},
		{"неизвестная секция", "postgers:\n  dsn: x\n", "postgers"},
		{"длительность без единицы", "http:\n  total_timeout: 20\n", "длительность"},
		{"кривая длительность", "http:\n  total_timeout: \"abc\"\n", "длительность"},
		{"кривой размер", "content:\n  pack_size: \"512QB\"\n", "размер"},
		{"размер не число", "content:\n  pack_size: \"много\"\n", "размер"},
		{"не тот тип", "http:\n  concurrency: \"много\"\n", "unmarshal"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadFrom(t, tt.yaml, validDSN())
			if err == nil {
				t.Fatalf("ожидается ошибка, конфигурация загрузилась")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("ошибка %q не содержит %q", err, tt.want)
			}
		})
	}
}

func TestParseSizeForms(t *testing.T) {
	tests := []struct {
		in   string
		want int64
	}{
		{"2097152", 2097152},
		{"512MB", 512 * 1000 * 1000},
		{"512mb", 512 * 1000 * 1000},
		{"1GiB", 1 << 30},
		{"16 MiB", 16 << 20},
		{"2KB", 2000},
		{"3KiB", 3 << 10},
		{"100B", 100},
	}
	for _, tt := range tests {
		got, err := ParseSize(tt.in)
		if err != nil {
			t.Errorf("ParseSize(%q): %v", tt.in, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseSize(%q) = %d, ожидается %d", tt.in, got, tt.want)
		}
	}
	if _, err := ParseSize("512 XB"); err == nil {
		t.Errorf("ParseSize должен отклонять неизвестную единицу")
	}
	if _, err := ParseSize(""); err == nil {
		t.Errorf("ParseSize должен отклонять пустую строку")
	}
	if _, err := ParseSize("99999999999999999999GB"); err == nil {
		t.Errorf("ParseSize должен отклонять переполнение")
	}
}

func TestEnvOverrides(t *testing.T) {
	cfg := mustLoad(t, "postgres:\n  dsn: \"postgres://yaml@db/crawler\"\n", map[string]string{
		"CRAWLER_POSTGRES_DSN":                     "postgres://env@db/crawler",
		"CRAWLER_POSTGRES_POOL_SIZE":               "40",
		"CRAWLER_REDIS_ADDR":                       "10.9.8.7:6379",
		"CRAWLER_INGEST_DIR":                       "/env/incoming",
		"CRAWLER_INGEST_BATCH_SIZE":                "1234",
		"CRAWLER_INGEST_STABLE_SECONDS":            "25",
		"CRAWLER_SCHEDULER_RECHECK_INTERVAL":       "2190h",
		"CRAWLER_SCHEDULER_BATCH_SIZE":             "777",
		"CRAWLER_SCHEDULER_QUEUE_HIGH_WATERMARK":   "4242",
		"CRAWLER_SCHEDULER_LEASE":                  "15m",
		"CRAWLER_SCHEDULER_TEMP_ERROR_RETRY_AFTER": "6h",
		"CRAWLER_DNS_RESOLVERS":                    "10.0.0.2:53, 10.0.0.3:53",
		"CRAWLER_DNS_FALLBACK_RESOLVERS":           "8.8.8.8:53",
		"CRAWLER_DNS_CONCURRENCY":                  "321",
		"CRAWLER_DNS_MAX_QPS":                      "123",
		"CRAWLER_DNS_IPV6":                         "false",
		"CRAWLER_HTTP_CONCURRENCY":                 "654",
		"CRAWLER_HTTP_PER_IP_LIMIT":                "3",
		"CRAWLER_HTTP_CONNECT_TIMEOUT":             "2s",
		"CRAWLER_HTTP_HEADER_TIMEOUT":              "4s",
		"CRAWLER_HTTP_TOTAL_TIMEOUT":               "9s",
		"CRAWLER_HTTP_MAX_REDIRECTS":               "3",
		"CRAWLER_HTTP_MAX_BODY_BYTES":              "4MiB",
		"CRAWLER_HTTP_USER_AGENT":                  "EnvCrawler/1.0 (contact: env@example.invalid)",
		"CRAWLER_RENDER_REMOTE_URL":                "ws://10.0.0.9:9222",
		"CRAWLER_RENDER_CONTEXTS":                  "2",
		"CRAWLER_RENDER_TIMEOUT":                   "11s",
		"CRAWLER_RENDER_RESTART_EVERY_PAGES":       "99",
		"CRAWLER_CONTENT_DIR":                      "/env/content",
		"CRAWLER_CONTENT_PACK_SIZE":                "256MB",
		"CRAWLER_CONTENT_MAX_STORE_BYTES":          "512KiB",
		"CRAWLER_CONTENT_ZSTD_DICTIONARY":          "/env/dict.zst",
		"CRAWLER_ANALYZER_RULES_DIR":               "/env/rules",
		"CRAWLER_ANALYZER_LARAVEL_MIN_SCORE":       "0.9",
		"CRAWLER_SUBDOMAINS":                       "www, mail",
		"CRAWLER_MONITOR_LISTEN":                   ":9199",
		"CRAWLER_MONITOR_ZONE_STATS_INTERVAL":      "45s",
		"CRAWLER_LOG_LEVEL":                        "debug",
	})
	type check struct {
		name string
		got  any
		want any
	}
	checks := []check{
		{"postgres.dsn", cfg.Postgres.DSN, "postgres://env@db/crawler"},
		{"postgres.pool_size", cfg.Postgres.PoolSize, 40},
		{"redis.addr", cfg.Redis.Addr, "10.9.8.7:6379"},
		{"ingest.dir", cfg.Ingest.Dir, "/env/incoming"},
		{"ingest.batch_size", cfg.Ingest.BatchSize, 1234},
		{"ingest.stable_seconds", cfg.Ingest.StableSeconds, 25},
		{"scheduler.recheck_interval", cfg.Scheduler.RecheckInterval.D(), 2190 * time.Hour},
		{"scheduler.batch_size", cfg.Scheduler.BatchSize, 777},
		{"scheduler.queue_high_watermark", cfg.Scheduler.QueueHighWatermark, 4242},
		{"scheduler.lease", cfg.Scheduler.Lease.D(), 15 * time.Minute},
		{"scheduler.temp_error_retry_after", cfg.Scheduler.TempErrorRetryAfter.D(), 6 * time.Hour},
		{"dns.resolvers", strings.Join(cfg.DNS.Resolvers, ";"), "10.0.0.2:53;10.0.0.3:53"},
		{"dns.fallback_resolvers", strings.Join(cfg.DNS.FallbackResolvers, ";"), "8.8.8.8:53"},
		{"dns.concurrency", cfg.DNS.Concurrency, 321},
		{"dns.max_qps", cfg.DNS.MaxQPS, 123},
		{"dns.ipv6", cfg.DNS.IPv6, false},
		{"http.concurrency", cfg.HTTP.Concurrency, 654},
		{"http.per_ip_limit", cfg.HTTP.PerIPLimit, 3},
		{"http.connect_timeout", cfg.HTTP.ConnectTimeout.D(), 2 * time.Second},
		{"http.header_timeout", cfg.HTTP.HeaderTimeout.D(), 4 * time.Second},
		{"http.total_timeout", cfg.HTTP.TotalTimeout.D(), 9 * time.Second},
		{"http.max_redirects", cfg.HTTP.MaxRedirects, 3},
		{"http.max_body_bytes", cfg.HTTP.MaxBodyBytes.B(), int64(4 << 20)},
		{"http.user_agent", cfg.HTTP.UserAgent, "EnvCrawler/1.0 (contact: env@example.invalid)"},
		{"render.remote_url", cfg.Render.RemoteURL, "ws://10.0.0.9:9222"},
		{"render.contexts", cfg.Render.Contexts, 2},
		{"render.timeout", cfg.Render.Timeout.D(), 11 * time.Second},
		{"render.restart_every_pages", cfg.Render.RestartEveryPages, 99},
		{"content.dir", cfg.Content.Dir, "/env/content"},
		{"content.pack_size", cfg.Content.PackSize.B(), int64(256 * 1000 * 1000)},
		{"content.max_store_bytes", cfg.Content.MaxStoreBytes.B(), int64(512 << 10)},
		{"content.zstd_dictionary", cfg.Content.ZstdDictionary, "/env/dict.zst"},
		{"analyzer.rules_dir", cfg.Analyzer.RulesDir, "/env/rules"},
		{"analyzer.laravel_min_score", cfg.Analyzer.LaravelMinScore, 0.9},
		{"monitor.listen", cfg.Monitor.Listen, ":9199"},
		{"monitor.zone_stats_interval", cfg.Monitor.ZoneStatsInterval.D(), 45 * time.Second},
		{"log.level", cfg.Log.Level, "debug"},
	}
	for _, ch := range checks {
		if ch.got != ch.want {
			t.Errorf("%s: ожидается %v, получено %v", ch.name, ch.want, ch.got)
		}
	}
	// Список субдоменов из env заменяет список целиком (включая апекс).
	if want := []string{"www", "mail"}; strings.Join(cfg.Subdomains, ",") != strings.Join(want, ",") {
		t.Errorf("subdomains: ожидается %v (замена целиком, без апекса), получено %v", want, cfg.Subdomains)
	}
}

func TestEnvBadValues(t *testing.T) {
	tests := []struct {
		env  map[string]string
		want string
	}{
		{map[string]string{"CRAWLER_HTTP_CONCURRENCY": "много"}, "CRAWLER_HTTP_CONCURRENCY"},
		{map[string]string{"CRAWLER_DNS_IPV6": "да"}, "CRAWLER_DNS_IPV6"},
		{map[string]string{"CRAWLER_HTTP_TOTAL_TIMEOUT": "20"}, "CRAWLER_HTTP_TOTAL_TIMEOUT"},
		{map[string]string{"CRAWLER_CONTENT_PACK_SIZE": "big"}, "CRAWLER_CONTENT_PACK_SIZE"},
		{map[string]string{"CRAWLER_ANALYZER_LARAVEL_MIN_SCORE": "x"}, "CRAWLER_ANALYZER_LARAVEL_MIN_SCORE"},
		{map[string]string{"CRAWLER_LOG_LEVEL": "verbose"}, "log.level"},
	}
	for _, tt := range tests {
		env := validDSN()
		for k, v := range tt.env {
			env[k] = v
		}
		_, err := loadFrom(t, "", env)
		if err == nil {
			t.Fatalf("переменная %s: ожидается ошибка разбора", tt.want)
		}
		if !strings.Contains(err.Error(), tt.want) {
			t.Errorf("ошибка %q не содержит имя переменной %q", err, tt.want)
		}
	}
}

// Приоритет: переменные окружения > YAML > значение по умолчанию.
// Пустое значение переменной считается незаданным.
func TestPriorityEnvYAMLDefault(t *testing.T) {
	cfg := mustLoad(t, `
postgres:
  dsn: "postgres://yaml@db/crawler"
http:
  total_timeout: "30s"
  header_timeout: "11s"
`, map[string]string{
		"CRAWLER_HTTP_TOTAL_TIMEOUT":  "40s",
		"CRAWLER_HTTP_HEADER_TIMEOUT": "",
	})
	if cfg.HTTP.TotalTimeout.D() != 40*time.Second {
		t.Errorf("env > yaml: ожидается 40s, получено %s", cfg.HTTP.TotalTimeout)
	}
	if cfg.HTTP.HeaderTimeout.D() != 11*time.Second {
		t.Errorf("пустой env не должен перекрывать YAML: ожидается 11s, получено %s", cfg.HTTP.HeaderTimeout)
	}
	if cfg.HTTP.ConnectTimeout.D() != 5*time.Second {
		t.Errorf("нет env и нет yaml: ожидается default 5s, получено %s", cfg.HTTP.ConnectTimeout)
	}
	if cfg.Postgres.DSN != "postgres://yaml@db/crawler" {
		t.Errorf("postgres.dsn должен прийти из YAML, получено %q", cfg.Postgres.DSN)
	}
}

// Устаревшая APP_PG_DSN применяется, новая CRAWLER_POSTGRES_DSN важнее.
func TestLegacyPGDSN(t *testing.T) {
	cfg := mustLoad(t, "", map[string]string{"APP_PG_DSN": "postgres://legacy@db/crawler"})
	if cfg.Postgres.DSN != "postgres://legacy@db/crawler" {
		t.Errorf("APP_PG_DSN: ожидается DSN из переменной, получено %q", cfg.Postgres.DSN)
	}

	cfg = mustLoad(t, "postgres:\n  dsn: \"postgres://yaml@db/crawler\"\n", map[string]string{
		"APP_PG_DSN":           "postgres://legacy@db/crawler",
		"CRAWLER_POSTGRES_DSN": "postgres://new@db/crawler",
	})
	if cfg.Postgres.DSN != "postgres://new@db/crawler" {
		t.Errorf("CRAWLER_POSTGRES_DSN должен быть важнее APP_PG_DSN, получено %q", cfg.Postgres.DSN)
	}
}

func TestConfigPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "custom.yaml")
	if err := os.WriteFile(path, []byte("postgres:\n  dsn: \"postgres://path@db/crawler\"\nredis:\n  addr: \"10.1.1.1:6379\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	env := func(m map[string]string) func(string) string {
		return func(k string) string { return m[k] }
	}

	// CRAWLER_CONFIG указывает на файл.
	cfg, err := LoadEnv(env(map[string]string{"CRAWLER_CONFIG": path}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Redis.Addr != "10.1.1.1:6379" || cfg.Postgres.DSN != "postgres://path@db/crawler" {
		t.Errorf("CRAWLER_CONFIG: файл не прочитан: %+v", cfg.Redis)
	}

	// Устаревшая APP_CONFIG работает как запасное имя.
	cfg, err = LoadEnv(env(map[string]string{"APP_CONFIG": path}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Redis.Addr != "10.1.1.1:6379" {
		t.Errorf("APP_CONFIG: файл не прочитан")
	}

	// Явно заданный несуществующий файл — ошибка (а не молчаливые дефолты).
	_, err = LoadEnv(env(map[string]string{"CRAWLER_CONFIG": filepath.Join(dir, "missing.yaml")}))
	if err == nil || !strings.Contains(err.Error(), "missing.yaml") {
		t.Errorf("ожидается ошибка с путём к файлу, получено %v", err)
	}

	// Без переменной и без ./config.yaml — только defaults + env.
	cfg, err = LoadEnv(env(map[string]string{"CRAWLER_POSTGRES_DSN": "postgres://envonly@db/crawler"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Redis.Addr != "127.0.0.1:6379" || cfg.Postgres.DSN != "postgres://envonly@db/crawler" {
		t.Errorf("без файла ожидаются defaults+env, получено redis=%q dsn=%q", cfg.Redis.Addr, cfg.Postgres.DSN)
	}
}

func TestEmptyYAMLFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.yaml")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	// LoadFile читает окружение процесса: DSN даём переменной через t.Setenv.
	t.Setenv("CRAWLER_POSTGRES_DSN", "postgres://empty@db/crawler")
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("пустой файл: %v", err)
	}
	if cfg.Redis.Addr != "127.0.0.1:6379" {
		t.Errorf("пустой файл: ожидаются значения по умолчанию")
	}
}

func TestValidate(t *testing.T) {
	valid := func() *Config {
		c := Default()
		c.Postgres.DSN = "postgres://u:p@127.0.0.1:5432/crawler"
		return c
	}
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"dsn пуст", func(c *Config) { c.Postgres.DSN = "" }, "postgres.dsn"},
		{"dsn из пробелов", func(c *Config) { c.Postgres.DSN = "   " }, "postgres.dsn"},
		{"pool_size 0", func(c *Config) { c.Postgres.PoolSize = 0 }, "postgres.pool_size"},
		{"redis без порта", func(c *Config) { c.Redis.Addr = "127.0.0.1" }, "redis.addr"},
		{"redis кривой порт", func(c *Config) { c.Redis.Addr = "127.0.0.1:port" }, "redis.addr"},
		{"ingest.dir пуст", func(c *Config) { c.Ingest.Dir = "" }, "ingest.dir"},
		{"batch_size 0", func(c *Config) { c.Ingest.BatchSize = 0 }, "ingest.batch_size"},
		{"stable_seconds 0", func(c *Config) { c.Ingest.StableSeconds = 0 }, "ingest.stable_seconds"},
		{"recheck_interval 0", func(c *Config) { c.Scheduler.RecheckInterval = 0 }, "scheduler.recheck_interval"},
		{"batch scheduler 0", func(c *Config) { c.Scheduler.BatchSize = 0 }, "scheduler.batch_size"},
		{"watermark 0", func(c *Config) { c.Scheduler.QueueHighWatermark = 0 }, "scheduler.queue_high_watermark"},
		{"lease 0", func(c *Config) { c.Scheduler.Lease = 0 }, "scheduler.lease"},
		{"retry_after 0", func(c *Config) { c.Scheduler.TempErrorRetryAfter = 0 }, "scheduler.temp_error_retry_after"},
		{"resolvers пуст", func(c *Config) { c.DNS.Resolvers = nil }, "dns.resolvers"},
		{"resolver без порта", func(c *Config) { c.DNS.Resolvers = []string{"127.0.0.1"} }, "dns.resolvers"},
		{"fallback кривой", func(c *Config) { c.DNS.FallbackResolvers = []string{"noport"} }, "dns.fallback"},
		{"dns concurrency 0", func(c *Config) { c.DNS.Concurrency = 0 }, "dns.concurrency"},
		{"max_qps 0", func(c *Config) { c.DNS.MaxQPS = 0 }, "dns.max_qps"},
		{"http concurrency 0", func(c *Config) { c.HTTP.Concurrency = 0 }, "http.concurrency"},
		{"per_ip_limit 0", func(c *Config) { c.HTTP.PerIPLimit = 0 }, "http.per_ip_limit"},
		{"connect_timeout 0", func(c *Config) { c.HTTP.ConnectTimeout = 0 }, "http.connect_timeout"},
		{"header_timeout 0", func(c *Config) { c.HTTP.HeaderTimeout = 0 }, "http.header_timeout"},
		{"total_timeout отрицательный", func(c *Config) { c.HTTP.TotalTimeout = Duration(-time.Second) }, "http.total_timeout"},
		{"max_redirects -1", func(c *Config) { c.HTTP.MaxRedirects = -1 }, "http.max_redirects"},
		{"max_body_bytes 0", func(c *Config) { c.HTTP.MaxBodyBytes = 0 }, "http.max_body_bytes"},
		{"user_agent пуст", func(c *Config) { c.HTTP.UserAgent = "" }, "http.user_agent"},
		{"remote_url кривой", func(c *Config) { c.Render.RemoteURL = "not-url" }, "render.remote_url"},
		{"contexts 0", func(c *Config) { c.Render.Contexts = 0 }, "render.contexts"},
		{"render.timeout 0", func(c *Config) { c.Render.Timeout = 0 }, "render.timeout"},
		{"restart_every_pages 0", func(c *Config) { c.Render.RestartEveryPages = 0 }, "render.restart_every_pages"},
		{"content.dir пуст", func(c *Config) { c.Content.Dir = "" }, "content.dir"},
		{"pack_size мал", func(c *Config) { c.Content.PackSize = 1 }, "content.pack_size"},
		{"pack_size велик", func(c *Config) { c.Content.PackSize = ByteSize(16 << 30) }, "content.pack_size"},
		{"max_store_bytes 0", func(c *Config) { c.Content.MaxStoreBytes = 0 }, "content.max_store_bytes"},
		{"rules_dir пуст", func(c *Config) { c.Analyzer.RulesDir = "" }, "analyzer.rules_dir"},
		{"min_score 0", func(c *Config) { c.Analyzer.LaravelMinScore = 0 }, "analyzer.laravel_min_score"},
		{"min_score >1", func(c *Config) { c.Analyzer.LaravelMinScore = 1.5 }, "analyzer.laravel_min_score"},
		{"subdomains пуст", func(c *Config) { c.Subdomains = nil }, "subdomains"},
		{"subdomains 64", func(c *Config) {
			s := make([]string, 64)
			for i := range s {
				s[i] = fmt.Sprintf("s%d", i)
			}
			c.Subdomains = s
		}, "subdomains"},
		{"subdomain с точкой", func(c *Config) { c.Subdomains = []string{"", "a.b"} }, "subdomains"},
		{"subdomain с дефисом в начале", func(c *Config) { c.Subdomains = []string{"", "-www"} }, "subdomains"},
		{"subdomain повтор", func(c *Config) { c.Subdomains = []string{"", "www", "www"} }, "subdomains"},
		{"listen кривой", func(c *Config) { c.Monitor.Listen = "abc" }, "monitor.listen"},
		{"zone_stats_interval 0", func(c *Config) { c.Monitor.ZoneStatsInterval = 0 }, "monitor.zone_stats_interval"},
		{"log.level кривой", func(c *Config) { c.Log.Level = "verbose" }, "log.level"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := valid()
			tt.mutate(c)
			c.normalize()
			err := c.Validate()
			if err == nil {
				t.Fatalf("ожидается ошибка валидации")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("ошибка %q не содержит %q", err, tt.want)
			}
		})
	}

	// Граничные корректные значения.
	c := valid()
	c.Analyzer.LaravelMinScore = 1
	c.HTTP.MaxRedirects = 0
	c.Subdomains = make([]string, maxSubdomains)
	for i := range c.Subdomains {
		c.Subdomains[i] = fmt.Sprintf("s%d", i)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("граничные значения должны быть валидны: %v", err)
	}
}

// Регистр субдоменов приводится к нижнему (normalize), «WWW» и «www» — дубль.
func TestSubdomainsNormalize(t *testing.T) {
	cfg := mustLoad(t, "subdomains:\n  - \"\"\n  - \" WWW \"\n  - \"Api\"\n", validDSN())
	if want := []string{"", "www", "api"}; strings.Join(cfg.Subdomains, ",") != strings.Join(want, ",") {
		t.Errorf("subdomains после normalize: ожидаются %v, получено %v", want, cfg.Subdomains)
	}
	_, err := loadFrom(t, "subdomains:\n  - \"\"\n  - \"www\"\n  - \"WWW\"\n", validDSN())
	if err == nil || !strings.Contains(err.Error(), "повтор") {
		t.Errorf("www и WWW после normalize — дубль, ожидается ошибка, получено %v", err)
	}
}

// Регистр и пробелы log.level приводятся к каноничной форме (normalize);
// уровень нужен каркасу сервисов (internal/service).
func TestLogLevelNormalize(t *testing.T) {
	cfg := mustLoad(t, "log:\n  level: \"  WARN \"\n", validDSN())
	if cfg.Log.Level != "warn" {
		t.Errorf("log.level после normalize: ожидается %q, получено %q", "warn", cfg.Log.Level)
	}
	env := validDSN()
	env["CRAWLER_LOG_LEVEL"] = "ERROR"
	cfg = mustLoad(t, "", env)
	if cfg.Log.Level != "error" {
		t.Errorf("log.level из env после normalize: ожидается %q, получено %q", "error", cfg.Log.Level)
	}
}

// В таблице envSpecs нет повторов имён (рассинхронизация таблицы и
// структуры ловится TestEnvOverrides, покрывающим каждую строку).
func TestEnvSpecsNoDuplicates(t *testing.T) {
	seen := map[string]bool{}
	for _, s := range envSpecs() {
		if seen[s.key] {
			t.Errorf("переменная %s встречается в таблице дважды", s.key)
		}
		seen[s.key] = true
	}
}

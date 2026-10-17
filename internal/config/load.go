package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Переменные окружения, задающие источник конфигурации.
const (
	// EnvConfigPath — путь к YAML-файлу конфигурации.
	EnvConfigPath = "CRAWLER_CONFIG"
	// legacyEnvConfigPath — устаревшее имя переменной пути (dev-окружение).
	legacyEnvConfigPath = "APP_CONFIG"
	// DefaultConfigPath — используется, когда переменная пути не задана
	// и файл существует в текущем каталоге (удобно для локального запуска
	// из корня репозитория рядом с config.example.yaml).
	DefaultConfigPath = "config.yaml"
)

// Load читает конфигурацию в окружении процесса: значения по умолчанию →
// YAML-файл (CRAWLER_CONFIG/APP_CONFIG или config.yaml в текущем каталоге,
// если существует) → переменные окружения; затем валидация.
func Load() (*Config, error) {
	return LoadEnv(os.Getenv)
}

// LoadEnv — как Load, но окружение читается через getenv (тесты и встраивание).
func LoadEnv(getenv func(string) string) (*Config, error) {
	path := firstNonEmpty(getenv(EnvConfigPath), getenv(legacyEnvConfigPath))
	if path == "" {
		if st, err := os.Stat(DefaultConfigPath); err == nil && !st.IsDir() {
			path = DefaultConfigPath
		}
	}
	if path == "" {
		return build(Default(), getenv, "без файла (значения по умолчанию + окружение)")
	}
	return loadPath(path, getenv)
}

// LoadFile читает конфигурацию из явного пути; переопределения берутся из
// окружения процесса (по ТЗ конфигурация едина: YAML + переменные сверху).
func LoadFile(path string) (*Config, error) {
	return loadPath(path, os.Getenv)
}

func loadPath(path string, getenv func(string) string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: чтение %s: %w", path, err)
	}
	cfg := Default()
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true) // опечатка в ключе — ошибка, а не молчаливо пропущенное значение
	if err := dec.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("config: разбор %s: %w", path, err)
	}
	return build(cfg, getenv, path)
}

func build(cfg *Config, getenv func(string) string, src string) (*Config, error) {
	if err := cfg.applyEnv(getenv); err != nil {
		return nil, fmt.Errorf("config: %s: %w", src, err)
	}
	cfg.normalize()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: %s: %w", src, err)
	}
	return cfg, nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// envSpec — одна привязка «переменная окружения → поле». Таблица — единый
// источник имён переопределений; её отражение — в docs/configuration.md.
type envSpec struct {
	key string
	set func(c *Config, v string) error
}

// envSpecs возвращает все поддерживаемые переменные переопределения
// (кроме CRAWLER_CONFIG — путь к файлу, и устаревших имён).
func envSpecs() []envSpec {
	return []envSpec{
		{"CRAWLER_POSTGRES_DSN", setString(func(c *Config) *string { return &c.Postgres.DSN })},
		{"CRAWLER_POSTGRES_POOL_SIZE", setInt(func(c *Config) *int { return &c.Postgres.PoolSize })},
		{"CRAWLER_REDIS_ADDR", setString(func(c *Config) *string { return &c.Redis.Addr })},
		{"CRAWLER_REDIS_MAXLEN", setInt(func(c *Config) *int { return &c.Redis.MaxLen })},
		{"CRAWLER_REDIS_MAX_ATTEMPTS", setInt(func(c *Config) *int { return &c.Redis.MaxAttempts })},
		{"CRAWLER_REDIS_CLAIM_MIN_IDLE", setDuration(func(c *Config) *Duration { return &c.Redis.ClaimMinIdle })},
		{"CRAWLER_INGEST_DIR", setString(func(c *Config) *string { return &c.Ingest.Dir })},
		{"CRAWLER_INGEST_BATCH_SIZE", setInt(func(c *Config) *int { return &c.Ingest.BatchSize })},
		{"CRAWLER_INGEST_STABLE_SECONDS", setInt(func(c *Config) *int { return &c.Ingest.StableSeconds })},
		{"CRAWLER_INGEST_SCAN_INTERVAL", setDuration(func(c *Config) *Duration { return &c.Ingest.ScanInterval })},
		{"CRAWLER_INGEST_MAX_ERROR_RATIO", setFloat(func(c *Config) *float64 { return &c.Ingest.MaxErrorRatio })},
		{"CRAWLER_SCHEDULER_RECHECK_INTERVAL", setDuration(func(c *Config) *Duration { return &c.Scheduler.RecheckInterval })},
		{"CRAWLER_SCHEDULER_BATCH_SIZE", setInt(func(c *Config) *int { return &c.Scheduler.BatchSize })},
		{"CRAWLER_SCHEDULER_QUEUE_HIGH_WATERMARK", setInt(func(c *Config) *int { return &c.Scheduler.QueueHighWatermark })},
		{"CRAWLER_SCHEDULER_LEASE", setDuration(func(c *Config) *Duration { return &c.Scheduler.Lease })},
		{"CRAWLER_SCHEDULER_TEMP_ERROR_RETRY_AFTER", setDuration(func(c *Config) *Duration { return &c.Scheduler.TempErrorRetryAfter })},
		{"CRAWLER_SCHEDULER_TEMP_ERROR_MAX_RETRIES", setInt(func(c *Config) *int { return &c.Scheduler.TempErrorMaxRetries })},
		{"CRAWLER_DNS_RESOLVERS", setStrings(func(c *Config) *[]string { return &c.DNS.Resolvers })},
		{"CRAWLER_DNS_FALLBACK_RESOLVERS", setStrings(func(c *Config) *[]string { return &c.DNS.FallbackResolvers })},
		{"CRAWLER_DNS_CONCURRENCY", setInt(func(c *Config) *int { return &c.DNS.Concurrency })},
		{"CRAWLER_DNS_MAX_QPS", setInt(func(c *Config) *int { return &c.DNS.MaxQPS })},
		{"CRAWLER_DNS_IPV6", setBool(func(c *Config) *bool { return &c.DNS.IPv6 })},
		{"CRAWLER_DNS_TIMEOUT", setDuration(func(c *Config) *Duration { return &c.DNS.Timeout })},
		{"CRAWLER_DNS_TCP_TIMEOUT", setDuration(func(c *Config) *Duration { return &c.DNS.TCPTimeout })},
		{"CRAWLER_DNS_RETRIES", setInt(func(c *Config) *int { return &c.DNS.Retries })},
		{"CRAWLER_DNS_RETRY_DELAY", setDuration(func(c *Config) *Duration { return &c.DNS.RetryDelay })},
		{"CRAWLER_DNS_UDP_POOL_SIZE", setInt(func(c *Config) *int { return &c.DNS.UDPPoolSize })},
		{"CRAWLER_HTTP_CONCURRENCY", setInt(func(c *Config) *int { return &c.HTTP.Concurrency })},
		{"CRAWLER_HTTP_PER_IP_LIMIT", setInt(func(c *Config) *int { return &c.HTTP.PerIPLimit })},
		{"CRAWLER_HTTP_CONNECT_TIMEOUT", setDuration(func(c *Config) *Duration { return &c.HTTP.ConnectTimeout })},
		{"CRAWLER_HTTP_HEADER_TIMEOUT", setDuration(func(c *Config) *Duration { return &c.HTTP.HeaderTimeout })},
		{"CRAWLER_HTTP_TOTAL_TIMEOUT", setDuration(func(c *Config) *Duration { return &c.HTTP.TotalTimeout })},
		{"CRAWLER_HTTP_MAX_REDIRECTS", setInt(func(c *Config) *int { return &c.HTTP.MaxRedirects })},
		{"CRAWLER_HTTP_MAX_BODY_BYTES", setSize(func(c *Config) *ByteSize { return &c.HTTP.MaxBodyBytes })},
		{"CRAWLER_HTTP_USER_AGENT", setString(func(c *Config) *string { return &c.HTTP.UserAgent })},
		{"CRAWLER_RENDER_REMOTE_URL", setString(func(c *Config) *string { return &c.Render.RemoteURL })},
		{"CRAWLER_RENDER_CONTEXTS", setInt(func(c *Config) *int { return &c.Render.Contexts })},
		{"CRAWLER_RENDER_TIMEOUT", setDuration(func(c *Config) *Duration { return &c.Render.Timeout })},
		{"CRAWLER_RENDER_RESTART_EVERY_PAGES", setInt(func(c *Config) *int { return &c.Render.RestartEveryPages })},
		{"CRAWLER_CONTENT_DIR", setString(func(c *Config) *string { return &c.Content.Dir })},
		{"CRAWLER_CONTENT_PACK_SIZE", setSize(func(c *Config) *ByteSize { return &c.Content.PackSize })},
		{"CRAWLER_CONTENT_MAX_STORE_BYTES", setSize(func(c *Config) *ByteSize { return &c.Content.MaxStoreBytes })},
		{"CRAWLER_CONTENT_ZSTD_DICTIONARY", setString(func(c *Config) *string { return &c.Content.ZstdDictionary })},
		{"CRAWLER_CONTENT_DISK_FREE_PERCENT", setFloat(func(c *Config) *float64 { return &c.Content.DiskFreePercent })},
		{"CRAWLER_ANALYZER_RULES_DIR", setString(func(c *Config) *string { return &c.Analyzer.RulesDir })},
		{"CRAWLER_ANALYZER_LARAVEL_MIN_SCORE", setFloat(func(c *Config) *float64 { return &c.Analyzer.LaravelMinScore })},
		{"CRAWLER_SUBDOMAINS", setStrings(func(c *Config) *[]string { return &c.Subdomains })},
		{"CRAWLER_MONITOR_LISTEN", setString(func(c *Config) *string { return &c.Monitor.Listen })},
		{"CRAWLER_MONITOR_ZONE_STATS_INTERVAL", setDuration(func(c *Config) *Duration { return &c.Monitor.ZoneStatsInterval })},
		{"CRAWLER_LOG_LEVEL", setString(func(c *Config) *string { return &c.Log.Level })},
	}
}

// applyEnv накладывает непустые значения переменных окружения поверх YAML.
func (c *Config) applyEnv(getenv func(string) string) error {
	var errs []error
	for _, s := range envSpecs() {
		v := getenv(s.key)
		if v == "" {
			continue // пустое значение переменной считается незаданным
		}
		if err := s.set(c, v); err != nil {
			errs = append(errs, fmt.Errorf("переменная %s: %w", s.key, err))
		}
	}
	// Устаревшая APP_PG_DSN (deploy/docker/compose.yaml, Makefile):
	// применяется, только если не задана новая CRAWLER_POSTGRES_DSN.
	if getenv("CRAWLER_POSTGRES_DSN") == "" {
		if v := getenv("APP_PG_DSN"); v != "" {
			c.Postgres.DSN = v
		}
	}
	return errors.Join(errs...)
}

func setString(get func(*Config) *string) func(*Config, string) error {
	return func(c *Config, v string) error { *get(c) = v; return nil }
}

func setInt(get func(*Config) *int) func(*Config, string) error {
	return func(c *Config, v string) error {
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return fmt.Errorf("ожидается целое число, получено %q", v)
		}
		*get(c) = n
		return nil
	}
}

func setFloat(get func(*Config) *float64) func(*Config, string) error {
	return func(c *Config, v string) error {
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return fmt.Errorf("ожидается число, получено %q", v)
		}
		*get(c) = f
		return nil
	}
}

func setBool(get func(*Config) *bool) func(*Config, string) error {
	return func(c *Config, v string) error {
		b, err := strconv.ParseBool(strings.TrimSpace(v))
		if err != nil {
			return fmt.Errorf("ожидается булево значение (true/false/1/0), получено %q", v)
		}
		*get(c) = b
		return nil
	}
}

func setDuration(get func(*Config) *Duration) func(*Config, string) error {
	return func(c *Config, v string) error {
		d, err := ParseDuration(v)
		if err != nil {
			return err
		}
		*get(c) = Duration(d)
		return nil
	}
}

func setSize(get func(*Config) *ByteSize) func(*Config, string) error {
	return func(c *Config, v string) error {
		n, err := ParseSize(v)
		if err != nil {
			return err
		}
		*get(c) = ByteSize(n)
		return nil
	}
}

// setStrings разбирает список через запятую; пробелы по краям элементов
// обрезаются, пустые элементы сохраняются (для subdomains пустой элемент —
// апекс домена). Значение целиком заменяет список из YAML.
func setStrings(get func(*Config) *[]string) func(*Config, string) error {
	return func(c *Config, v string) error {
		parts := strings.Split(v, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		*get(c) = parts
		return nil
	}
}

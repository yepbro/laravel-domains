package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Environment variables that set the configuration source.
const (
	// EnvConfigPath is the path to the configuration YAML file.
	EnvConfigPath = "CRAWLER_CONFIG"
	// legacyEnvConfigPath is the deprecated name of the path variable (dev environment).
	legacyEnvConfigPath = "APP_CONFIG"
	// DefaultConfigPath is used when the path variable is not set
	// and the file exists in the current directory (handy for running locally
	// from the repository root next to config.example.yaml).
	DefaultConfigPath = "config.yaml"
)

// Load reads the configuration into the process environment: defaults →
// YAML file (CRAWLER_CONFIG/APP_CONFIG or config.yaml in the current directory,
// if it exists) → environment variables; then validation.
func Load() (*Config, error) {
	return LoadEnv(os.Getenv)
}

// LoadEnv is like Load, but the environment is read via getenv (tests and embedding).
func LoadEnv(getenv func(string) string) (*Config, error) {
	path := firstNonEmpty(getenv(EnvConfigPath), getenv(legacyEnvConfigPath))
	if path == "" {
		if st, err := os.Stat(DefaultConfigPath); err == nil && !st.IsDir() {
			path = DefaultConfigPath
		}
	}
	if path == "" {
		return build(Default(), getenv, "no file (defaults + environment)")
	}
	return loadPath(path, getenv)
}

// LoadFile reads the configuration from an explicit path; overrides are taken
// from the process environment (per the spec, the configuration is unified:
// YAML + variables on top).
func LoadFile(path string) (*Config, error) {
	return loadPath(path, os.Getenv)
}

func loadPath(path string, getenv func(string) string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: reading %s: %w", path, err)
	}
	cfg := Default()
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true) // a typo in a key is an error, not a silently skipped value
	if err := dec.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("config: parsing %s: %w", path, err)
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

// envSpec is a single "environment variable → field" binding. The table is
// the single source of override names; its mirror lives in docs/configuration.md.
type envSpec struct {
	key string
	set func(c *Config, v string) error
}

// envSpecs returns all supported override variables
// (except CRAWLER_CONFIG — the file path — and deprecated names).
func envSpecs() []envSpec {
	return []envSpec{
		{"CRAWLER_POSTGRES_DSN", setString(func(c *Config) *string { return &c.Postgres.DSN })},
		{"CRAWLER_POSTGRES_POOL_SIZE", setInt(func(c *Config) *int { return &c.Postgres.PoolSize })},
		{"CRAWLER_REDIS_ADDR", setString(func(c *Config) *string { return &c.Redis.Addr })},
		{"CRAWLER_REDIS_MAXLEN", setInt(func(c *Config) *int { return &c.Redis.MaxLen })},
		{"CRAWLER_REDIS_MAX_ATTEMPTS", setInt(func(c *Config) *int { return &c.Redis.MaxAttempts })},
		{"CRAWLER_REDIS_CLAIM_MIN_IDLE", setDuration(func(c *Config) *Duration { return &c.Redis.ClaimMinIdle })},
		{"CRAWLER_REDIS_RECOVER_BATCH", setInt(func(c *Config) *int { return &c.Redis.RecoverBatch })},
		{"CRAWLER_REDIS_RECOVER_INTERVAL", setDuration(func(c *Config) *Duration { return &c.Redis.RecoverInterval })},
		{"CRAWLER_REDIS_TRIM_INTERVAL", setDuration(func(c *Config) *Duration { return &c.Redis.TrimInterval })},
		{"CRAWLER_REDIS_CONSUMER_IDLE", setDuration(func(c *Config) *Duration { return &c.Redis.ConsumerIdle })},
		{"CRAWLER_INGEST_DIR", setString(func(c *Config) *string { return &c.Ingest.Dir })},
		{"CRAWLER_INGEST_BATCH_SIZE", setInt(func(c *Config) *int { return &c.Ingest.BatchSize })},
		{"CRAWLER_INGEST_STABLE_SECONDS", setInt(func(c *Config) *int { return &c.Ingest.StableSeconds })},
		{"CRAWLER_INGEST_SCAN_INTERVAL", setDuration(func(c *Config) *Duration { return &c.Ingest.ScanInterval })},
		{"CRAWLER_INGEST_MAX_ERROR_RATIO", setFloat(func(c *Config) *float64 { return &c.Ingest.MaxErrorRatio })},
		{"CRAWLER_INGEST_ARCHIVE_MAX_UNCOMPRESSED", setSize(func(c *Config) *ByteSize { return &c.Ingest.ArchiveMaxUncompressed })},
		{"CRAWLER_INGEST_ARCHIVE_MAX_FILES", setInt(func(c *Config) *int { return &c.Ingest.ArchiveMaxFiles })},
		{"CRAWLER_INGEST_ALLOWED_ZONE_PATHS", setZoneNames(func(c *Config) *[]string { return &c.Ingest.AllowedZonePaths })},
		{"CRAWLER_SCHEDULER_RECHECK_INTERVAL", setDuration(func(c *Config) *Duration { return &c.Scheduler.RecheckInterval })},
		{"CRAWLER_SCHEDULER_BACKLOG_INTERVAL", setDuration(func(c *Config) *Duration { return &c.Scheduler.BacklogInterval })},
		{"CRAWLER_SCHEDULER_BATCH_SIZE", setInt(func(c *Config) *int { return &c.Scheduler.BatchSize })},
		{"CRAWLER_SCHEDULER_QUEUE_HIGH_WATERMARK", setInt(func(c *Config) *int { return &c.Scheduler.QueueHighWatermark })},
		{"CRAWLER_SCHEDULER_LEASE", setDuration(func(c *Config) *Duration { return &c.Scheduler.Lease })},
		{"CRAWLER_SCHEDULER_TEMP_ERROR_RETRY_AFTER", setDuration(func(c *Config) *Duration { return &c.Scheduler.TempErrorRetryAfter })},
		{"CRAWLER_SCHEDULER_TEMP_ERROR_MAX_RETRIES", setInt(func(c *Config) *int { return &c.Scheduler.TempErrorMaxRetries })},
		{"CRAWLER_SCHEDULER_ZONE_WEIGHTS", setZoneWeights(func(c *Config) *map[string]int { return &c.Scheduler.ZoneWeights })},
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
		{"CRAWLER_HTTP_PER_IP_OVERRIDES", setPerIPOverrides(func(c *Config) *map[string]int { return &c.HTTP.PerIPOverrides })},
		{"CRAWLER_HTTP_CONNECT_TIMEOUT", setDuration(func(c *Config) *Duration { return &c.HTTP.ConnectTimeout })},
		{"CRAWLER_HTTP_TLS_HANDSHAKE_TIMEOUT", setDuration(func(c *Config) *Duration { return &c.HTTP.TLSHandshakeTimeout })},
		{"CRAWLER_HTTP_HEADER_TIMEOUT", setDuration(func(c *Config) *Duration { return &c.HTTP.HeaderTimeout })},
		{"CRAWLER_HTTP_TOTAL_TIMEOUT", setDuration(func(c *Config) *Duration { return &c.HTTP.TotalTimeout })},
		{"CRAWLER_HTTP_MAX_REDIRECTS", setInt(func(c *Config) *int { return &c.HTTP.MaxRedirects })},
		{"CRAWLER_HTTP_MAX_BODY_BYTES", setSize(func(c *Config) *ByteSize { return &c.HTTP.MaxBodyBytes })},
		{"CRAWLER_HTTP_MIN_BODY_SPEED", setSize(func(c *Config) *ByteSize { return &c.HTTP.MinBodySpeed })},
		{"CRAWLER_HTTP_MAX_RPS", setInt(func(c *Config) *int { return &c.HTTP.MaxRPS })},
		{"CRAWLER_HTTP_USER_AGENT", setString(func(c *Config) *string { return &c.HTTP.UserAgent })},
		{"CRAWLER_HTTP_BLOCK_INTERNAL_ADDRS", setBool(func(c *Config) *bool { return &c.HTTP.BlockInternalAddrs })},
		{"CRAWLER_HTTP_TASK_CONCURRENCY", setInt(func(c *Config) *int { return &c.HTTP.TaskConcurrency })},
		{"CRAWLER_HTTP_TASK_CONSUME", setInt(func(c *Config) *int { return &c.HTTP.TaskConsume })},
		{"CRAWLER_HTTP_TASK_PER_DOMAIN", setInt(func(c *Config) *int { return &c.HTTP.TaskPerDomain })},
		{"CRAWLER_HTTP_DEAD_HOST_CUTOFF", setBool(func(c *Config) *bool { return &c.HTTP.DeadHostCutoff })},
		{"CRAWLER_HTTP_MEM_LIMIT", setSize(func(c *Config) *ByteSize { return &c.HTTP.MemLimit })},
		{"CRAWLER_RENDER_ENGINE", setString(func(c *Config) *string { return &c.Render.Engine })},
		{"CRAWLER_RENDER_REMOTE_URL", setString(func(c *Config) *string { return &c.Render.RemoteURL })},
		{"CRAWLER_RENDER_CONTEXTS", setInt(func(c *Config) *int { return &c.Render.Contexts })},
		{"CRAWLER_RENDER_TIMEOUT", setDuration(func(c *Config) *Duration { return &c.Render.Timeout })},
		{"CRAWLER_RENDER_TIMEOUT_ATTEMPTS", setInt(func(c *Config) *int { return &c.Render.TimeoutAttempts })},
		{"CRAWLER_RENDER_RESTART_EVERY_PAGES", setInt(func(c *Config) *int { return &c.Render.RestartEveryPages })},
		{"CRAWLER_RENDER_RESTART_MEMORY_BYTES", setSize(func(c *Config) *ByteSize { return &c.Render.RestartMemoryBytes })},
		{"CRAWLER_RENDER_BLOCK_STYLESHEET", setBool(func(c *Config) *bool { return &c.Render.BlockStylesheet })},
		{"CRAWLER_RENDER_BLOCK_TRACKER_DOMAINS", setBool(func(c *Config) *bool { return &c.Render.BlockTrackerDomains })},
		{"CRAWLER_RENDER_QUEUE_HIGH_WATERMARK", setInt(func(c *Config) *int { return &c.Render.QueueHighWatermark })},
		{"CRAWLER_RENDER_REQUEUE_DEPRIORITIZED_ZONES", setZoneNames(func(c *Config) *[]string { return &c.Render.RequeueDeprioritizedZones })},
		{"CRAWLER_RENDER_LATE_SELECT_TIMEOUT", setDuration(func(c *Config) *Duration { return &c.Render.LateSelectTimeout })},
		{"CRAWLER_RENDER_FALLBACK_ENABLED", setBool(func(c *Config) *bool { return &c.Render.Fallback.Enabled })},
		{"CRAWLER_RENDER_FALLBACK_WORKER_RATE", setInt(func(c *Config) *int { return &c.Render.Fallback.WorkerRate })},
		{"CRAWLER_RENDER_FALLBACK_SHARE_PERCENT", setFloat(func(c *Config) *float64 { return &c.Render.Fallback.SharePercent })},
		{"CRAWLER_RENDER_FALLBACK_HEALTH_GATE_STREAK", setInt(func(c *Config) *int { return &c.Render.Fallback.HealthGateStreak })},
		{"CRAWLER_CONTENT_DIR", setString(func(c *Config) *string { return &c.Content.Dir })},
		{"CRAWLER_CONTENT_PACK_SIZE", setSize(func(c *Config) *ByteSize { return &c.Content.PackSize })},
		{"CRAWLER_CONTENT_MAX_STORE_BYTES", setSize(func(c *Config) *ByteSize { return &c.Content.MaxStoreBytes })},
		{"CRAWLER_CONTENT_ZSTD_DICTIONARY", setString(func(c *Config) *string { return &c.Content.ZstdDictionary })},
		{"CRAWLER_CONTENT_DISK_FREE_PERCENT", setFloat(func(c *Config) *float64 { return &c.Content.DiskFreePercent })},
		{"CRAWLER_CONTENT_RETENTION", setDuration(func(c *Config) *Duration { return &c.Content.Retention })},
		{"CRAWLER_CONTENT_RETENTION_INTERVAL", setDuration(func(c *Config) *Duration { return &c.Content.RetentionInterval })},
		{"CRAWLER_CONTENT_RETENTION_BATCH", setInt(func(c *Config) *int { return &c.Content.RetentionBatch })},
		{"CRAWLER_CONTENT_RETENTION_ANALYZE_WATERMARK", setInt(func(c *Config) *int { return &c.Content.RetentionAnalyzeWatermark })},
		{"CRAWLER_CONTENT_RETENTION_SELECT_TIMEOUT", setDuration(func(c *Config) *Duration { return &c.Content.RetentionSelectTimeout })},
		{"CRAWLER_ANALYZER_RULES_DIR", setString(func(c *Config) *string { return &c.Analyzer.RulesDir })},
		{"CRAWLER_ANALYZER_LARAVEL_MIN_SCORE", setFloat(func(c *Config) *float64 { return &c.Analyzer.LaravelMinScore })},
		{"CRAWLER_SUBDOMAINS", setStrings(func(c *Config) *[]string { return &c.Subdomains })},
		{"CRAWLER_MONITOR_LISTEN", setString(func(c *Config) *string { return &c.Monitor.Listen })},
		{"CRAWLER_MONITOR_ZONE_STATS_INTERVAL", setDuration(func(c *Config) *Duration { return &c.Monitor.ZoneStatsInterval })},
		{"CRAWLER_MONITOR_ALERTS_INTERVAL", setDuration(func(c *Config) *Duration { return &c.Monitor.AlertsInterval })},
		// Mapping missed at T-173: the variable was declared in the docs
		// monitor table of docs/configuration.md — docs and load.go diverged.
		{"CRAWLER_MONITOR_LOAD_REPORT_INTERVAL", setDuration(func(c *Config) *Duration { return &c.Monitor.LoadReportInterval })},
		{"CRAWLER_MONITOR_SLOW_QUERY_INTERVAL", setDuration(func(c *Config) *Duration { return &c.Monitor.SlowQueryInterval })},
		{"CRAWLER_MONITOR_SLOW_QUERY_MIN_DURATION", setDuration(func(c *Config) *Duration { return &c.Monitor.SlowQueryMinDuration })},
		{"CRAWLER_MONITOR_SLOW_QUERY_TELEGRAM", setBool(func(c *Config) *bool { return &c.Monitor.SlowQueryTelegram })},
		{"CRAWLER_MONITOR_RENDER_DEBT_INTERVAL", setDuration(func(c *Config) *Duration { return &c.Monitor.RenderDebtInterval })},
		{"CRAWLER_MONITOR_INGEST_METRICS_URL", setString(func(c *Config) *string { return &c.Monitor.IngestMetricsURL })},
		{"CRAWLER_MONITOR_RENDER_METRICS_URL", setString(func(c *Config) *string { return &c.Monitor.RenderMetricsURL })},
		{"CRAWLER_MONITOR_ALERTS_TELEGRAM", setBool(func(c *Config) *bool { return &c.Monitor.Alerts.Telegram })},
		{"CRAWLER_MONITOR_ALERTS_TELEGRAM_SCRIPT", setString(func(c *Config) *string { return &c.Monitor.Alerts.TelegramScript })},
		{"CRAWLER_MONITOR_ALERTS_DISK_FREE_MIN_BYTES", setSize(func(c *Config) *ByteSize { return &c.Monitor.Alerts.DiskFreeMinBytes })},
		{"CRAWLER_MONITOR_ALERTS_DISK_FREE_CRIT_BYTES", setSize(func(c *Config) *ByteSize { return &c.Monitor.Alerts.DiskFreeCritBytes })},
		{"CRAWLER_MONITOR_ALERTS_QUEUE_FREEZE_WINDOW", setDuration(func(c *Config) *Duration { return &c.Monitor.Alerts.QueueFreeze })},
		{"CRAWLER_MONITOR_ALERTS_PSI_CPU_PERCENT", setFloat(func(c *Config) *float64 { return &c.Monitor.Alerts.PSICPUPercent })},
		{"CRAWLER_MONITOR_ALERTS_PSI_CPU_WINDOW", setDuration(func(c *Config) *Duration { return &c.Monitor.Alerts.PSIWindow })},
		{"CRAWLER_MONITOR_ALERTS_MEM_HIGH_PASSES", setInt(func(c *Config) *int { return &c.Monitor.Alerts.MemHighPasses })},
		{"CRAWLER_MONITOR_ALERTS_RENDER_DEBT_DOMAINS", setInt64(func(c *Config) *int64 { return &c.Monitor.Alerts.RenderDebtDomains })},
		{"CRAWLER_MONITOR_ALERTS_RENDER_DEBT_PASSES", setInt(func(c *Config) *int { return &c.Monitor.Alerts.RenderDebtPasses })},
		{"CRAWLER_WEB_LISTEN", setString(func(c *Config) *string { return &c.Web.Listen })},
		{"CRAWLER_WEB_RATE_LIMIT", setInt(func(c *Config) *int { return &c.Web.RateLimit })},
		{"CRAWLER_WEB_LIVE_CONCURRENCY", setInt(func(c *Config) *int { return &c.Web.LiveConcurrency })},
		{"CRAWLER_WEB_TIMEOUT", setDuration(func(c *Config) *Duration { return &c.Web.Timeout })},
		{"CRAWLER_WEB_PUBLIC_HOST", setString(func(c *Config) *string { return &c.Web.PublicHost })},
		{"CRAWLER_DASHBOARD_LISTEN", setString(func(c *Config) *string { return &c.Dashboard.Listen })},
		{"CRAWLER_DASHBOARD_REFRESH_INTERVAL", setDuration(func(c *Config) *Duration { return &c.Dashboard.Refresh })},
		{"CRAWLER_DASHBOARD_STATUS_CACHE_TTL", setDuration(func(c *Config) *Duration { return &c.Dashboard.StatusCacheTTL })},
		{"CRAWLER_DASHBOARD_USERNAME", setString(func(c *Config) *string { return &c.Dashboard.Username })},
		{"CRAWLER_DASHBOARD_PASSWORD_HASH", setString(func(c *Config) *string { return &c.Dashboard.PasswordHash })},
		{"CRAWLER_DASHBOARD_PROBES", setStringMap(func(c *Config) *map[string]string { return &c.Dashboard.Probes })},
		{"CRAWLER_DASHBOARD_RENDER_METRICS_URLS", setStrings(func(c *Config) *[]string { return &c.Dashboard.RenderMetrics })},
		{"CRAWLER_LOG_LEVEL", setString(func(c *Config) *string { return &c.Log.Level })},
	}
}

// applyEnv overlays non-empty environment variable values on top of the YAML.
func (c *Config) applyEnv(getenv func(string) string) error {
	var errs []error
	for _, s := range envSpecs() {
		v := getenv(s.key)
		if v == "" {
			continue // an empty variable value counts as unset
		}
		if err := s.set(c, v); err != nil {
			errs = append(errs, fmt.Errorf("env var %s: %w", s.key, err))
		}
	}
	// Deprecated APP_PG_DSN (deploy/docker/compose.yaml, Makefile):
	// applied only when the new CRAWLER_POSTGRES_DSN is not set.
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
			return fmt.Errorf("expected an integer, got %q", v)
		}
		*get(c) = n
		return nil
	}
}

func setInt64(get func(*Config) *int64) func(*Config, string) error {
	return func(c *Config, v string) error {
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return fmt.Errorf("expected an integer, got %q", v)
		}
		*get(c) = n
		return nil
	}
}

func setFloat(get func(*Config) *float64) func(*Config, string) error {
	return func(c *Config, v string) error {
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return fmt.Errorf("expected a number, got %q", v)
		}
		*get(c) = f
		return nil
	}
}

func setBool(get func(*Config) *bool) func(*Config, string) error {
	return func(c *Config, v string) error {
		b, err := strconv.ParseBool(strings.TrimSpace(v))
		if err != nil {
			return fmt.Errorf("expected a boolean (true/false/1/0), got %q", v)
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

// setStrings parses a comma-separated list; leading/trailing spaces of the
// elements are trimmed, empty elements are preserved (for subdomains an empty
// element means the domain apex). The value replaces the YAML list entirely.
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

// setZoneNames parses a comma-separated list of zone names (xn--p1ai,net) —
// following the pattern of empty map elements (setZoneWeights): an empty zone
// name is impossible by definition, a trailing comma and a templated
// "${ZONES:-xn--p1ai,}" must not crash the start; the value replaces
// the YAML list entirely. Case folding is normalize's job, the name shape
// and duplicates are checked by validation; a zone contains no comma, so the
// separator is unambiguous.
func setZoneNames(get func(*Config) *[]string) func(*Config, string) error {
	return func(c *Config, v string) error {
		parts := make([]string, 0, 4)
		for _, part := range strings.Split(v, ",") {
			if part = strings.TrimSpace(part); part != "" {
				parts = append(parts, part)
			}
		}
		*get(c) = parts
		return nil
	}
}

// setZoneWeights parses a "zone name = weight" map, comma-separated
// (com=5, xn--p1ai=2); the value replaces the YAML map entirely (like
// the lists). The name is lower-cased, the weight is an integer; empty
// elements (a trailing comma) are skipped, the weight range and the name
// shape are checked by validation. A zone contains no "=" and no "," (a DNS
// name), so the separators are unambiguous.
func setZoneWeights(get func(*Config) *map[string]int) func(*Config, string) error {
	return func(c *Config, v string) error {
		m := make(map[string]int)
		for _, part := range strings.Split(v, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			name, weight, ok := strings.Cut(part, "=")
			if !ok {
				return fmt.Errorf("expected a comma-separated zone=weight map (com=5,org=2), got %q", part)
			}
			n, err := strconv.Atoi(strings.TrimSpace(weight))
			if err != nil {
				return fmt.Errorf("zone %s weight: expected an integer, got %q", strings.TrimSpace(name), weight)
			}
			m[strings.ToLower(strings.TrimSpace(name))] = n
		}
		*get(c) = m
		return nil
	}
}

// setPerIPOverrides parses a "CIDR = limit" map, comma-separated
// (195.208.0.0/16=12, 2a02:0000::/32=8) — following the pattern of the zone
// weights (T-106); the value replaces the YAML map entirely. The key
// is normalized to the masked canonical CIDR (netip): "195.208.9.9/16"
// and "195.208.0.0/16" are one network; empty elements (a trailing comma)
// are skipped. CIDR parseability and the limit range are checked by
// validation; a CIDR contains no "=" and no ",", so the separators are
// unambiguous.
func setPerIPOverrides(get func(*Config) *map[string]int) func(*Config, string) error {
	return func(c *Config, v string) error {
		m := make(map[string]int)
		for _, part := range strings.Split(v, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			cidr, limit, ok := strings.Cut(part, "=")
			if !ok {
				return fmt.Errorf("expected a comma-separated CIDR=limit map (195.208.0.0/16=12), got %q", part)
			}
			prefix, err := netip.ParsePrefix(strings.TrimSpace(cidr))
			if err != nil {
				return fmt.Errorf("per-IP limit %s: expected an IPv4/IPv6 CIDR, got %q", strings.TrimSpace(cidr), cidr)
			}
			n, err := strconv.Atoi(strings.TrimSpace(limit))
			if err != nil {
				return fmt.Errorf("per-IP limit %s: expected an integer, got %q", strings.TrimSpace(cidr), limit)
			}
			m[prefix.Masked().String()] = n
		}
		*get(c) = m
		return nil
	}
}

// setStringMap parses a "name = value" map, comma-separated
// (mon=http://m:9101, api=http://a:9100) — following the pattern of the zone
// weights (T-106); the value replaces the YAML map entirely, the name is
// lower-cased, the value edges are trimmed, empty elements (a trailing
// comma) are skipped, the value shape is checked by validation. The split is
// on the first "=": an "=" in the value after it is allowed (a URL with a
// query); the value cannot contain a comma (the element separator, the same
// restriction as for the zone weights) — the base metric URLs contain no
// commas.
func setStringMap(get func(*Config) *map[string]string) func(*Config, string) error {
	return func(c *Config, v string) error {
		m := make(map[string]string)
		for _, part := range strings.Split(v, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			name, val, ok := strings.Cut(part, "=")
			if !ok {
				return fmt.Errorf("expected a comma-separated name=value map (mon=http://m:9101), got %q", part)
			}
			m[strings.ToLower(strings.TrimSpace(name))] = strings.TrimSpace(val)
		}
		*get(c) = m
		return nil
	}
}

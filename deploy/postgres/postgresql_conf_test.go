// Package postgres — the production PostgreSQL configuration for the
// systemd host (deploy/postgres/postgresql.conf, spec §6.3/§3.2/§6). The
// file is a deploy artifact of stage 9; the unit test guards its format
// (the GUC parameter syntax, no duplicates) and the key values of the
// spec contract from an unnoticed regression — the configuration is not
// compiled and without the check it silently drives to the production
// server.
package postgres

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// confFile lies next to the test: go test runs with cwd = the package
// directory (in the container make test — /src/deploy/postgres).
const confFile = "postgresql.conf"

// settingRe — the "name = value[ # comment]" line in the postgresql.conf
// syntax.
var settingRe = regexp.MustCompile(`^([a-z][a-z0-9_]*)\s*=\s*(\S.*)$`)

// parseConf parses the configuration into a name→value map (without the
// quotes and the tail comment). A '#' comment that started outside the
// value quotes is cut off — as PostgreSQL itself does it.
func parseConf(t *testing.T, content string) map[string]string {
	t.Helper()
	settings := make(map[string]string)
	for i, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		m := settingRe.FindStringSubmatch(trimmed)
		if m == nil {
			t.Errorf("line %d is not \"name = value\": %q", i+1, line)
			continue
		}
		name, value := m[1], m[2]
		if _, dup := settings[name]; dup {
			t.Errorf("the parameter %s is set again (line %d)", name, i+1)
			continue
		}
		settings[name] = stripComment(unquote(value))
	}
	return settings
}

// stripComment cuts the tail comment "# …" outside the value quotes.
func stripComment(value string) string {
	inQuote := false
	for i, r := range value {
		switch r {
		case '\'':
			inQuote = !inQuote
		case '#':
			if !inQuote {
				return strings.TrimSpace(value[:i])
			}
		}
	}
	return strings.TrimSpace(value)
}

// unquote removes the single quotes of the value ('localhost' →
// localhost).
func unquote(value string) string {
	if len(value) >= 2 && strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
		return value[1 : len(value)-1]
	}
	return value
}

// parseBytes parses a size in a PostgreSQL entry (16MB, 2GB, …) into
// bytes.
func parseBytes(t *testing.T, value string) int64 {
	t.Helper()
	m := regexp.MustCompile(`^(\d+)(B|kB|MB|GB)$`).FindStringSubmatch(value)
	if m == nil {
		t.Fatalf("the size %q does not match \"<number>B|kB|MB|GB\"", value)
	}
	units := map[string]int64{"B": 1, "kB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30}
	n, _ := strconv.ParseInt(m[1], 10, 64)
	return n * units[m[2]]
}

func readConf(t *testing.T) map[string]string {
	t.Helper()
	content, err := os.ReadFile(confFile)
	if err != nil {
		t.Fatalf("read %s: %v", confFile, err)
	}
	if len(content) == 0 {
		t.Fatalf("%s is empty", confFile)
	}
	return parseConf(t, string(content))
}

// TestFormat — every significant line is "name = value", there are no
// duplicates.
func TestFormat(t *testing.T) {
	settings := readConf(t)
	if len(settings) < 20 {
		t.Fatalf("suspiciously few parameters: %d", len(settings))
	}
}

// TestRequiredSettings — the contract values of spec §6.3 (the NVMe
// scheduler, autovacuum, synchronous_commit), §3.2 (the memory budget)
// and §6 (the WAL).
func TestRequiredSettings(t *testing.T) {
	settings := readConf(t)
	required := map[string]string{
		"listen_addresses":                      "localhost",
		"max_connections":                       "50",
		"shared_buffers":                        "2GB",
		"autovacuum":                            "on",
		"autovacuum_naptime":                    "30s",
		"autovacuum_max_workers":                "4",
		"autovacuum_vacuum_scale_factor":        "0.02",
		"autovacuum_vacuum_insert_scale_factor": "0.02",
		"autovacuum_analyze_scale_factor":       "0.01",
		"autovacuum_vacuum_cost_limit":          "2000",
		"synchronous_commit":                    "off",
		"wal_compression":                       "on",
		"checkpoint_timeout":                    "15min",
		"checkpoint_completion_target":          "0.9",
		"random_page_cost":                      "1.1",
		"maintenance_io_concurrency":            "200",
		"shared_preload_libraries":              "pg_stat_statements",
		// T-177: the slow-query log threshold = the threshold of the
		// monitor loop (monitor.slow_query_min_duration) — one
		// "pathology" boundary.
		"log_min_duration_statement": "3000",
		// T-192: the lock waits longer than deadlock_timeout go into the
		// log ("process N still waiting …"): the name of the holder of the
		// convoy of the domain rows in the next incident (25.10 — not
		// recoverable).
		"log_lock_waits": "on",
	}
	for name, want := range required {
		if got := settings[name]; got != want {
			t.Errorf("%s = %q, want %q (spec)", name, got, want)
		}
	}
}

// TestRanges — the spec requirements set by the inequalities and the
// budget corridors: effective_io_concurrency ≥ 200 (§6.3), the autovacuum
// factors below the PostgreSQL defaults (0.2/0.2/0.1) under the
// partitions of 30–50 million rows, work_mem — per backends within the
// budget of §3.2, max_wal_size — rare checkpoints under NVMe (§10.3).
func TestRanges(t *testing.T) {
	settings := readConf(t)

	if n, err := strconv.Atoi(settings["effective_io_concurrency"]); err != nil || n < 200 {
		t.Errorf("effective_io_concurrency = %q, want ≥ 200 (spec §6.3)", settings["effective_io_concurrency"])
	}

	for name, def := range map[string]float64{
		"autovacuum_vacuum_scale_factor":        0.2,
		"autovacuum_vacuum_insert_scale_factor": 0.2,
		"autovacuum_analyze_scale_factor":       0.1,
	} {
		v, err := strconv.ParseFloat(settings[name], 64)
		if err != nil {
			t.Errorf("%s is not a number: %v", name, err)
			continue
		}
		if v >= def {
			t.Errorf("%s = %v is not below the PostgreSQL default %v", name, v, def)
		}
	}

	// The budget of §3.2: shared_buffers ≈ 2 GB, the whole instance ≈
	// 2.5 GB.
	if got := parseBytes(t, settings["shared_buffers"]); got < 2<<30 || got > 3<<30 {
		t.Errorf("shared_buffers = %d bytes, the §3.2 budget corridor is 2–3 GB", got)
	}
	if got := parseBytes(t, settings["work_mem"]); got > 32<<20 {
		t.Errorf("work_mem = %d bytes: above 32 MB it breaks out of the §3.2 backend budget", got)
	}
	if got := parseBytes(t, settings["max_wal_size"]); got < 4<<30 {
		t.Errorf("max_wal_size = %d bytes: on NVMe with plenty of disk (§3.1/§10.3) a margin of ≥ 4 GB is needed", got)
	}
}

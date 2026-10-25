// Package postgres — боевая конфигурация PostgreSQL для systemd-хоста
// (deploy/postgres/postgresql.conf, ТЗ §6.3/§3.2/§6). Файл — deploy-артефакт
// этапа 9; юнит-тест охраняет его формат (синтаксис параметров GUC, отсутствие
// дублей) и ключевые значения контракта ТЗ от незаметной регрессии —
// конфигурация не компилируется и без проверки молча едет на боевой сервер.
package postgres

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// confFile лежит рядом с тестом: go test запускается с cwd = каталог пакета
// (в контейнере make test — /src/deploy/postgres).
const confFile = "postgresql.conf"

// settingRe — строка «name = value[ # комментарий]» в синтаксисе postgresql.conf.
var settingRe = regexp.MustCompile(`^([a-z][a-z0-9_]*)\s*=\s*(\S.*)$`)

// parseConf разбирает конфигурацию в map имя→значение (без кавычек и
// хвостового комментария). Комментарий '#', начавшийся вне кавычек значения,
// отрезается — как это делает сам PostgreSQL.
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
			t.Errorf("строка %d не является «name = value»: %q", i+1, line)
			continue
		}
		name, value := m[1], m[2]
		if _, dup := settings[name]; dup {
			t.Errorf("параметр %s задаётся повторно (строка %d)", name, i+1)
			continue
		}
		settings[name] = stripComment(unquote(value))
	}
	return settings
}

// stripComment отрезает хвостовой комментарий «# …» вне кавычек значения.
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

// unquote убирает одиночные кавычки значения ('localhost' → localhost).
func unquote(value string) string {
	if len(value) >= 2 && strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
		return value[1 : len(value)-1]
	}
	return value
}

// parseBytes разбирает размер в записи PostgreSQL (16MB, 2GB, …) в байты.
func parseBytes(t *testing.T, value string) int64 {
	t.Helper()
	m := regexp.MustCompile(`^(\d+)(B|kB|MB|GB)$`).FindStringSubmatch(value)
	if m == nil {
		t.Fatalf("размер %q не по форме «<число>B|kB|MB|GB»", value)
	}
	units := map[string]int64{"B": 1, "kB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30}
	n, _ := strconv.ParseInt(m[1], 10, 64)
	return n * units[m[2]]
}

func readConf(t *testing.T) map[string]string {
	t.Helper()
	content, err := os.ReadFile(confFile)
	if err != nil {
		t.Fatalf("прочитать %s: %v", confFile, err)
	}
	if len(content) == 0 {
		t.Fatalf("%s пуст", confFile)
	}
	return parseConf(t, string(content))
}

// TestFormat — каждая значимая строка это «name = value», дубликатов нет.
func TestFormat(t *testing.T) {
	settings := readConf(t)
	if len(settings) < 20 {
		t.Fatalf("подозрительно мало параметров: %d", len(settings))
	}
}

// TestRequiredSettings — контрактные значения ТЗ §6.3 (планировщик NVMe,
// autovacuum, synchronous_commit), §3.2 (бюджет памяти) и §6 (WAL).
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
		// T-177: порог лога медленных запросов = порогу цикла monitor
		// (monitor.slow_query_min_duration) — одна граница «патология».
		"log_min_duration_statement": "3000",
		// T-192: ожидания блокировок дольше deadlock_timeout — в журнал
		// («process N still waiting …»): имя держателя конвоя доменных
		// строк в следующем инциденте (25.10 — не восстановим).
		"log_lock_waits": "on",
	}
	for name, want := range required {
		if got := settings[name]; got != want {
			t.Errorf("%s = %q, ожидается %q (ТЗ)", name, got, want)
		}
	}
}

// TestRanges — требования ТЗ, заданные неравенствами, и бюджетные коридоры:
// effective_io_concurrency ≥ 200 (§6.3), факторы autovacuum ниже дефолтов
// PostgreSQL (0.2/0.2/0.1) под партиции 30–50 млн строк, work_mem — на бэкенды
// в бюджет §3.2, max_wal_size — редкие чекпойнты под NVMe (§10.3).
func TestRanges(t *testing.T) {
	settings := readConf(t)

	if n, err := strconv.Atoi(settings["effective_io_concurrency"]); err != nil || n < 200 {
		t.Errorf("effective_io_concurrency = %q, ожидается ≥ 200 (ТЗ §6.3)", settings["effective_io_concurrency"])
	}

	for name, def := range map[string]float64{
		"autovacuum_vacuum_scale_factor":        0.2,
		"autovacuum_vacuum_insert_scale_factor": 0.2,
		"autovacuum_analyze_scale_factor":       0.1,
	} {
		v, err := strconv.ParseFloat(settings[name], 64)
		if err != nil {
			t.Errorf("%s не число: %v", name, err)
			continue
		}
		if v >= def {
			t.Errorf("%s = %v не ниже дефолта PostgreSQL %v", name, v, def)
		}
	}

	// Бюджет §3.2: shared_buffers ≈ 2 ГБ, весь инстанс ≈ 2,5 ГБ.
	if got := parseBytes(t, settings["shared_buffers"]); got < 2<<30 || got > 3<<30 {
		t.Errorf("shared_buffers = %d байт, коридор бюджета §3.2 — 2–3 ГБ", got)
	}
	if got := parseBytes(t, settings["work_mem"]); got > 32<<20 {
		t.Errorf("work_mem = %d байт: выше 32 МБ вылезает из бюджета бэкендов §3.2", got)
	}
	if got := parseBytes(t, settings["max_wal_size"]); got < 4<<30 {
		t.Errorf("max_wal_size = %d байт: под NVMe и «места много» (§3.1/§10.3) нужен запас ≥ 4 ГБ", got)
	}
}

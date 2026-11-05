// Юнит-тесты crawlerctl fixtures export (T-242): роутинг/флаги/справка —
// без инфраструктуры (валидный вызов доходит до конфигурации и падает
// детерминированно, стиль TestRunZonesCollapseExecution); пропорции квот
// (метод наибольших остатков), smoke-подвыборка (детерминизм, имена),
// NDJSON-кодирование строк (контракт формата v1: NULL → null, bytea →
// base64, jsonb как есть), коллектор дублей и SQL-контракты выборки
// (подстроки предикатов — стиль renderq_test.go). Живой прогон выгрузки
// против дев-БД — самопроверка задачи (результаты — в отчёте), не тест.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// TestRunFixtures — использование команды: справка, неизвестное действие,
// валидация флагов до соединения с БД (код 2), отсутствие конфигурации
// после валидации (код 1).
func TestRunFixtures(t *testing.T) {
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	cases := []runCase{
		{name: "fixtures без действия", args: []string{"fixtures"}, code: exitUsage, inErr: "укажите действие"},
		{name: "fixtures неизвестное действие", args: []string{"fixtures", "restore"}, code: exitUsage, inErr: `допустимы: export, import`},
		{name: "fixtures --help", args: []string{"fixtures", "--help"}, code: exitOK, inOut: "фикстуры стенда сравнения движков рендера"},
		{name: "fixtures help", args: []string{"fixtures", "help"}, code: exitOK, inOut: "fixtures export --out"},
		{name: "export --help", args: []string{"fixtures", "export", "--help"}, code: exitOK, inOut: "--seed N"},
		{name: "import --help", args: []string{"fixtures", "import", "--help"}, code: exitOK, inOut: "--publish РЕЖИМ"},
		{name: "import help", args: []string{"fixtures", "import", "help"}, code: exitOK, inOut: "fixtures import --dir"},
		{name: "export без --out", args: []string{"fixtures", "export"}, code: exitUsage, inErr: "укажите каталог дампа"},
		{name: "export --out пустой", args: []string{"fixtures", "export", "--out", "  "}, code: exitUsage, inErr: "укажите каталог дампа"},
		{name: "export лишний аргумент", args: []string{"fixtures", "export", "--out", "d", "stray"}, code: exitUsage, inErr: "лишние аргументы"},
		{name: "export отрицательная квота", args: []string{"fixtures", "export", "--out", "d", "--ok", "-1"},
			code: exitUsage, inErr: "--ok"},
		{name: "export нулевые страты", args: []string{"fixtures", "export", "--out", "d", "--ok", "0", "--failed", "0", "--heavy", "0"},
			code: exitUsage, inErr: "сумма квот страт нулевая"},
		{name: "export weeks 0", args: []string{"fixtures", "export", "--out", "d", "--weeks", "0"},
			code: exitUsage, inErr: "--weeks"},
		{name: "export валидный — нет конфигурации", args: []string{"fixtures", "export", "--out", "d"},
			code: exitError, inErr: "fixtures export:"},
		{name: "import без --dir", args: []string{"fixtures", "import"}, code: exitUsage, inErr: "укажите каталог дампа"},
		{name: "import --dir пустой", args: []string{"fixtures", "import", "--dir", " "}, code: exitUsage, inErr: "укажите каталог дампа"},
		{name: "import --publish мусор", args: []string{"fixtures", "import", "--dir", "d", "--publish", "some"},
			code: exitUsage, inErr: "--publish"},
		{name: "import лишний аргумент", args: []string{"fixtures", "import", "--dir", "d", "stray"},
			code: exitUsage, inErr: "лишние аргументы"},
		{name: "import валидный — нет конфигурации", args: []string{"fixtures", "import", "--dir", "d"},
			code: exitError, inErr: "fixtures import:"},
		{name: "import валидный с публикацией — нет конфигурации", args: []string{"fixtures", "import", "--dir", "d", "--publish", "smoke"},
			code: exitError, inErr: "fixtures import:"},
		{name: "справка верхнего уровня упоминает fixtures", args: []string{"--help"},
			code: exitOK, inOut: "fixtures export --out"},
		{name: "справка верхнего уровня упоминает fixtures import", args: []string{"--help"},
			code: exitOK, inOut: "fixtures import --dir"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tc.args, &stdout, &stderr)
			if code != tc.code {
				t.Errorf("run(%q) код = %d, хочу %d; stderr:\n%s", tc.args, code, tc.code, stderr.String())
			}
			if tc.inErr != "" && !strings.Contains(stderr.String(), tc.inErr) {
				t.Errorf("run(%q) stderr не содержит %q:\n%s", tc.args, tc.inErr, stderr.String())
			}
			if tc.inOut != "" && !strings.Contains(stdout.String(), tc.inOut) {
				t.Errorf("run(%q) stdout не содержит %q:\n%s", tc.args, tc.inOut, stdout.String())
			}
		})
	}
}

// TestFixturesAllocateQuotas — метод наибольших остатков: точные доли,
// добор остатка, детерминизм при равных долях, вырожденные случаи.
func TestFixturesAllocateQuotas(t *testing.T) {
	t.Run("точные доли без остатка", func(t *testing.T) {
		got := fixturesAllocateQuotas(map[int16]int64{1: 100, 2: 300}, 8)
		want := map[int16]int{1: 2, 2: 6}
		if len(got) != len(want) {
			t.Fatalf("квоты = %v, хочу %v", got, want)
		}
		for z, q := range want {
			if got[z] != q {
				t.Errorf("зона %d: квота %d, хочу %d (все: %v)", z, got[z], q, got)
			}
		}
	})

	t.Run("добор наибольшим остатком", func(t *testing.T) {
		// 100/(100+100+100+1)*10: базы 2,2,2,0; остаток 2 — зоны с большей
		// дробной частью (последняя: 0.098) и, при равенстве, меньший zone_id.
		got := fixturesAllocateQuotas(map[int16]int64{1: 100, 2: 100, 3: 100, 4: 1}, 10)
		if total := got[1] + got[2] + got[3] + got[4]; total != 10 {
			t.Fatalf("сумма квот = %d, хочу 10 (%v)", total, got)
		}
		if got[4] > 1 {
			t.Errorf("зона с 1 строкой получила квоту %d (> доступного)", got[4])
		}
		// Три равные зоны делят 10 как 4/3/3 (+1 наименьшему id).
		if got[1] != 4 || got[2] != 3 || got[3] != 3 {
			t.Errorf("равные зоны: квоты %d/%d/%d, хочу 4/3/3", got[1], got[2], got[3])
		}
	})

	t.Run("квота зоны не выше доступного", func(t *testing.T) {
		got := fixturesAllocateQuotas(map[int16]int64{7: 3}, 100)
		if got[7] != 3 {
			t.Errorf("квота = %d, хочу 3 (доступно 3)", got[7])
		}
	})

	t.Run("вырожденные", func(t *testing.T) {
		if got := fixturesAllocateQuotas(nil, 10); len(got) != 0 {
			t.Errorf("пустые счётчики: %v", got)
		}
		if got := fixturesAllocateQuotas(map[int16]int64{1: 5}, 0); len(got) != 0 {
			t.Errorf("нулевой total: %v", got)
		}
		if got := fixturesAllocateQuotas(map[int16]int64{1: 0, 2: 4}, 4); len(got) != 1 || got[2] != 4 {
			t.Errorf("нулевая зона не участвует: %v", got)
		}
	})

	t.Run("сумма равна total", func(t *testing.T) {
		counts := map[int16]int64{1: 7, 2: 13, 3: 31, 4: 5, 5: 44}
		for _, total := range []int{1, 2, 7, 33, 100, 1000} {
			got := fixturesAllocateQuotas(counts, total)
			sum := 0
			for _, q := range got {
				sum += q
			}
			available := 0
			for _, n := range counts {
				available += int(n)
			}
			want := total
			if want > available {
				want = available
			}
			if sum != want {
				t.Errorf("total %d: сумма квот %d, хочу %d (%v)", total, sum, want, got)
			}
			for z, q := range got {
				if q > int(counts[z]) {
					t.Errorf("total %d: зона %d: квота %d > доступного %d", total, z, q, counts[z])
				}
			}
		}
	})
}

// TestFixturesCollect — коллектор выборки: метка страты, отсечение дублей
// ключей (строка не попадает в дамп дважды — идемпотентность наката).
func TestFixturesCollect(t *testing.T) {
	c := newFixturesCollect()
	c.add("ok", []fixturesResult{
		{ZoneID: 1, DomainID: 1, SubdomainID: 0, Variant: 0},
		{ZoneID: 1, DomainID: 1, SubdomainID: 1, Variant: 0},
	})
	c.add("heavy", []fixturesResult{
		{ZoneID: 1, DomainID: 1, SubdomainID: 0, Variant: 0}, // дубль ok
		{ZoneID: 1, DomainID: 2, SubdomainID: 0, Variant: 0},
	})
	if len(c.rows) != 3 {
		t.Fatalf("строк %d, хочу 3", len(c.rows))
	}
	if c.dups != 1 {
		t.Errorf("дублей %d, хочу 1", c.dups)
	}
	for i, want := range []string{"ok", "ok", "heavy"} {
		if c.rows[i].Stratum != want {
			t.Errorf("строка %d: страта %q, хочу %q", i, c.rows[i].Stratum, want)
		}
	}
}

// TestFixturesEncodeAll — контракт NDJSON формата v1: по объекту на
// строку, NULL-колонки → null, bytea → base64, jsonb → как есть, без
// экранирования HTML, time → RFC 3339.
func TestFixturesEncodeAll(t *testing.T) {
	ts := time.Date(2026, 11, 5, 12, 30, 0, 0, time.UTC)
	httpStatus := int16(200)
	url := "https://example.com/?a=1&b=<2>"
	rows := []fixturesResult{{
		ZoneID: 7, DomainID: 42, SubdomainID: 1, Variant: 1,
		Scheme: 2, HTTPStatus: &httpStatus, FinalURL: &url,
		Headers:     json.RawMessage(`{"content-type":["text/html"]}`),
		Cookies:     nil,
		Rendered:    true,
		ContentHash: []byte{0x01, 0x02, 0x03}, // base64: AQID
		CheckedAt:   ts,
		Stratum:     "ok",
	}}

	var buf bytes.Buffer
	if err := fixturesEncodeAll(&buf, rows); err != nil {
		t.Fatalf("кодирование: %v", err)
	}
	line := strings.TrimRight(buf.String(), "\n")
	if strings.Count(line, "\n") != 0 {
		t.Errorf("ожидаена одна строка NDJSON, получено:\n%s", line)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("строка не JSON: %v\n%s", err, line)
	}

	// bytea → base64
	if m["content_hash"] != "AQID" {
		t.Errorf("content_hash = %v, хочу AQID", m["content_hash"])
	}
	// jsonb → вложенный JSON; NULL jsonb → null
	if h, ok := m["headers"].(map[string]any); !ok || h["content-type"] == nil {
		t.Errorf("headers не вложенный JSON: %v", m["headers"])
	}
	if m["cookies"] != nil {
		t.Errorf("cookies = %v, хочу null", m["cookies"])
	}
	// NULL-колонки → null (не отсутствие поля)
	if m["render_failed_at"] != nil {
		t.Errorf("render_failed_at = %v, хочу null", m["render_failed_at"])
	}
	// без экранирования HTML: & и <> из final_url — как есть
	if !strings.Contains(line, "&b=<2>") {
		t.Errorf("final_url экранирован HTML в NDJSON:\n%s", line)
	}
	// время → RFC 3339
	if m["checked_at"] != "2026-11-05T12:30:00Z" {
		t.Errorf("checked_at = %v, хочу RFC 3339", m["checked_at"])
	}
	if m["stratum"] != "ok" {
		t.Errorf("stratum = %v, хочу ok", m["stratum"])
	}

	// NULL-строка: все опциональные поля отсутствуют.
	var nullLine bytes.Buffer
	if err := fixturesEncodeAll(&nullLine, []fixturesResult{{ZoneID: 1, DomainID: 2, SubdomainID: 0, Variant: 0, Scheme: 1, CheckedAt: ts, Stratum: "failed"}}); err != nil {
		t.Fatalf("кодирование NULL-строки: %v", err)
	}
	var nm map[string]any
	if err := json.Unmarshal(nullLine.Bytes(), &nm); err != nil {
		t.Fatalf("NULL-строка не JSON: %v", err)
	}
	for _, col := range []string{"http_status", "final_url", "alias_subdomain_id", "headers", "cookies", "content_hash", "error", "response_ms", "rules_version", "render_failed_at"} {
		v, ok := nm[col]
		if !ok {
			t.Errorf("колонка %s отсутствует в дампе (контракт: null, не отсутствие)", col)
		} else if v != nil {
			t.Errorf("колонка %s = %v, хочу null", col, v)
		}
	}
}

// TestFixturesDomainNDJSON — строка domains дампа: NULL-колонки → null,
// все колонки таблицы присутствуют (контракт наката T-243).
func TestFixturesDomainNDJSON(t *testing.T) {
	var buf bytes.Buffer
	if err := fixturesEncodeAll(&buf, []fixturesDomain{{ZoneID: 3, ID: 9, Label: "example", Status: 2, CheckState: 2, Flags: 1, DNSMask: 5, TempAttempts: 0}}); err != nil {
		t.Fatalf("кодирование: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("строка не JSON: %v", err)
	}
	for _, col := range []string{"zone_id", "id", "label", "status", "check_state", "flags", "dns_mask", "queued_at", "last_checked_at", "temp_attempts"} {
		if _, ok := m[col]; !ok {
			t.Errorf("колонка %s отсутствует в domains.ndjson", col)
		}
	}
	if m["queued_at"] != nil || m["last_checked_at"] != nil {
		t.Errorf("NULL-колонки домена = %v/%v, хочу null", m["queued_at"], m["last_checked_at"])
	}
}

// TestFixturesSmokePick — smoke-подвыборка: детерминизм (тот же seed —
// тот же порядок), лимит, имя label.zone, устойчивость к порядку строк.
func TestFixturesSmokePick(t *testing.T) {
	rows := []fixturesResult{
		{ZoneID: 7, DomainID: 1, SubdomainID: 0, Variant: 0},
		{ZoneID: 7, DomainID: 1, SubdomainID: 1, Variant: 0}, // тот же домен
		{ZoneID: 7, DomainID: 2, SubdomainID: 0, Variant: 0},
		{ZoneID: 3, DomainID: 5, SubdomainID: 0, Variant: 0},
	}
	domains := []fixturesDomain{
		{ZoneID: 7, ID: 1, Label: "alpha"},
		{ZoneID: 7, ID: 2, Label: "beta"},
		{ZoneID: 3, ID: 5, Label: "gamma"},
	}
	zones := []fixturesZone{{ID: 7, Name: "su"}, {ID: 3, Name: "ru"}}

	a := fixturesSmokePick(rows, domains, zones, "242", 2)
	b := fixturesSmokePick(rows, domains, zones, "242", 2)
	if len(a) != 2 || len(b) != 2 {
		t.Fatalf("smoke размер %d/%d, хочу 2", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Errorf("недетерминизм: %v против %v", a[i], b[i])
		}
		if !strings.HasSuffix(a[i].Name, ".su") && !strings.HasSuffix(a[i].Name, ".ru") {
			t.Errorf("имя smoke-домена %q без зоны", a[i].Name)
		}
	}

	// Все домены уникальны в выборке.
	seen := map[fixturesDomainKey]bool{}
	for _, s := range a {
		k := fixturesDomainKey{s.ZoneID, s.DomainID}
		if seen[k] {
			t.Errorf("домен %d/%d дважды в smoke", s.ZoneID, s.DomainID)
		}
		seen[k] = true
	}

	// Лимит больше уникальных доменов — все; нулевой — пусто.
	if got := fixturesSmokePick(rows, domains, zones, "242", 100); len(got) != 3 {
		t.Errorf("лимит 100: %d доменов, хочу 3 (уникальных)", len(got))
	}
	if got := fixturesSmokePick(rows, domains, zones, "242", 0); got != nil {
		t.Errorf("лимит 0: %v, хочу nil", got)
	}
}

// TestFixturesSQLContract — контракты SQL выборки: базовый предикат
// «реальная задача q:render» во всех стратах, свежесть failed, размер
// контента heavy, анти-join ok против heavy, детерминированный порядок
// md5, идентификаторы ключей в выборке доменов.
func TestFixturesSQLContract(t *testing.T) {
	base := "final_url IS NOT NULL"
	for name, sql := range map[string]string{
		"countOk":      fixturesCountOkSQL,
		"countFailed":  fixturesCountFailedSQL,
		"countHeavy":   fixturesCountHeavySQL,
		"selectOk":     fixturesSelectOkSQL,
		"selectHeavy":  fixturesSelectHeavySQL,
		"selectFailed": fixturesSelectFailedSQL,
	} {
		if !strings.Contains(sql, base) {
			t.Errorf("%s: нет базового предиката %q: %s", name, base, sql)
		}
		if !strings.HasPrefix(sql, "SELECT") {
			t.Errorf("%s: не SELECT: %s", name, sql)
		}
	}
	if !strings.Contains(fixturesCountOkSQL, "rendered") ||
		strings.Contains(fixturesCountOkSQL, "render_failed_at") {
		t.Errorf("countOk: страта ok = rendered без маркера: %s", fixturesCountOkSQL)
	}
	if !strings.Contains(fixturesCountFailedSQL, "render_failed_at IS NOT NULL") ||
		!strings.Contains(fixturesCountFailedSQL, "render_failed_at >= now() - make_interval(weeks => $1::int)") {
		t.Errorf("countFailed: маркер/окно свежести (по render_failed_at, не checked_at) потеряны: %s", fixturesCountFailedSQL)
	}
	if !strings.Contains(fixturesSelectFailedSQL, "r.render_failed_at >= now() - make_interval(weeks => $2::int)") {
		t.Errorf("selectFailed: окно свежести не по маркеру render_failed_at: %s", fixturesSelectFailedSQL)
	}
	if !strings.Contains(fixturesSelectHeavySQL, "JOIN contents AS c ON c.hash = r.content_hash") ||
		!strings.Contains(fixturesSelectHeavySQL, "ORDER BY c.raw_len DESC") {
		t.Errorf("selectHeavy: размер контента не приоритет: %s", fixturesSelectHeavySQL)
	}
	if !strings.Contains(fixturesSelectOkSQL, "AS ex(zone_id, domain_id, subdomain_id, variant)") ||
		!strings.Contains(fixturesSelectOkSQL, "ex.zone_id IS NULL") {
		t.Errorf("selectOk: анти-join против heavy потерян: %s", fixturesSelectOkSQL)
	}
	// Детерминированный порядок — во всех по-зонных выборках.
	for name, sql := range map[string]string{
		"ok": fixturesSelectOkSQL, "heavy": fixturesSelectHeavySQL, "failed": fixturesSelectFailedSQL,
	} {
		if !strings.Contains(sql, "ORDER BY") || !strings.Contains(sql, "md5($") {
			t.Errorf("%s: нет детерминированного md5-порядка: %s", name, sql)
		}
		if !strings.Contains(sql, "LIMIT $") {
			t.Errorf("%s: нет LIMIT квоты: %s", name, sql)
		}
	}
	if !strings.Contains(fixturesDomainsSQL, "SELECT DISTINCT k.zone_id, k.domain_id") ||
		!strings.Contains(fixturesDomainsSQL, "unnest($1::int2[], $2::int8[])") {
		t.Errorf("domainsSQL: ключи доменов не unnest-массивами: %s", fixturesDomainsSQL)
	}
	if !strings.Contains(fixturesZonesSQL, "id = ANY($1::int2[])") {
		t.Errorf("zonesSQL: зоны не массивом id: %s", fixturesZonesSQL)
	}
	// Колонки выборки: контракт формата (без addrs/cname/tls/redirects).
	for _, banned := range []string{"r.addrs", "r.cname", "r.tls", "r.redirects"} {
		if strings.Contains(fixturesResultCols, banned) {
			t.Errorf("fixturesResultCols: обезличиваемая колонка %s в выборке", banned)
		}
	}
	for _, need := range []string{"r.final_url", "r.headers", "r.cookies", "r.alias_kind", "r.render_failed_at", "r.content_hash"} {
		if !strings.Contains(fixturesResultCols, need) {
			t.Errorf("fixturesResultCols: нет колонки %s контракта рендера", need)
		}
	}
}

// TestFixturesWriteFile — атомарная запись: файл появляется под финальным
// именем, .tmp не остаётся; ошибка записи не оставляет полуфайла.
func TestFixturesWriteFile(t *testing.T) {
	dir := t.TempDir()
	write := func(s string) error {
		return fixturesWriteFile(dir, "a.ndjson", func(w *bufio.Writer) error {
			_, err := w.WriteString(s)
			return err
		})
	}
	if err := write("строка 1\n"); err != nil {
		t.Fatalf("запись: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(dir, "a.ndjson")); err != nil || string(b) != "строка 1\n" {
		t.Fatalf("файл после записи: %q, %v", b, err)
	}
	if fi, err := os.Stat(filepath.Join(dir, "a.ndjson")); err == nil && fi.Mode().Perm() != 0o600 {
		t.Errorf("режим файла %v, хочу 0600 (дамп приватен)", fi.Mode().Perm())
	}
	if _, err := os.Stat(filepath.Join(dir, "a.ndjson.tmp")); !os.IsNotExist(err) {
		t.Errorf(".tmp остался после успешной записи: %v", err)
	}

	// Ошибка функции записи: полуфайл под финальным именем не появляется,
	// старое содержимое не портится (переименование не выполняется).
	fail := fixturesWriteFile(dir, "a.ndjson", func(w *bufio.Writer) error {
		_, _ = w.WriteString("мусор")
		return errors.New("прервано")
	})
	if fail == nil {
		t.Fatal("ошибка записи не возвращена")
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "a.ndjson")); string(b) != "строка 1\n" {
		t.Errorf("содержимое испорчено прерванной записью: %q", b)
	}
	if _, err := os.Stat(filepath.Join(dir, "a.ndjson.tmp")); !os.IsNotExist(err) {
		t.Errorf(".tmp остался после прерванной записи: %v", err)
	}
}

// ---------------------------------------------------------------- оркестрация

// fixturesRows — pgx.Rows с заготовленными строками выборки фикстуры
// (приёмники — типы полей fixtures*, включая указатели NULL-колонок;
// общий statusRows их не поддерживает — здесь свой набор, стиль T-079).
type fixturesRows struct {
	rows [][]any
	i    int
}

func (r *fixturesRows) Close()                                       {}
func (r *fixturesRows) Err() error                                   { return nil }
func (r *fixturesRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fixturesRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fixturesRows) Values() ([]any, error)                       { return r.rows[r.i-1], nil }
func (r *fixturesRows) RawValues() [][]byte                          { return nil }
func (r *fixturesRows) Conn() *pgx.Conn                              { return nil }
func (r *fixturesRows) TypeMap() *pgtype.Map                         { return nil }

func (r *fixturesRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *fixturesRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("fixturesRows: Scan без Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("fixturesRows: приёмников %d, колонок %d", len(dest), len(row))
	}
	set := func(d, v any) error {
		switch dst := d.(type) {
		case *int16:
			*dst = v.(int16)
		case *int32:
			*dst = v.(int32)
		case *int64:
			*dst = v.(int64)
		case *string:
			*dst = v.(string)
		case *bool:
			*dst = v.(bool)
		case *[]byte:
			if v == nil {
				*dst = nil
			} else {
				*dst = v.([]byte)
			}
		case *json.RawMessage:
			if v == nil {
				*dst = nil
			} else {
				*dst = json.RawMessage(v.([]byte))
			}
		case *time.Time:
			*dst = v.(time.Time)
		case **int16:
			if v == nil {
				*dst = nil
			} else {
				x := v.(int16)
				*dst = &x
			}
		case **int32:
			if v == nil {
				*dst = nil
			} else {
				x := v.(int32)
				*dst = &x
			}
		case **string:
			if v == nil {
				*dst = nil
			} else {
				x := v.(string)
				*dst = &x
			}
		case **time.Time:
			if v == nil {
				*dst = nil
			} else {
				x := v.(time.Time)
				*dst = &x
			}
		default:
			return fmt.Errorf("fixturesRows: приёмник %T не поддерживается", d)
		}
		return nil
	}
	for i, d := range dest {
		if err := set(d, row[i]); err != nil {
			return err
		}
	}
	return nil
}

// fixturesPool — фейковый db.DB: маршрутизация по маркерам SQL выборки
// фикстуры. Сценарий: зона 7 (su) с двумя ok-строками, прочие страты
// пусты (деградация), один домен и одна зона.
type fixturesPool struct {
	failOn string // подстрока SQL: вернуть ошибку (имитация сбоя БД)
}

func (p *fixturesPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("fixturesPool: CopyFrom не вызывается")
}

func (p *fixturesPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("fixturesPool: Exec не вызывается (выгрузка — только SELECT)")
}

func (p *fixturesPool) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	if p.failOn != "" && strings.Contains(sql, p.failOn) {
		return nil, fmt.Errorf("фиктивный сбой БД (%s)", p.failOn)
	}
	ts := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	switch {
	case strings.Contains(sql, "LIMIT $7::int8"): // select ok
		url, hdr := "https://ok.example/", []byte(`{"a":["b"]}`)
		return &fixturesRows{rows: [][]any{
			{int16(7), int64(1), int16(0), int16(0), int16(2), nil, url, int16(0), nil,
				hdr, nil, true, []byte{1, 2}, nil, nil, ts, nil, nil},
			{int16(7), int64(1), int16(1), int16(0), int16(2), nil, url, int16(0), nil,
				hdr, nil, true, nil, nil, nil, ts, nil, nil},
		}}, nil
	case strings.Contains(sql, "count(*)") && strings.Contains(sql, "rendered"): // count ok
		return &fixturesRows{rows: [][]any{{int16(7), int64(2)}}}, nil
	case strings.Contains(sql, "count(*)"): // count heavy/failed — пусто
		return &fixturesRows{}, nil
	case strings.Contains(sql, "LIMIT $3::int8"), strings.Contains(sql, "LIMIT $4::int8"): // heavy/failed
		return &fixturesRows{}, nil
	case strings.Contains(sql, "FROM domains AS d"):
		return &fixturesRows{rows: [][]any{{int16(7), int64(1), "ok", int16(4), int16(2),
			int16(0), int64(3), nil, nil, int16(0)}}}, nil
	case strings.Contains(sql, "FROM zones WHERE"):
		return &fixturesRows{rows: [][]any{{int16(7), "su", nil, true, int64(2), int64(2), nil}}}, nil
	default:
		return nil, fmt.Errorf("fixturesPool: неизвестный SQL: %s", sql)
	}
}

// TestFixturesExportRun — оркестрация выгрузки на фейковом пуле: все пять
// файлов, манифест согласован со строками, деградации честные; повторный
// прогон поверх старого манифеста перезаписывает его; падение до записи
// файлов сохраняет прежний манифест, падение в середине записи — убирает
// (контракт «manifest есть ⇒ дамп завершён», ревью T-242 P1).
func TestFixturesExportRun(t *testing.T) {
	params := fixturesParams{OK: 5, Heavy: 5, Failed: 5, Smoke: 1, Weeks: 4, Seed: 242}

	t.Run("успех: файлы и манифест", func(t *testing.T) {
		dir := t.TempDir()
		params.OutDir = dir
		if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte("старый"), 0o600); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if _, err := fixturesExportRun(context.Background(), &fixturesPool{}, params, &out); err != nil {
			t.Fatalf("выгрузка: %v", err)
		}
		// Все файлы дампа на месте, прежний манифест заменён.
		manifest := filepath.Join(dir, "manifest.json")
		b, err := os.ReadFile(manifest)
		if err != nil {
			t.Fatalf("манифест: %v", err)
		}
		if string(b) == "старый" {
			t.Error("прежний манифест не перезаписан")
		}
		var m fixturesManifest
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("манифест не JSON: %v", err)
		}
		if m.FormatVersion != 1 || m.SmokeDomains != 1 || m.Seed != 242 {
			t.Errorf("манифест: %+v", m)
		}
		if m.Strata["ok"].Taken != 2 || !m.Strata["ok"].Degraded || m.Strata["ok"].Available != 2 {
			t.Errorf("страта ok: %+v (хочу: доступно 2, взято 2, деградация)", m.Strata["ok"])
		}
		for _, s := range []string{"failed", "heavy"} {
			if m.Strata[s].Taken != 0 || !m.Strata[s].Degraded {
				t.Errorf("страта %s: %+v (хочу пусто с деградацией)", s, m.Strata[s])
			}
		}
		// Согласованность файлов со счётчиками манифеста.
		for name, want := range map[string]int{
			"zones.ndjson": 1, "domains.ndjson": 1, "subdomain_results.ndjson": 2, "smoke.ndjson": 1,
		} {
			lines, err := readLines(filepath.Join(dir, name))
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if len(lines) != want || m.Files[name] != want {
				t.Errorf("%s: строк %d (манифест %d), хочу %d", name, len(lines), m.Files[name], want)
			}
		}
		if !strings.Contains(out.String(), "manifest.json: ok") {
			t.Errorf("вывод без строки манифеста:\n%s", out.String())
		}
		if strings.Contains(out.String(), "manifest.json: 0") {
			t.Errorf("вывод с «manifest.json: 0 строк»:\n%s", out.String())
		}
		// smoke-домен — из выборки, с именем label.zone.
		smokeLine, err := os.ReadFile(filepath.Join(dir, "smoke.ndjson"))
		if err != nil || !strings.Contains(string(smokeLine), `"name":"ok.su"`) {
			t.Errorf("smoke.ndjson: %s (%v)", smokeLine, err)
		}
	})

	t.Run("сбой БД до записи — прежний манифест жив", func(t *testing.T) {
		dir := t.TempDir()
		params.OutDir = dir
		if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte("старый-полный"), 0o600); err != nil {
			t.Fatal(err)
		}
		pool := &fixturesPool{failOn: "FROM zones WHERE"} // последний запрос, до записи файлов
		var out bytes.Buffer
		if _, err := fixturesExportRun(context.Background(), pool, params, &out); err == nil {
			t.Fatal("сбой БД не возвращён ошибкой")
		}
		if b, _ := os.ReadFile(filepath.Join(dir, "manifest.json")); string(b) != "старый-полный" {
			t.Errorf("прежний манифест повреждён сбоем до записи: %q", b)
		}
	})

	t.Run("падение в середине записи — манифест удалён", func(t *testing.T) {
		dir := t.TempDir()
		params.OutDir = dir
		if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte("старый-полный"), 0o600); err != nil {
			t.Fatal(err)
		}
		// zones.ndjson — каталог: запись .tmp пройдёт, rename поверх
		// каталога упадёт — сбой ровно между удалением манифеста и его
		// новой записью.
		if err := os.Mkdir(filepath.Join(dir, "zones.ndjson"), 0o700); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if _, err := fixturesExportRun(context.Background(), &fixturesPool{}, params, &out); err == nil {
			t.Fatal("сбой записи не возвращён ошибкой")
		}
		if _, err := os.Stat(filepath.Join(dir, "manifest.json")); !os.IsNotExist(err) {
			t.Errorf("незавершённый дамп с манифестом (контракт «manifest есть ⇒ завершён»): %v", err)
		}
	})
}

// readLines — непустые строки файла (для счётчиков файлов дампа).
func readLines(path string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, l := range strings.Split(string(b), "\n") {
		if l != "" {
			out = append(out, l)
		}
	}
	return out, nil
}

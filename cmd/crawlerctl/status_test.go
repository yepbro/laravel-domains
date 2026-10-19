// Юнит-тесты вывода crawlerctl status (T-079, ТЗ 5.9): формат таблицы и
// JSON (null для «н/д»), роутинг/коды и справка. SQL-контракты, семантика
// агрегатов и расчёты — тесты пакета internal/status (T-126, ТЗД §4.5);
// поведение против живых PostgreSQL/Redis — dev-окружение (make status).
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/status"
)

// --- общий фейк pgx.Rows -------------------------------------------------------

// statusRows — pgx.Rows с заготовленными строками (Scan позиционный,
// типы int16/string/bool/int64/float32/float64/[]byte). Общий фейк
// тестов команд crawlerctl (zones, subdomains, near, recheck, …);
// родился со status (T-079), логика ушла в internal/status (T-126) —
// фейк остаётся здесь, у тестов CLI.
type statusRows struct {
	rows [][]any
	i    int
}

func statusRowsOf(rows ...[]any) *statusRows { return &statusRows{rows: rows} }

func (r *statusRows) Close()                                       {}
func (r *statusRows) Err() error                                   { return nil }
func (r *statusRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *statusRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *statusRows) Values() ([]any, error)                       { return r.rows[r.i-1], nil }
func (r *statusRows) RawValues() [][]byte                          { return nil }
func (r *statusRows) Conn() *pgx.Conn                              { return nil }
func (r *statusRows) TypeMap() *pgtype.Map                         { return nil }

func (r *statusRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *statusRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("statusRows: Scan без Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("statusRows: приёмников %d, колонок %d", len(dest), len(row))
	}
	for i, d := range dest {
		v := row[i]
		switch dst := d.(type) {
		case *int16:
			*dst = v.(int16)
		case *string:
			*dst = v.(string)
		case *bool:
			*dst = v.(bool)
		case *int64:
			*dst = v.(int64)
		case *float32:
			*dst = v.(float32)
		case *float64:
			*dst = v.(float64)
		case *[]byte:
			*dst = v.([]byte)
		default:
			return fmt.Errorf("statusRows: приёмник %T не поддерживается", d)
		}
	}
	return nil
}

// --- вывод ------------------------------------------------------------------------

// TestPercentMark — доля проверенных; пустая зона — «—».
func TestPercentMark(t *testing.T) {
	cases := []struct {
		checked, total int64
		want           string
	}{
		{40, 100, "40%"},
		{1, 3, "33%"}, // доля до целых процентов
		{0, 0, "—"},
		{5, 0, "—"},
	}
	for _, tc := range cases {
		if got := percentMark(tc.checked, tc.total); got != tc.want {
			t.Errorf("percentMark(%d, %d) = %q, want %q", tc.checked, tc.total, got, tc.want)
		}
	}
}

// statusTestSnapshot — согласованный набор данных для тестов вывода:
// две зоны (одна без доменов), замеры и скорости из TestComputeRates.
func statusTestSnapshot() (zones []status.ZoneStats, tot status.Totals, thr status.Throughput, rates status.Rates) {
	zones = []status.ZoneStats{
		{ID: 1, Name: "crawl.test", IsActive: true, DomainsTotal: 100, DomainsChecked: 40, Pending: 60, Queued: 2, Done: 38},
		// Зона без доменов (LEFT JOIN): счётчик monitor остался от прошлого
		// наполнения — в итог он всё равно входит.
		{ID: 3, Name: "empty.test", IsActive: false, DomainsTotal: 10, DomainsChecked: 0},
	}
	tot = status.ComputeTotals(zones)
	thr = status.Throughput{DoneToday: 3600, ElapsedSec: 3600, ResultsHour: 7200, RenderedHour: 3600}
	rates = status.ComputeRates(thr, tot.Pending, tot.Queued) // 1 дом./с → ETA 62с
	return zones, tot, thr, rates
}

// TestWriteStatusTable — формат: заголовок и колонки зон, итого, очереди
// по всем стримам, скорость с окнами, цикл с ETA; выравнивание чисел
// вправо не рвёт колонки итога.
func TestWriteStatusTable(t *testing.T) {
	zones, tot, thr, rates := statusTestSnapshot()
	lens := map[string]int64{"q:dns": 5, "q:http": 0, "q:render": 12, "q:analyze": 3, "q:dead": 1}

	var b bytes.Buffer
	writeStatusTable(&b, zones, tot, thr, rates, lens)
	got := b.String()

	for _, want := range []string{
		"зона", "pending", "queued", "done", // заголовок
		"crawl.test +", "empty.test -",
		"60", "38", "итого", "40%", // агрегаты и итог
		"q:dns 5", "q:render 12", "q:dead 1", // очереди
		"доменов/с          1.0   (3600 проверено с начала суток, прошло 1ч 00м)",
		"HTTP-результатов/с 2.0   (7200 за последний час)",
		"рендеров/с         1.0   (3600 с итоговым DOM за час)",
		"цикл: осталось 62 (pending+queued); ETA ≈ 1м (по 1.0 дом./с)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("таблица не содержит %q:\n%s", want, got)
		}
	}

	// Итог выровнен по правому краю числовых колонок: ширина колонки
	// «всего» — не меньше самого длинного значения (100), итого (110)
	// расширяет её; строка итога заканчивается числами done.
	for _, line := range strings.Split(got, "\n") {
		if strings.HasPrefix(line, "итого") {
			if !strings.HasSuffix(line, "38") {
				t.Errorf("строка итога не кончается done: %q", line)
			}
			if !strings.Contains(line, "110") {
				t.Errorf("строка итога без суммы всего: %q", line)
			}
		}
	}
}

// TestWriteStatusTableDegraded — Redis недоступен: очереди «н/д», сводка
// по БД печатается; пустой цикл помечается завершённым.
func TestWriteStatusTableDegraded(t *testing.T) {
	_, tot, thr, _ := statusTestSnapshot()
	rates := status.ComputeRates(thr, 0, 0) // цикл пуст

	var b bytes.Buffer
	writeStatusTable(&b, nil, tot, thr, rates, nil)
	got := b.String()
	for _, want := range []string{
		"н/д (Redis недоступен)",
		"итого",
		"доменов/с          1.0",
		"цикл: осталось 0 (pending и queued пусты) — проверять нечего",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("деградированный вывод не содержит %q:\n%s", want, got)
		}
	}

	// Короткое окно суток — скорость и ETA честно «н/д».
	thr = status.Throughput{ElapsedSec: 300, ResultsHour: 0}
	rates = status.ComputeRates(thr, 10, 0)
	b.Reset()
	writeStatusTable(&b, nil, status.Totals{}, thr, rates, nil)
	got = b.String()
	for _, want := range []string{
		"н/д   (окно с начала суток короче 10м)",
		"ETA н/д (скорость неизвестна или 0)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("вывод без скорости не содержит %q:\n%s", want, got)
		}
	}
}

// TestWriteStatusJSON — машинный вывод: поля зон/итога/очередей/темпа,
// округление скоростей, null для «н/д» (nil-карта очередей, скорости
// без окна, ETA без скорости).
func TestWriteStatusJSON(t *testing.T) {
	zones, tot, thr, rates := statusTestSnapshot()
	lens := map[string]int64{"q:dns": 5, "q:http": 0, "q:render": 12, "q:analyze": 3, "q:dead": 1}

	var b bytes.Buffer
	if err := writeStatusJSON(&b, zones, tot, thr, rates, lens); err != nil {
		t.Fatalf("writeStatusJSON: %v", err)
	}

	type zoneJSON struct {
		Name           string `json:"name"`
		NameUnicode    string `json:"name_unicode"`
		Active         bool   `json:"active"`
		DomainsTotal   int64  `json:"domains_total"`
		DomainsChecked int64  `json:"domains_checked"`
		Pending        int64  `json:"pending"`
		Queued         int64  `json:"queued"`
		Done           int64  `json:"done"`
	}
	var doc struct {
		Zones      []zoneJSON       `json:"zones"`
		Totals     map[string]int64 `json:"totals"`
		Queues     map[string]int64 `json:"queues"`
		Throughput struct {
			DoneToday   int64   `json:"done_today"`
			ElapsedSec  float64 `json:"elapsed_sec"`
			ResultsHour int64   `json:"results_hour"`
		} `json:"throughput"`
		Rates struct {
			DomainsPerSec *float64 `json:"domains_per_sec"`
			ResultsPerSec float64  `json:"results_per_sec"`
			Remaining     int64    `json:"remaining"`
			ETASec        *int64   `json:"eta_sec"`
		} `json:"rates"`
	}
	if err := json.Unmarshal(b.Bytes(), &doc); err != nil {
		t.Fatalf("вывод не json: %v;\n%s", err, b.String())
	}
	if len(doc.Zones) != 2 || doc.Zones[0].Name != "crawl.test" || !doc.Zones[0].Active {
		t.Errorf("zones = %+v", doc.Zones)
	}
	if doc.Zones[1].NameUnicode != "" {
		t.Errorf("name_unicode пустой зоны = %q, хочу \"\" (omitempty вне теста)", doc.Zones[1].NameUnicode)
	}
	if doc.Totals["pending"] != 60 || doc.Totals["domains_total"] != 110 {
		t.Errorf("totals = %v", doc.Totals)
	}
	if doc.Queues["q:render"] != 12 || len(doc.Queues) != 5 {
		t.Errorf("queues = %v", doc.Queues)
	}
	if doc.Throughput.DoneToday != 3600 || doc.Throughput.ElapsedSec != 3600 {
		t.Errorf("throughput = %+v", doc.Throughput)
	}
	if doc.Rates.DomainsPerSec == nil || *doc.Rates.DomainsPerSec != 1 {
		t.Errorf("domains_per_sec = %v, хочу 1", doc.Rates.DomainsPerSec)
	}
	if doc.Rates.ResultsPerSec != 2 {
		t.Errorf("results_per_sec = %v, хочу 2", doc.Rates.ResultsPerSec)
	}
	if doc.Rates.ETASec == nil || *doc.Rates.ETASec != 62 {
		t.Errorf("eta_sec = %v, хочу 62", doc.Rates.ETASec)
	}

	// Деградация: короткое окно + недоступный Redis → null-поля, остаток
	// без ETA; json при этом валиден.
	b.Reset()
	thr = status.Throughput{ElapsedSec: 300}
	rates = status.ComputeRates(thr, 10, 0)
	if err := writeStatusJSON(&b, nil, status.Totals{}, thr, rates, nil); err != nil {
		t.Fatalf("writeStatusJSON: %v", err)
	}
	var deg struct {
		Zones  []struct{}       `json:"zones"`
		Queues map[string]int64 `json:"queues"`
		Rates  struct {
			DomainsPerSec *float64 `json:"domains_per_sec"`
			Remaining     int64    `json:"remaining"`
			ETASec        *int64   `json:"eta_sec"`
		} `json:"rates"`
	}
	if err := json.Unmarshal(b.Bytes(), &deg); err != nil {
		t.Fatalf("вывод не json: %v;\n%s", err, b.String())
	}
	if len(deg.Zones) != 0 {
		t.Errorf("zones = %d, хочу пустой массив", len(deg.Zones))
	}
	if deg.Queues != nil {
		t.Errorf("queues = %v, хочу null", deg.Queues)
	}
	if deg.Rates.DomainsPerSec != nil || deg.Rates.ETASec != nil {
		t.Errorf("rates = %+v, хочу null/null", deg.Rates)
	}
	if deg.Rates.Remaining != 10 {
		t.Errorf("remaining = %d, хочу 10", deg.Rates.Remaining)
	}
}

// --- роутинг/коды -------------------------------------------------------------------

// TestRunStatus — путь выполнения без инфраструктуры: команды нужен
// конфиг (DSN PostgreSQL, Redis) — при его отсутствии ошибка выполнения
// (код 1) до соединений (стиль TestRunReanalyze); --json не ломает путь.
func TestRunStatus(t *testing.T) {
	// Явно отсутствующий конфиг-файл: детерминированный отказ до любых
	// соединений (в контейнере tools переменные CRAWLER_* могли бы собрать
	// конфигурацию из окружения — тест не должен ходить в живой стек).
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	var stdout, stderr bytes.Buffer
	code := run([]string{"status"}, &stdout, &stderr)
	if code != exitError {
		t.Fatalf("код = %d, хочу %d; stderr:\n%s", code, exitError, stderr.String())
	}
	if !strings.Contains(stderr.String(), "status: config:") {
		t.Errorf("stderr не содержит контекст конфигурации:\n%s", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout до соединения не пуст:\n%s", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = run([]string{"status", "--json"}, &stdout, &stderr)
	if code != exitError || !strings.Contains(stderr.String(), "status: config:") {
		t.Errorf("--json: код = %d, stderr:\n%s; хочу ошибку конфигурации (код 1)", code, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Errorf("--json: stdout до соединения не пуст:\n%s", stdout.String())
	}
}

// TestStatusHelpMentionsStreams — справка перечисляет все стримы и ETA
// (содержание целиком проверяет TestRunHelpPerCommand в main_test.go).
func TestStatusHelpMentionsStreams(t *testing.T) {
	for _, s := range status.Streams {
		if !strings.Contains(statusHelp, s) {
			t.Errorf("statusHelp не упоминает стрим %q", s)
		}
	}
	if !strings.Contains(statusHelp, "ETA") {
		t.Error("statusHelp не описывает ETA цикла")
	}
}

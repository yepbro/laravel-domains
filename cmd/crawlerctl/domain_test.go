// Юнит-тесты crawlerctl domain (T-081, ТЗ 5.11) на фейковых пуле (db.DB)
// и хранилище контента — без БД и пак-файлов. Проверяются точный текст
// SQL-запросов (контракт с функцией domain_info миграции 000006 — единая
// точка правды, стиль status_test.go), разбор jsonb-ответа, нормализация
// форм имени (регистр/точка/unicode/punycode — до обращения к БД),
// выбор записи для --content (приоритет DOM-рендера), формат сводки
// (разделы, расшифровка dns_mask, таблица результатов, признаки Laravel
// с фрагментами и слабыми срабатываниями), машинный вывод (--json —
// сырой jsonb; found:false для неизвестного) и понятные сообщения
// «что именно не найдено». Поведение против живого стека — dev-окружение
// (make domain NAME=…).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/idn"
)

// --- SQL-контракты ----------------------------------------------------------

// wantDomainInfoSQL / wantZoneLongestSQL — ожидаемый текст запросов;
// фейк сопоставляет SQL точно, неожидаемый запрос валит тест громко.
const wantDomainInfoSQL = "SELECT domain_info($1) AS info"

const wantZoneLongestSQL = "SELECT z.name FROM zones AS z " +
	"WHERE z.name = ANY($1::text[]) " +
	"ORDER BY length(z.name) DESC, z.name LIMIT 1"

// domainFixtureJSON — типовой ответ domain_info: живой домен с двумя
// DNS-живыми субдоменами, тремя результатами (апекс DOM, алиас www,
// ошибка api), агрегатом Laravel и парой срабатываний — уверенное
// и слабое. Формат — контракт миграции 000006 (T-082).
const domainFixtureJSON = `{
  "zone": {"id": 1, "name": "crawl.test", "name_unicode": null},
  "domain": {
    "id": 42, "label": "laravel-11", "name": "laravel-11.crawl.test",
    "status": 4, "status_name": "alive",
    "check_state": 2, "check_state_name": "done",
    "is_idn": false, "render_pending": false,
    "dns_mask": 3, "temp_attempts": 0,
    "queued_at": "2026-10-16T09:00:00.5+00:00",
    "last_checked_at": "2026-10-16"
  },
  "dns_alive": [{"id": 0, "label": ""}, {"id": 1, "label": "www"}],
  "results": [
    {"subdomain_id": 0, "subdomain": "", "variant": 0, "scheme": 2, "scheme_name": "https",
     "http_status": 200, "final_url": "https://laravel-11.crawl.test/",
     "alias_kind": 0, "alias_kind_name": "none", "alias": null,
     "rendered": true, "error": null, "content_hash": "00112233445566778899aabbccddeeff",
     "checked_at": "2026-10-16T09:00:05.123456+00:00"},
    {"subdomain_id": 1, "subdomain": "www", "variant": 0, "scheme": 2, "scheme_name": "https",
     "http_status": 200, "final_url": "https://laravel-11.crawl.test/",
     "alias_kind": 1, "alias_kind_name": "identical", "alias": "",
     "rendered": false, "error": null, "content_hash": "00112233445566778899aabbccddeeff",
     "checked_at": "2026-10-16T09:00:07+00:00"},
    {"subdomain_id": 2, "subdomain": "api", "variant": 1, "scheme": 1, "scheme_name": "http",
     "http_status": null, "final_url": null,
     "alias_kind": 0, "alias_kind_name": "none", "alias": null,
     "rendered": false, "error": 1, "content_hash": "",
     "checked_at": "2026-10-16T09:00:08+00:00"}
  ],
  "laravel": {
    "score": 1.25, "ver_min": "11.0", "ver_max": "11.4",
    "found_on_mask": 1, "found_on": [{"id": 0, "label": ""}],
    "rules_version": 42, "updated_at": "2026-10-16T09:01:00+00:00",
    "findings": [
      {"subdomain_id": 0, "subdomain": "", "variant": 0, "score": 1.25,
       "ver_min": "11.0", "ver_max": "11.4", "is_laravel": true,
       "signals": [
         {"rule": "cookie_xsrf", "weight": 0.5, "evidence": "Set-Cookie: XSRF-TOKEN=eyJ…"},
         {"rule": "header_laravel", "weight": 0.75, "evidence": "X-Laravel-Version: 11"}]},
      {"subdomain_id": 1, "subdomain": "www", "variant": 0, "score": 0.25,
       "ver_min": null, "ver_max": null, "is_laravel": false,
       "signals": [{"rule": "meta_generator", "weight": 0.25, "evidence": "Laravel"}]}
    ]
  }
}`

// domainFixtureInfo — разобранный доменныйFixtureJSON.
func domainFixtureInfo(t *testing.T) domainInfo {
	t.Helper()
	var info domainInfo
	if err := json.Unmarshal([]byte(domainFixtureJSON), &info); err != nil {
		t.Fatalf("разбор фикстуры: %v", err)
	}
	return info
}

// --- фейковый пул (db.DB) ----------------------------------------------------

// domainPool — db.DB без БД: infos (punycode имя → json domain_info,
// пустое значение или отсутствие ключа — NULL «не найдено»), zones —
// справочник для zoneLongestSQL.
type domainPool struct {
	infos map[string]string
	zones []string

	mu        sync.Mutex
	queries   []string
	queryArgs [][]any
	queryErr  error
}

var _ db.DB = (*domainPool)(nil)

func newDomainPool() *domainPool {
	return &domainPool{infos: map[string]string{}}
}

func (p *domainPool) seedInfo(name, json string) { p.infos[name] = json }

func (p *domainPool) setQueryErr(err error) {
	p.mu.Lock()
	p.queryErr = err
	p.mu.Unlock()
}

func (p *domainPool) calls() ([]string, [][]any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.queries...), append([][]any(nil), p.queryArgs...)
}

func (p *domainPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("domainPool: CopyFrom не должен вызываться")
}

func (p *domainPool) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, fmt.Errorf("domainPool: неожидаемый Exec: %s", sql)
}

func (p *domainPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, sql)
	p.queryArgs = append(p.queryArgs, append([]any(nil), args...))
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	switch sql {
	case domainInfoSQL:
		name, _ := args[0].(string)
		if js, ok := p.infos[name]; ok && js != "" {
			return domainRowsOf([]any{[]byte(js)}), nil
		}
		return domainRowsOf([]any{nil}), nil // NULL: не найдено
	case zoneLongestSQL:
		cands, _ := args[0].([]string)
		best := ""
		for _, z := range p.zones { // ORDER BY length DESC, name
			for _, c := range cands {
				if z != c {
					continue
				}
				if len(z) > len(best) || (len(z) == len(best) && z < best) {
					best = z
				}
			}
		}
		if best == "" {
			return domainRowsOf([]any{nil}), nil
		}
		return domainRowsOf([]any{[]byte(best)}), nil
	default:
		return nil, fmt.Errorf("domainPool: неожидаемый SQL Query: %s", sql)
	}
}

// domainRows — pgx.Rows с заготовленными строками (Scan позиционный;
// nil-значение — SQL NULL). Значения колонок: []byte, string.
type domainRows struct {
	rows [][]any
	i    int
}

func domainRowsOf(rows ...[]any) *domainRows { return &domainRows{rows: rows} }

func (r *domainRows) Close()                                       {}
func (r *domainRows) Err() error                                   { return nil }
func (r *domainRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *domainRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *domainRows) Values() ([]any, error)                       { return r.rows[r.i-1], nil }
func (r *domainRows) RawValues() [][]byte                          { return nil }
func (r *domainRows) Conn() *pgx.Conn                              { return nil }
func (r *domainRows) TypeMap() *pgtype.Map                         { return nil }

func (r *domainRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *domainRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("domainRows: Scan без Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("domainRows: приёмников %d, колонок %d", len(dest), len(row))
	}
	for i, d := range dest {
		v := row[i]
		switch dst := d.(type) {
		case *[]byte:
			if v == nil {
				*dst = nil
				continue
			}
			*dst = v.([]byte)
		case *string:
			if v == nil {
				*dst = ""
				continue
			}
			*dst = v.(string)
		default:
			return fmt.Errorf("domainRows: приёмник %T не поддерживается", d)
		}
	}
	return nil
}

// --- фейковое хранилище контента ---------------------------------------------

// fakeContentStore — domainContentStore: байты по hex-хэшу или ошибка.
type fakeContentStore struct {
	data map[string][]byte
	err  error
}

func (s *fakeContentStore) Get(_ context.Context, h content.Hash) ([]byte, error) {
	if s.err != nil {
		return nil, s.err
	}
	d, ok := s.data[h.String()]
	if !ok {
		return nil, content.ErrNotFound
	}
	return d, nil
}

// --- SQL и семантика выборок ---------------------------------------------------

func TestDomainSQLContract(t *testing.T) {
	if domainInfoSQL != wantDomainInfoSQL {
		t.Errorf("domainInfoSQL:\n%s\nwant:\n%s", domainInfoSQL, wantDomainInfoSQL)
	}
	if zoneLongestSQL != wantZoneLongestSQL {
		t.Errorf("zoneLongestSQL:\n%s\nwant:\n%s", zoneLongestSQL, wantZoneLongestSQL)
	}
}

// TestFetchDomainInfo — вызов функции с punycode-именем, разбор jsonb
// (зона/статусы/dns_alive/результаты/laravel с findings), NULL → found
// false без ошибки; ошибки пула — с контекстом команды.
func TestFetchDomainInfo(t *testing.T) {
	pool := newDomainPool()
	pool.seedInfo("laravel-11.crawl.test", domainFixtureJSON)

	info, raw, found, err := fetchDomainInfo(context.Background(), pool, "laravel-11.crawl.test")
	if err != nil {
		t.Fatalf("fetchDomainInfo: %v", err)
	}
	if !found {
		t.Fatal("found = false, хочу true")
	}
	if string(raw) != domainFixtureJSON {
		t.Errorf("raw не совпадает с ответом функции:\n%s", raw)
	}
	queries, args := pool.calls()
	if len(queries) != 1 || queries[0] != wantDomainInfoSQL {
		t.Fatalf("запросы = %q, хочу один domainInfoSQL", queries)
	}
	if len(args) != 1 || args[0][0] != "laravel-11.crawl.test" {
		t.Errorf("аргументы = %v, хочу имя punycode первым аргументом", args)
	}

	if info.Zone.ID != 1 || info.Zone.Name != "crawl.test" || info.Zone.NameUnicode != "" {
		t.Errorf("zone = %+v", info.Zone)
	}
	d := info.Domain
	if d.ID != 42 || d.Label != "laravel-11" || d.Name != "laravel-11.crawl.test" ||
		d.StatusName != "alive" || d.CheckStateName != "done" ||
		d.DNSMask != 3 || d.TempAttempts != 0 || d.QueuedAt == "" || d.LastCheckedAt != "2026-10-16" {
		t.Errorf("domain = %+v", d)
	}
	if len(info.DNSAlive) != 2 || info.DNSAlive[0].Label != "" || info.DNSAlive[1].Label != "www" {
		t.Errorf("dns_alive = %+v", info.DNSAlive)
	}
	if len(info.Results) != 3 {
		t.Fatalf("results = %d, хочу 3", len(info.Results))
	}
	first := info.Results[0]
	if first.Subdomain != "" || first.SchemeName != "https" || first.HTTPStatus != 200 ||
		!first.Rendered || first.ContentHash != "00112233445566778899aabbccddeeff" {
		t.Errorf("results[0] = %+v", first)
	}
	if info.Laravel == nil {
		t.Fatal("laravel = nil, хочу агрегат")
	}
	if info.Laravel.Score != 1.25 || info.Laravel.VerMin != "11.0" || info.Laravel.VerMax != "11.4" ||
		info.Laravel.RulesVersion != 42 {
		t.Errorf("laravel = %+v", info.Laravel)
	}
	if len(info.Laravel.Findings) != 2 || !info.Laravel.Findings[0].IsLaravel || info.Laravel.Findings[1].IsLaravel {
		t.Errorf("findings = %+v, хочу уверенное + слабое", info.Laravel.Findings)
	}
	if sigs := info.Laravel.Findings[0].Signals; len(sigs) != 2 || sigs[0].Rule != "cookie_xsrf" || sigs[1].Evidence != "X-Laravel-Version: 11" {
		t.Errorf("signals = %+v", sigs)
	}

	// NULL «не найдено» — не ошибка.
	pool = newDomainPool()
	_, raw, found, err = fetchDomainInfo(context.Background(), pool, "no-such.crawl.test")
	if err != nil || found || raw != nil {
		t.Errorf("NULL: raw=%v found=%v err=%v, хочу nil/false/nil", raw, found, err)
	}

	// Ошибки пула — с контекстом; nil-пул — ошибка, не паника.
	boom := errors.New("boom")
	pool = newDomainPool()
	pool.setQueryErr(boom)
	if _, _, _, err := fetchDomainInfo(context.Background(), pool, "x.crawl.test"); err == nil || !strings.Contains(err.Error(), "domain: domain_info:") {
		t.Errorf("err = %v, хочу контекст «domain: domain_info:»", err)
	}
	if _, _, _, err := fetchDomainInfo(context.Background(), nil, "x.crawl.test"); err == nil {
		t.Error("fetchDomainInfo(nil) = nil, хочу ошибку")
	}

	// Битый json — ошибка разбора, а не молчание.
	pool = newDomainPool()
	pool.seedInfo("bad.crawl.test", "{не json")
	if _, _, _, err := fetchDomainInfo(context.Background(), pool, "bad.crawl.test"); err == nil || !strings.Contains(err.Error(), "разбор json") {
		t.Errorf("битый json: err = %v, хочу ошибку разбора", err)
	}
}

// TestDomainNormalizeForms — нормализация входа оператора (ТЗ 5.2):
// регистр, точка на конце, пробелы, unicode и punycode дают одно
// punycode-имя — оно и уходит в domain_info (аргумент запроса).
func TestDomainNormalizeForms(t *testing.T) {
	forms := map[string]string{
		"laravel-11.crawl.test":   "laravel-11.crawl.test",
		"Laravel-11.CRAWL.Test.":  "laravel-11.crawl.test",
		" laravel-11.crawl.test ": "laravel-11.crawl.test",
		"пример.crawl.test":       "xn--e1afmkfd.crawl.test",
		"xn--e1afmkfd.crawl.test": "xn--e1afmkfd.crawl.test",
		"ПРИМЕР.crawl.TEST.":      "xn--e1afmkfd.crawl.test",
	}
	pool := newDomainPool()
	for in, want := range forms {
		res, err := idn.Normalize(in)
		if err != nil {
			t.Errorf("Normalize(%q): %v", in, err)
			continue
		}
		if res.ASCII != want {
			t.Errorf("Normalize(%q).ASCII = %q, хочу %q", in, res.ASCII, want)
		}
		pool.seedInfo(want, domainFixtureJSON)
		if _, _, found, err := fetchDomainInfo(context.Background(), pool, res.ASCII); err != nil || !found {
			t.Errorf("fetchDomainInfo(%q → %q): found=%v err=%v", in, res.ASCII, found, err)
		}
	}
}

// TestZoneSuffixCandidates — кандидаты в зону: все суффиксы кроме
// полного имени, длиннейший первым (как режет domain_info).
func TestZoneSuffixCandidates(t *testing.T) {
	cases := []struct {
		name string
		want []string
	}{
		{"example.crawl.test", []string{"crawl.test", "test"}},
		{"a.b.crawl.test", []string{"b.crawl.test", "crawl.test", "test"}},
		{"x.test", []string{"test"}},
		{"пример.crawl.test", []string{"crawl.test", "test"}},
	}
	for _, tc := range cases {
		if got := zoneSuffixCandidates(tc.name); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("zoneSuffixCandidates(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestFetchCatalogZone — длиннейший суффикс в zones (co.uk предпочтается
// uk); не найдено — best = длиннейший кандидат, found=false.
func TestFetchCatalogZone(t *testing.T) {
	pool := newDomainPool()
	pool.zones = []string{"uk", "co.uk", "test"}

	zone, found, err := fetchCatalogZone(context.Background(), pool, "shop.example.co.uk")
	if err != nil || !found || zone != "co.uk" {
		t.Errorf("co.uk: zone=%q found=%v err=%v, хочу co.uk/true", zone, found, err)
	}
	_, args := pool.calls()
	if len(args) != 1 || !reflect.DeepEqual(args[0][0], []string{"example.co.uk", "co.uk", "uk"}) {
		t.Errorf("аргументы = %v, хочу суффиксы-кандидаты", args)
	}

	zone, found, err = fetchCatalogZone(context.Background(), pool, "free.test")
	if err != nil || !found || zone != "test" {
		t.Errorf("test: zone=%q found=%v err=%v, хочу test/true", zone, found, err)
	}

	zone, found, err = fetchCatalogZone(context.Background(), pool, "x.unknown.tld")
	if err != nil || found || zone != "unknown.tld" {
		t.Errorf("нет зоны: zone=%q found=%v err=%v, хочу unknown.tld/false", zone, found, err)
	}

	// Ошибки и nil-пул.
	pool.setQueryErr(errors.New("boom"))
	if _, _, err := fetchCatalogZone(context.Background(), pool, "x.test"); err == nil || !strings.Contains(err.Error(), "domain: зоны:") {
		t.Errorf("err = %v, хочу контекст «domain: зоны:»", err)
	}
	if _, _, err := fetchCatalogZone(context.Background(), nil, "x.test"); err == nil {
		t.Error("fetchCatalogZone(nil) = nil, хочу ошибку")
	}
}

// --- выбор контента ------------------------------------------------------------

// TestSelectContentResult — приоритет отрендеренной записи (DOM —
// итоговый результат конвейера), иначе первая запись с контентом
// в порядке субдомен×вариант; без контента — false.
func TestSelectContentResult(t *testing.T) {
	hash := "00112233445566778899aabbccddeeff"
	cases := []struct {
		name    string
		results []domainResult
		wantIdx int
		wantOK  bool
	}{
		{
			name: "рендер приоритетнее первой",
			results: []domainResult{
				{Subdomain: "", ContentHash: hash, Rendered: false},
				{Subdomain: "www", ContentHash: hash, Rendered: true},
			},
			wantIdx: 1, wantOK: true,
		},
		{
			name: "без рендера — первая с контентом",
			results: []domainResult{
				{Subdomain: "", ContentHash: ""},
				{Subdomain: "www", ContentHash: hash},
				{Subdomain: "api", ContentHash: hash},
			},
			wantIdx: 1, wantOK: true,
		},
		{
			name: "апекс-DOM раньше www-DOM (порядок функции)",
			results: []domainResult{
				{Subdomain: "", ContentHash: hash, Rendered: true},
				{Subdomain: "www", ContentHash: hash, Rendered: true},
			},
			wantIdx: 0, wantOK: true,
		},
		{
			name: "контента нет вообще",
			results: []domainResult{
				{Subdomain: "", ContentHash: ""},
				{Subdomain: "api", ContentHash: "", Error: 1},
			},
			wantIdx: 0, wantOK: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := selectContentResult(tc.results)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, хочу %v", ok, tc.wantOK)
			}
			if ok && got != tc.results[tc.wantIdx] {
				t.Errorf("выбрана запись %+v, хочу %+v", got, tc.results[tc.wantIdx])
			}
		})
	}
	if _, ok := selectContentResult(nil); ok {
		t.Error("selectContentResult(nil) = true, хочу false")
	}
}

// --- форматирование ---------------------------------------------------------------

func TestSubLabel(t *testing.T) {
	if got := subLabel(""); got != "(апекс)" {
		t.Errorf("subLabel(\"\") = %q, хочу (апекс)", got)
	}
	if got := subLabel("www"); got != "www" {
		t.Errorf("subLabel(www) = %q", got)
	}
}

func TestVerRange(t *testing.T) {
	cases := []struct {
		min, max, want string
	}{
		{"11.0", "11.4", "11.0 – 11.4"},
		{"11.0", "11.0", "11.0"},
		{"11.0", "", "≥ 11.0"},
		{"", "11.4", "≤ 11.4"},
		{"", "", "—"},
	}
	for _, tc := range cases {
		if got := verRange(tc.min, tc.max); got != tc.want {
			t.Errorf("verRange(%q, %q) = %q, хочу %q", tc.min, tc.max, got, tc.want)
		}
	}
}

func TestHTTPErrorName(t *testing.T) {
	cases := []struct {
		code int16
		want string
	}{
		{0, "none"}, {1, "timeout"}, {2, "redirects"},
		{3, "tls"}, {4, "body"}, {5, "network"}, {9, "код 9"},
	}
	for _, tc := range cases {
		if got := httpErrorName(tc.code); got != tc.want {
			t.Errorf("httpErrorName(%d) = %q, хочу %q", tc.code, got, tc.want)
		}
	}
}

func TestFormatDomainTS(t *testing.T) {
	// timestamptz (ISO 8601 из jsonb) → дата-время; date и мусор — как есть.
	cases := []struct {
		in, want string
	}{
		{"2026-10-16T09:00:00.5+00:00", "2026-10-16 09:00:00"},
		{"2026-10-16T09:00:08+00:00", "2026-10-16 09:00:08"},
		{"2026-10-16", "2026-10-16"},
		{"не время", "не время"},
	}
	for _, tc := range cases {
		if got := formatDomainTS(tc.in); got != tc.want {
			t.Errorf("formatDomainTS(%q) = %q, хочу %q", tc.in, got, tc.want)
		}
	}
	if got := formatDomainTSShort("2026-10-16T09:00:08+00:00"); got != "2026-10-16 09:00" {
		t.Errorf("formatDomainTSShort = %q", got)
	}
}

// TestResultCells — ячейки таблицы результатов: статус/http 0 и пустой
// URL без ошибки — «—», контент DOM/HTML/—, алиасы, ошибка с классом.
func TestResultCells(t *testing.T) {
	cases := []struct {
		name string
		r    domainResult
		want string
	}{
		{"http 0 — прочерк", domainResult{}, "—"},
		{"статус", domainResult{HTTPStatus: 200}, "200"},
	}
	for _, tc := range cases {
		if got := httpStatusDesc(tc.r); got != tc.want {
			t.Errorf("%s: httpStatusDesc = %q, хочу %q", tc.name, got, tc.want)
		}
	}

	content := []struct {
		name string
		r    domainResult
		want string
	}{
		{"без контента", domainResult{}, "—"},
		{"сырой HTML", domainResult{ContentHash: "ab"}, "HTML"},
		{"DOM рендера", domainResult{ContentHash: "ab", Rendered: true}, "DOM"},
	}
	for _, tc := range content {
		if got := contentKind(tc.r); got != tc.want {
			t.Errorf("%s: contentKind = %q, хочу %q", tc.name, got, tc.want)
		}
	}

	aliases := []struct {
		name string
		r    domainResult
		want string
	}{
		{"нет связи", domainResult{AliasKind: 0}, "—"},
		{"идентичен апексу", domainResult{AliasKind: 1, Alias: ""}, "≡ (апекс)"},
		{"редирект на www", domainResult{AliasKind: 2, Alias: "www"}, "→ www"},
	}
	for _, tc := range aliases {
		if got := aliasDesc(tc.r); got != tc.want {
			t.Errorf("%s: aliasDesc = %q, хочу %q", tc.name, got, tc.want)
		}
	}

	finals := []struct {
		name string
		r    domainResult
		want string
	}{
		{"только URL", domainResult{FinalURL: "https://x.test/"}, "https://x.test/"},
		{"только ошибка", domainResult{Error: 1}, "ошибка 1 (timeout)"},
		{"URL и ошибка", domainResult{FinalURL: "http://x.test/", Error: 3}, "http://x.test/ — ошибка 3 (tls)"},
		{"пусто", domainResult{}, "—"},
	}
	for _, tc := range finals {
		if got := resultFinalDesc(tc.r); got != tc.want {
			t.Errorf("%s: resultFinalDesc = %q, хочу %q", tc.name, got, tc.want)
		}
	}
}

// --- человекочитаемый вывод ---------------------------------------------------------

// TestWriteDomainText — разделы сводки по фикстуре: шапка с зоной
// и статусами, расшифровка dns_mask, таблица результатов (схема/статус/
// контент/алиас/URL/ошибка), признаки Laravel с версиями, правилами
// и фрагментами, слабые срабатывания отдельной секцией.
func TestWriteDomainText(t *testing.T) {
	info := domainFixtureInfo(t)

	var b strings.Builder
	writeDomainText(&b, info, 0.6)
	got := b.String()

	for _, want := range []string{
		"домен:        laravel-11.crawl.test\n",
		"зона:         crawl.test (id 1)\n",
		"статус:       alive\n",
		"проверка:     done\n",
		"проверен:     2026-10-16\n",
		"в очереди с:  2026-10-16 09:00:00\n",
		"DNS-живые субдомены (dns_mask = 3): (апекс), www\n",
		"результаты по субдоменам и вариантам:\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("сводка не содержит %q:\n%s", want, got)
		}
	}

	// Строки таблицы: паддинг не проверяем — только ячейки в строке.
	var apexLine, wwwLine, apiLine string
	for _, line := range strings.Split(got, "\n") {
		switch {
		case strings.HasPrefix(line, "(апекс)"):
			apexLine = line
		case strings.HasPrefix(line, "www"):
			wwwLine = line
		case strings.HasPrefix(line, "api"):
			apiLine = line
		}
	}
	for _, cell := range []string{"main", "https", "200", "DOM", "—", "2026-10-16 09:00", "https://laravel-11.crawl.test/"} {
		if !strings.Contains(apexLine, cell) {
			t.Errorf("строка апекса без %q: %q", cell, apexLine)
		}
	}
	for _, cell := range []string{"www", "HTML", "≡ (апекс)"} {
		if !strings.Contains(wwwLine, cell) {
			t.Errorf("строка www без %q: %q", cell, wwwLine)
		}
	}
	for _, cell := range []string{"api", "http", "—", "ошибка 1 (timeout)"} {
		if !strings.Contains(apiLine, cell) {
			t.Errorf("строка api без %q: %q", cell, apiLine)
		}
	}

	for _, want := range []string{
		"Laravel:\n",
		"  score:       1.250 (порог 0.600)\n",
		"  версии:      11.0 – 11.4\n",
		"  субдомены:   (апекс)\n",
		"  правила:     версия 42, обновлено 2026-10-16 09:01:00\n",
		"  срабатывания (is_laravel = true):\n",
		"    (апекс) main  score 1.250, версии 11.0 – 11.4:\n",
		"      cookie_xsrf (+0.500): «Set-Cookie: XSRF-TOKEN=eyJ…»\n",
		"      header_laravel (+0.750): «X-Laravel-Version: 11»\n",
		"  слабые срабатывания (is_laravel = false, score < 0.600):\n",
		"    www main  score 0.250:\n",
		"      meta_generator (+0.250): «Laravel»\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("раздел Laravel без %q:\n%s", want, got)
		}
	}
}

// TestWriteDomainTextVariants — крайние случаи сводки: IDN-домен
// (unicode-строка и флаги), пустые результаты, отсутствие признаков
// Laravel, temp-попытки.
func TestWriteDomainTextVariants(t *testing.T) {
	var info domainInfo
	info.Zone.Name = "xn--p1ai"
	info.Zone.NameUnicode = "рф"
	info.Zone.ID = 7
	info.Domain.Name = "xn--e1afmkfd.xn--p1ai"
	info.Domain.Label = "xn--e1afmkfd"
	info.Domain.StatusName = "temp_error"
	info.Domain.CheckStateName = "queued"
	info.Domain.TempAttempts = 2
	info.Domain.IsIDN = true
	info.Domain.RenderPending = true

	var b strings.Builder
	writeDomainText(&b, info, 0.6)
	got := b.String()
	for _, want := range []string{
		"домен:        xn--e1afmkfd.xn--p1ai\n",
		"unicode:      пример.рф\n",
		"зона:         xn--p1ai (unicode: рф, id 7)\n",
		"статус:       temp_error\n",
		"проверка:     queued\n",
		"проверен:     —\n",
		"флаги:        is_idn, render_pending\n",
		"temp-попытки: 2\n",
		"DNS-живые субдомены (dns_mask = 0): нет\n",
		"  — нет (домен не проверялся или DNS не делегирован)\n",
		"  — признаков не найдено (нет записи в laravel_domains;\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("сводка (вариант) не содержит %q:\n%s", want, got)
		}
	}
}

// --- машинный вывод -------------------------------------------------------------

// TestWriteDomainJSON — сырой jsonb pretty-print: содержимое не меняется,
// отступ 2 пробела, завершающий перевод строки.
func TestWriteDomainJSON(t *testing.T) {
	var b strings.Builder
	if err := writeDomainJSON(&b, []byte(domainFixtureJSON)); err != nil {
		t.Fatalf("writeDomainJSON: %v", err)
	}
	got := b.String()
	if !strings.HasSuffix(got, "\n") {
		t.Error("json не кончается переводом строки")
	}
	if !strings.Contains(got, "\n  \"zone\"") {
		t.Errorf("json без отступа 2 пробела:\n%s", got)
	}
	var have, want map[string]any
	if err := json.Unmarshal([]byte(got), &have); err != nil {
		t.Fatalf("вывод не json: %v;\n%s", err, got)
	}
	if err := json.Unmarshal([]byte(domainFixtureJSON), &want); err != nil {
		t.Fatalf("фикстура не json: %v", err)
	}
	if !reflect.DeepEqual(have, want) {
		t.Errorf("pretty-print изменил данные:\n%s", got)
	}
}

// TestWriteDomainNotFoundJSON — found:false с уточнением по зоне.
func TestWriteDomainNotFoundJSON(t *testing.T) {
	var b bytes.Buffer
	if err := writeDomainNotFoundJSON(&b, "example.crawl.test", "crawl.test", false); err != nil {
		t.Fatalf("writeDomainNotFoundJSON: %v", err)
	}
	var doc struct {
		Found     bool   `json:"found"`
		Name      string `json:"name"`
		Zone      string `json:"zone"`
		ZoneFound bool   `json:"zone_found"`
	}
	if err := json.Unmarshal(b.Bytes(), &doc); err != nil {
		t.Fatalf("вывод не json: %v;\n%s", err, b.String())
	}
	if doc.Found || doc.Name != "example.crawl.test" || doc.Zone != "crawl.test" || doc.ZoneFound {
		t.Errorf("doc = %+v, хочу found=false, зона crawl.test без записи", doc)
	}
}

// --- не найдено -------------------------------------------------------------------

// TestDomainNotFound — что именно не найдено (ТЗ 5.11): зона есть —
// «нет домена с меткой», зоны нет — «зона отсутствует в справочнике»;
// --json печатает found:false и возвращает ошибку (код 1 через run).
func TestDomainNotFound(t *testing.T) {
	pool := newDomainPool()
	pool.zones = []string{"crawl.test"}

	err := domainNotFound(context.Background(), pool, "example.crawl.test", false, &strings.Builder{})
	if err == nil || !strings.Contains(err.Error(), "в зоне crawl.test нет домена с меткой example") {
		t.Errorf("err = %v, хочу «в зоне crawl.test нет домена с меткой example»", err)
	}

	var out bytes.Buffer
	err = domainNotFound(context.Background(), pool, "example.crawl.test", true, &out)
	if err == nil || !strings.Contains(out.String(), `"found":false`) || !strings.Contains(out.String(), `"zone_found":true`) {
		t.Errorf("--json: err=%v out=%s; хочу found:false + zone_found:true + ошибка", err, out.String())
	}

	pool = newDomainPool() // зон нет вовсе
	err = domainNotFound(context.Background(), pool, "example.unknown.tld", false, &strings.Builder{})
	if err == nil || !strings.Contains(err.Error(), "зона unknown.tld отсутствует в справочнике zones") {
		t.Errorf("err = %v, хочу «зона unknown.tld отсутствует в справочнике zones»", err)
	}

	// Ошибка уточнения — ошибка команды, а не «не найдено».
	pool = newDomainPool()
	pool.setQueryErr(errors.New("boom"))
	if err := domainNotFound(context.Background(), pool, "x.crawl.test", false, &strings.Builder{}); err == nil || !strings.Contains(err.Error(), "domain: зоны:") {
		t.Errorf("err = %v, хочу ошибку уточнения зоны", err)
	}
}

// --- контент ----------------------------------------------------------------------

// TestWriteDomainContent — stdout получает ровно байты выбранной записи,
// stderr — сводку и пометку (запись, вид, размер, хэш); отсутствие
// контента и ошибки хранилища — понятные ошибки.
func TestWriteDomainContent(t *testing.T) {
	info := domainFixtureInfo(t)
	page := []byte("<!DOCTYPE html><html><body>Laravel 11</body></html>\n")
	store := &fakeContentStore{data: map[string][]byte{
		"00112233445566778899aabbccddeeff": page,
	}}

	var stdout, stderr bytes.Buffer
	if err := writeDomainContent(context.Background(), store, info, 0.6, &stdout, &stderr); err != nil {
		t.Fatalf("writeDomainContent: %v", err)
	}
	if stdout.String() != string(page) {
		t.Errorf("stdout = %q, хочу только байты контента", stdout.String())
	}
	for _, want := range []string{
		"домен:        laravel-11.crawl.test", // сводка — в stderr
		"контент (--content): субдомен (апекс), вариант main, DOM (рендер),",
		fmt.Sprintf(" %d байт, хэш 00112233445566778899aabbccddeeff", len(page)),
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("stderr не содержит %q:\n%s", want, stderr.String())
		}
	}

	// Записи с контентом нет — понятная ошибка.
	noContent := info
	noContent.Results = []domainResult{{Subdomain: "api", Error: 1}}
	if err := writeDomainContent(context.Background(), store, noContent, 0.6, &stdout, &stderr); err == nil ||
		!strings.Contains(err.Error(), "нет сохранённого контента") {
		t.Errorf("err = %v, хочу «нет сохранённого контента»", err)
	}

	// Хэш из функции битый (не 16 байт hex) — ошибка данных, не паника.
	badHash := info
	badHash.Results = []domainResult{{Subdomain: "", ContentHash: "abc", Rendered: true}}
	if err := writeDomainContent(context.Background(), store, badHash, 0.6, &stdout, &stderr); err == nil ||
		!strings.Contains(err.Error(), "некорректный content_hash") {
		t.Errorf("err = %v, хочу «некорректный content_hash»", err)
	}

	// Хранилище не нашло байты (ссылка есть, блока нет) — ошибка с хэшем.
	store = &fakeContentStore{err: content.ErrNotFound}
	if err := writeDomainContent(context.Background(), store, info, 0.6, &stdout, &stderr); err == nil ||
		!strings.Contains(err.Error(), "00112233445566778899aabbccddeeff") {
		t.Errorf("err = %v, хочу ошибку с хэшем контента", err)
	}
}

// --- роутинг/коды -------------------------------------------------------------------

// TestRunDomain — путь выполнения без инфраструктуры: команде нужен
// конфиг (DSN PostgreSQL) — при его отсутствии ошибка выполнения (код 1)
// до соединений (стиль TestRunStatus). Валидация входа идёт раньше:
// неверное имя и сочетание флагов — код 2 даже без конфигурации;
// нормализованные формы имени проходят валидацию (доходят до конфига).
func TestRunDomain(t *testing.T) {
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	runCase := func(args ...string) (int, string, string) {
		var stdout, stderr bytes.Buffer
		code := run(args, &stdout, &stderr)
		return code, stdout.String(), stderr.String()
	}

	code, stdout, stderr := runCase("domain", "laravel-11.crawl.test")
	if code != exitError {
		t.Fatalf("код = %d, хочу %d; stderr:\n%s", code, exitError, stderr)
	}
	if !strings.Contains(stderr, "domain: config:") {
		t.Errorf("stderr без контекста конфигурации:\n%s", stderr)
	}
	if stdout != "" {
		t.Errorf("stdout до соединения не пуст:\n%s", stdout)
	}

	// Формы имени (регистр/точка/unicode) проходят нормализацию — та же
	// ошибка конфигурации, не «некорректное имя».
	for _, name := range []string{"Laravel-11.CRAWL.Test.", "пример.crawl.test", "xn--e1afmkfd.crawl.test"} {
		code, _, stderr = runCase("domain", "--json", name)
		if code != exitError || !strings.Contains(stderr, "domain: config:") {
			t.Errorf("domain --json %q: код %d, stderr:\n%s; хочу ошибку конфигурации", name, code, stderr)
		}
	}

	// Валидация входа — раньше конфигурации.
	code, _, stderr = runCase("domain", "bad!!name.test")
	if code != exitUsage || !strings.Contains(stderr, "некорректное имя домена") {
		t.Errorf("невалидное имя: код %d, stderr:\n%s; хочу код 2", code, stderr)
	}
	code, _, stderr = runCase("domain", "--json", "--content", "x.crawl.test")
	if code != exitUsage || !strings.Contains(stderr, "несовместимы") {
		t.Errorf("--json --content: код %d, stderr:\n%s; хочу код 2", code, stderr)
	}
}

// Соответствие структур схемы БД: теги db повторяют колонки таблиц
// миграции 000001 в том же порядке (жёсткая сверка — при изменении схемы
// тест напоминает обновить структуры); нулевые значения структур —
// колонкам с DEFAULT.
package model

import (
	"reflect"
	"slices"
	"testing"
)

// TestStructColumnsMatchSchema — колонки каждой таблицы перечислены
// жёстко по migrations/000001_init.up.sql (000002/000003 колонок не
// меняют, 000005 добавляет domains.temp_attempts). Порядок обязателен:
// структуры используются с COPY и pgx.RowToStructByName.
func TestStructColumnsMatchSchema(t *testing.T) {
	cases := []struct {
		row     any
		columns []string
	}{
		{Zone{}, []string{
			"id", "name", "name_unicode", "is_active",
			"domains_total", "domains_checked", "updated_at",
		}},
		{Subdomain{}, []string{"id", "label", "enabled"}},
		{Domain{}, []string{
			"zone_id", "id", "label", "status", "check_state",
			"flags", "dns_mask", "queued_at", "last_checked_at", "temp_attempts",
		}},
		{SubdomainResult{}, []string{
			"zone_id", "domain_id", "subdomain_id", "variant", "scheme",
			"http_status", "final_url", "alias_kind", "alias_subdomain_id",
			"addrs", "cname", "tls", "redirects", "headers", "cookies",
			"rendered", "content_hash", "error", "response_ms",
			"checked_at", "rules_version",
		}},
		{Content{}, []string{
			"hash", "pack_id", "off", "len", "raw_len", "refcount",
		}},
		{LaravelDomain{}, []string{
			"zone_id", "domain_id", "score", "ver_min", "ver_max",
			"found_on_mask", "rules_version", "updated_at",
		}},
		{LaravelFinding{}, []string{
			"zone_id", "domain_id", "subdomain_id", "variant", "score",
			"ver_min", "ver_max", "signals", "is_laravel", "rules_version",
		}},
		{IngestFile{}, []string{
			"id", "zone_id", "action", "path", "sha256",
			"lines_total", "added", "existed", "deleted", "rejected",
			"processed_at",
		}},
	}
	for _, tc := range cases {
		name := reflect.TypeOf(tc.row).Name()
		t.Run(name, func(t *testing.T) {
			got := dbTags(t, tc.row)
			if !slices.Equal(got, tc.columns) {
				t.Errorf("колонки %s = %v,\nwant (миграция 000001) %v", name, got, tc.columns)
			}
		})
	}
}

// dbTags собирает теги db полей структуры по порядку.
func dbTags(t *testing.T, row any) []string {
	t.Helper()
	typ := reflect.TypeOf(row)
	if typ.Kind() != reflect.Struct {
		t.Fatalf("%T — не структура", row)
	}
	tags := make([]string, 0, typ.NumField())
	for i := range typ.NumField() {
		field := typ.Field(i)
		tag := field.Tag.Get("db")
		if tag == "" {
			t.Fatalf("поле %s.%s без тега db", typ.Name(), field.Name)
		}
		tags = append(tags, tag)
	}
	return tags
}

// TestDomainZeroValueMatchesDefaults — нулевое значение Domain совпадает
// со строкой, вставленной DEFAULT-ами колонок (миграция 000001:
// status, check_state, flags, dns_mask DEFAULT 0): значения по умолчанию
// собирать отдельно не нужно.
func TestDomainZeroValueMatchesDefaults(t *testing.T) {
	var d Domain
	if d.Status != StatusNew {
		t.Errorf("Domain{}.Status = %v, want %v (DEFAULT 0)", d.Status, StatusNew)
	}
	if d.CheckState != CheckStatePending {
		t.Errorf("Domain{}.CheckState = %v, want %v (DEFAULT 0)", d.CheckState, CheckStatePending)
	}
	if d.Flags != 0 {
		t.Errorf("Domain{}.Flags = %v, want 0 (DEFAULT 0)", d.Flags)
	}
	if d.DNSMask != 0 {
		t.Errorf("Domain{}.DNSMask = %d, want 0 (DEFAULT 0)", int64(d.DNSMask))
	}
}

// TestSubdomainResultZeroValueMatchesDefaults — DEFAULT колонок
// subdomain_results: variant 0, alias_kind 0, rendered false.
func TestSubdomainResultZeroValueMatchesDefaults(t *testing.T) {
	var r SubdomainResult
	if r.Variant != VariantMain {
		t.Errorf("SubdomainResult{}.Variant = %v, want %v (DEFAULT 0)", r.Variant, VariantMain)
	}
	if r.AliasKind != AliasNone {
		t.Errorf("SubdomainResult{}.AliasKind = %v, want %v (DEFAULT 0)", r.AliasKind, AliasNone)
	}
	if r.Rendered {
		t.Error("SubdomainResult{}.Rendered = true, want false (DEFAULT false)")
	}
}

// TestSubdomainInitialSet — начальный набор субдоменов из миграции 000001:
// бит 0 — апекс, 1 www, …; номер бита совпадает с id справочника.
func TestSubdomainInitialSet(t *testing.T) {
	want := []Subdomain{
		{ID: 0, Label: "", Enabled: true},
		{ID: 1, Label: "www", Enabled: true},
		{ID: 2, Label: "admin", Enabled: true},
		{ID: 3, Label: "crm", Enabled: true},
		{ID: 4, Label: "api", Enabled: true},
		{ID: 5, Label: "app", Enabled: true},
	}
	for _, s := range want {
		if err := ValidateSubdomainID(int(s.ID)); err != nil {
			t.Errorf("начальный субдомен id=%d: %v", s.ID, err)
		}
	}
}

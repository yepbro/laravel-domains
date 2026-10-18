// Юнит-тесты данных лендинга (T-122) на фейковом пуле web_test.go:
// SQL-контракты выборок, живая семантика фейка (статистика, облако с
// лог-масштабом, таблица с долями, последние находки с бейджами маски),
// ошибки не скрываются.
package web

import (
	"context"
	"strings"
	"testing"
)

// TestLandingSQLContracts — тексты выборок зафиксированы: кэши T-120,
// LIMIT облака/таблицы, ORDER BY updated_at по индексу T-120.
func TestLandingSQLContracts(t *testing.T) {
	for sql, want := range map[string]string{
		statsSQL:      "sum(domains_laravel)",
		last24hSQL:    "FROM laravel_history",
		cloudSQL:      "LIMIT 50",
		tableSQL:      "LIMIT 10",
		latestSQL:     "ORDER BY ld.updated_at DESC LIMIT 5",
		subdomainsSQL: "WHERE enabled",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("SQL не содержит %q:\n%s", want, sql)
		}
	}
}

// TestFetchLandingData — семантика: суммы статистики, лог-масштаб
// облака (ранг сохранён, максимум — верх границы), проценты таблицы,
// бейджи found_on_mask и версии находок.
func TestFetchLandingData(t *testing.T) {
	db := &fakeDB{seed: seedLanding()}
	data, err := FetchLandingData(context.Background(), db)
	if err != nil {
		t.Fatalf("FetchLandingData: %v", err)
	}

	if data.Stats.Zones != 12 || data.Stats.Total != 1_000_000 ||
		data.Stats.Checked != 800_000 || data.Stats.Laravel != 4_242 || data.Stats.Last24h != 17 {
		t.Errorf("статистика = %+v", data.Stats)
	}

	if len(data.Cloud) != 3 {
		t.Fatalf("облако = %d зон, хочу 3", len(data.Cloud))
	}
	if data.Cloud[0].Size < 1.49 || data.Cloud[0].Size > 1.5 {
		t.Errorf("максимум облака size = %v, хочу ~1.5 (верх границы)", data.Cloud[0].Size)
	}
	if data.Cloud[2].Size <= 0.875 {
		t.Errorf("минимум облака size = %v, хочу > 0.875", data.Cloud[2].Size)
	}
	if !(data.Cloud[0].Size > data.Cloud[1].Size && data.Cloud[1].Size > data.Cloud[2].Size) {
		t.Errorf("ранг размеров нарушен: %v", data.Cloud)
	}

	if len(data.Table) != 2 {
		t.Fatalf("таблица = %d зон, хочу 2", len(data.Table))
	}
	if p := data.Table[0].Percent; p < 0.24 && p > 0.26 {
		t.Errorf("процент зоны com = %v, хочу 0.25", p)
	}
	if data.Table[0].Checked != 800_000 {
		t.Errorf("checked = %d", data.Table[0].Checked)
	}

	if len(data.Latest) != 1 {
		t.Fatalf("находки = %d, хочу 1", len(data.Latest))
	}
	it := data.Latest[0]
	if it.Name != "shop.example.com" || it.VerText != "9.19–12" {
		t.Errorf("находка = %+v", it)
	}
	// found_on_mask 0b11 — апекс (бит 0, без бейджа) и www (бит 1).
	if len(it.Subdomains) != 1 || it.Subdomains[0].Label != "www" ||
		it.Subdomains[0].Host != "www.shop.example.com" {
		t.Errorf("бейджи = %+v, хочу только www", it.Subdomains)
	}
	if it.UpdatedText != "18.10.2026" {
		t.Errorf("дата = %q", it.UpdatedText)
	}
}

// TestFetchLandingNilPool — без пула — ошибка.
func TestFetchLandingNilPool(t *testing.T) {
	if _, err := FetchLandingData(context.Background(), nil); err == nil {
		t.Fatal("nil пул = nil error, хочу ошибку")
	}
}

// TestFetchLandingQueryError — ошибка запроса всплывает (страница без
// данных не рендерится наполовину).
func TestFetchLandingQueryError(t *testing.T) {
	if _, err := FetchLandingData(context.Background(), brokenDB{}); err == nil {
		t.Fatal("слой БД недоступен = nil error, хочу ошибку")
	}
}

// TestDisplayUnicode — punycode → Unicode, ASCII — как есть.
func TestDisplayUnicode(t *testing.T) {
	if got := displayUnicode("xn--80ak6aa92e.com"); got == "xn--80ak6aa92e.com" {
		t.Errorf("IDN не декодирован: %q", got)
	}
	if got := displayUnicode("example.com"); got != "example.com" {
		t.Errorf("ASCII изменился: %q", got)
	}
	if got := displayUnicode("!!"); got != "!!" {
		t.Errorf("мусор изменился: %q", got)
	}
}

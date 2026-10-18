// Данные лендинга (T-122): дёшево по кэшам T-120 — статистика и зоны
// читают справочник zones целиком (суммы кэширующих счётчиков, строки —
// тысячи), последние находки — индекс laravel_domains_updated_at_idx,
// «находки за 24 ч» — индекс laravel_history_changed_at_idx (T-118).
// Облако зон — топ-50 по domains_laravel, размер метки — лог-масштаб
// (как в larasites: линейный масштаб одной доминирующей зоной .com
// ужал бы остальные в нечитаемость).
package web

import (
	"context"
	"fmt"
	"math"
	"time"

	"golang.org/x/net/idna"

	"crawler/internal/model"
)

// LandingStats — карточки статистики.
type LandingStats struct {
	Zones   int64 // активные зоны
	Total   int64 // сумма zones.domains_total
	Checked int64 // сумма zones.domains_checked
	Laravel int64 // сумма zones.domains_laravel
	Last24h int64 // появления laravel_history за 24 ч (T-118)
}

// ZoneItem — зона облака/таблицы.
type ZoneItem struct {
	Name        string
	NameUnicode string
	Checked     int64
	Laravel     int64
	Percent     float64 // доля laravel/checked, %
	Size        float64 // размер шрифта облака, rem (лог-масштаб)
}

// LatestItem — последняя находка: домен, бейджи субдоменов, диапазон
// версий, время обновления.
type LatestItem struct {
	Name        string
	Subdomains  []Badge // субдомены с готовыми хостами для ссылок
	VerText     string  // «9.19–11» или "" (диапазон не определён)
	UpdatedText string
}

// Badge — бейдж субдомена находки.
type Badge struct {
	Label string // метка ("" — апекс, бейдж не строится)
	Host  string // label.domain — готовый хост ссылки
}

// LandingData — всё, что рендерит страница.
type LandingData struct {
	Stats  LandingStats
	Cloud  []ZoneItem
	Table  []ZoneItem
	Latest []LatestItem
}

// SQL данных (контракты — тесты на фейковом db.DB).
const (
	statsSQL = "SELECT count(*), COALESCE(sum(domains_total), 0), " +
		"COALESCE(sum(domains_checked), 0), COALESCE(sum(domains_laravel), 0) " +
		"FROM zones WHERE is_active"

	last24hSQL = "SELECT count(*) FROM laravel_history " +
		"WHERE appeared AND changed_at >= now() - interval '24 hours'"

	cloudSQL = "SELECT name, COALESCE(name_unicode, ''), domains_laravel " +
		"FROM zones WHERE domains_laravel > 0 " +
		"ORDER BY domains_laravel DESC LIMIT 50"

	tableSQL = "SELECT name, COALESCE(name_unicode, ''), domains_checked, domains_laravel " +
		"FROM zones WHERE domains_checked > 0 AND domains_laravel > 0 " +
		"ORDER BY domains_laravel DESC LIMIT 10"

	latestSQL = "SELECT d.label || '.' || z.name AS name, ld.found_on_mask, " +
		"COALESCE(ld.ver_min, ''), COALESCE(ld.ver_max, ''), ld.updated_at " +
		"FROM laravel_domains AS ld " +
		"JOIN domains AS d ON d.zone_id = ld.zone_id AND d.id = ld.domain_id " +
		"JOIN zones AS z ON z.id = ld.zone_id " +
		"ORDER BY ld.updated_at DESC LIMIT 5"

	subdomainsSQL = "SELECT id, label FROM subdomains WHERE enabled ORDER BY id"
)

// FetchLandingData собирает данные страницы одним проходом по дешёвым
// кэшам; ошибки запросов не скрываются — страница без данных бессмысленна
// (read-only, деградация отдельных секций — паттерн дашборда T-129, не
// публичной страницы).
func FetchLandingData(ctx context.Context, pool Querier) (LandingData, error) {
	if pool == nil {
		return LandingData{}, fmt.Errorf("web: данные лендинга: пул не задан")
	}
	var data LandingData

	rows, err := pool.Query(ctx, statsSQL)
	if err != nil {
		return LandingData{}, fmt.Errorf("web: статистика: %w", err)
	}
	for rows.Next() {
		if err := rows.Scan(&data.Stats.Zones, &data.Stats.Total,
			&data.Stats.Checked, &data.Stats.Laravel); err != nil {
			rows.Close()
			return LandingData{}, fmt.Errorf("web: статистика: чтение: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return LandingData{}, fmt.Errorf("web: статистика: %w", err)
	}
	rows.Close()
	data.Stats.Last24h, err = countRows(ctx, pool, last24hSQL, "находки за 24 ч")
	if err != nil {
		return LandingData{}, err
	}

	if data.Cloud, err = fetchZones(ctx, pool, cloudSQL, false); err != nil {
		return LandingData{}, err
	}
	sizeCloud(data.Cloud)
	if data.Table, err = fetchZones(ctx, pool, tableSQL, true); err != nil {
		return LandingData{}, err
	}
	for i := range data.Table {
		if c := data.Table[i].Checked; c > 0 {
			data.Table[i].Percent = math.Round(float64(data.Table[i].Laravel)/float64(c)*10000) / 100
		}
	}
	if data.Latest, err = fetchLatest(ctx, pool); err != nil {
		return data, err
	}
	return data, nil
}

// countRows — единственное значение агрегата (db.DB без QueryRow, стиль
// monitor.NearStatsOnce).
func countRows(ctx context.Context, pool Querier, sql, what string) (int64, error) {
	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return 0, fmt.Errorf("web: %s: %w", what, err)
	}
	defer rows.Close()
	var n int64
	if !rows.Next() {
		return 0, fmt.Errorf("web: %s: пустой ответ агрегата", what)
	}
	if err := rows.Scan(&n); err != nil {
		return 0, fmt.Errorf("web: %s: чтение: %w", what, err)
	}
	return n, rows.Err()
}

func fetchZones(ctx context.Context, pool Querier, sql string, withChecked bool) ([]ZoneItem, error) {
	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("web: зоны: %w", err)
	}
	defer rows.Close()
	var out []ZoneItem
	for rows.Next() {
		var z ZoneItem
		dest := []any{&z.Name, &z.NameUnicode, &z.Laravel}
		if withChecked {
			dest = []any{&z.Name, &z.NameUnicode, &z.Checked, &z.Laravel}
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("web: зоны: чтение: %w", err)
		}
		out = append(out, z)
	}
	return out, rows.Err()
}

// sizeCloud — лог-масштаб размеров меток облака: 0.875..1.5 rem по
// log(1+count)/log(1+max) — порядок разницы .com vs малой зоны сжат
// без потери ранга.
func sizeCloud(zones []ZoneItem) {
	if len(zones) == 0 {
		return
	}
	max := zones[0].Laravel
	if max < 1 {
		max = 1
	}
	denom := math.Log1p(float64(max))
	for i := range zones {
		frac := 0.0
		if denom > 0 {
			frac = math.Log1p(float64(zones[i].Laravel)) / denom
		}
		zones[i].Size = 0.875 + 0.625*frac
	}
}

func fetchLatest(ctx context.Context, pool Querier) ([]LatestItem, error) {
	// Справочник субдоменов — расшифровка бейджей found_on_mask.
	subs := map[int16]string{}
	srows, err := pool.Query(ctx, subdomainsSQL)
	if err != nil {
		return nil, fmt.Errorf("web: справочник субдоменов: %w", err)
	}
	for srows.Next() {
		var id int16
		var label string
		if err := srows.Scan(&id, &label); err != nil {
			srows.Close()
			return nil, fmt.Errorf("web: справочник субдоменов: чтение: %w", err)
		}
		subs[id] = label
	}
	srows.Close()
	if err := srows.Err(); err != nil {
		return nil, fmt.Errorf("web: справочник субдоменов: %w", err)
	}

	rows, err := pool.Query(ctx, latestSQL)
	if err != nil {
		return nil, fmt.Errorf("web: последние находки: %w", err)
	}
	defer rows.Close()
	var out []LatestItem
	for rows.Next() {
		var (
			it      LatestItem
			mask    int64
			verMin  string
			verMax  string
			updated time.Time
		)
		if err := rows.Scan(&it.Name, &mask, &verMin, &verMax, &updated); err != nil {
			return nil, fmt.Errorf("web: последние находки: чтение: %w", err)
		}
		m := model.DNSMask(mask)
		for _, id := range m.IDs() {
			label, ok := subs[int16(id)]
			if !ok || label == "" {
				continue // апекс (бит 0) бейджем не отмечается
			}
			it.Subdomains = append(it.Subdomains, Badge{Label: label, Host: label + "." + it.Name})
		}
		switch {
		case verMin != "" && verMax != "":
			it.VerText = verMin + "–" + verMax
		case verMin != "":
			it.VerText = "≥ " + verMin
		case verMax != "":
			it.VerText = "≤ " + verMax
		}
		it.UpdatedText = updated.UTC().Format("02.01.2006")
		out = append(out, it)
	}
	return out, rows.Err()
}

// displayUnicode — имя домена в Unicode для показа (punycode-метки
// декодируются, ASCII — как есть; ошибка декодирования — исходная строка).
func displayUnicode(name string) string {
	if u, err := idna.Lookup.ToUnicode(name); err == nil {
		return u
	}
	return name
}

// Landing data (T-122): cheap off the T-120 caches — the stats and the
// zones read the zones reference table in full (sums of caching
// counters, thousands of rows), the latest finds — the
// laravel_domains_updated_at_idx index, "finds in 24 h" — the
// laravel_history_changed_at_idx index (T-118). The zone cloud — the
// top 50 by domains_laravel, the label size is a log scale (as in
// larasites: a linear scale with one dominating .com zone would squeeze
// the rest into unreadability).
package web

import (
	"context"
	"fmt"
	"math"
	"time"

	"golang.org/x/net/idna"

	"crawler/internal/model"
)

// LandingStats — the statistics cards.
type LandingStats struct {
	Zones   int64 // active zones
	Total   int64 // the sum of zones.domains_total
	Checked int64 // the sum of zones.domains_checked
	Laravel int64 // the sum of zones.domains_laravel
	Last24h int64 // laravel_history appearances in 24 h (T-118)
}

// ZoneItem — a zone of the cloud/table.
type ZoneItem struct {
	Name        string
	NameUnicode string
	Checked     int64
	Laravel     int64
	Percent     float64 // the laravel/checked share, %
	Size        float64 // the cloud font size, rem (log scale)
}

// LatestItem — the latest find: the domain, the subdomain badges, the
// version range, the update time.
type LatestItem struct {
	Name        string
	Subdomains  []Badge // subdomains with ready hosts for links
	VerText     string  // "9.19–11" or "" (the range is undetermined)
	UpdatedText string
}

// Badge — a find's subdomain badge.
type Badge struct {
	Label string // the label ("" — the apex, no badge is built)
	Host  string // label.domain — the ready host of the link
}

// LandingData — everything the page renders.
type LandingData struct {
	Stats  LandingStats
	Cloud  []ZoneItem
	Table  []ZoneItem
	Latest []LatestItem
}

// Data SQL (the contracts — tests on the fake db.DB).
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

// FetchLandingData collects the page data in a single pass over the
// cheap caches; query errors are not hidden — a page without data is
// meaningless (read-only; the degradation of individual sections — the
// dashboard pattern of T-129, not a public page).
func FetchLandingData(ctx context.Context, pool Querier) (LandingData, error) {
	if pool == nil {
		return LandingData{}, fmt.Errorf("web: landing data: the pool is not set")
	}
	var data LandingData

	rows, err := pool.Query(ctx, statsSQL)
	if err != nil {
		return LandingData{}, fmt.Errorf("web: statistics: %w", err)
	}
	for rows.Next() {
		if err := rows.Scan(&data.Stats.Zones, &data.Stats.Total,
			&data.Stats.Checked, &data.Stats.Laravel); err != nil {
			rows.Close()
			return LandingData{}, fmt.Errorf("web: statistics: read: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return LandingData{}, fmt.Errorf("web: statistics: %w", err)
	}
	rows.Close()
	data.Stats.Last24h, err = countRows(ctx, pool, last24hSQL, "finds in 24 h")
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

// countRows — the single value of an aggregate (db.DB without QueryRow,
// the monitor.NearStatsOnce style).
func countRows(ctx context.Context, pool Querier, sql, what string) (int64, error) {
	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return 0, fmt.Errorf("web: %s: %w", what, err)
	}
	defer rows.Close()
	var n int64
	if !rows.Next() {
		return 0, fmt.Errorf("web: %s: an empty aggregate answer", what)
	}
	if err := rows.Scan(&n); err != nil {
		return 0, fmt.Errorf("web: %s: read: %w", what, err)
	}
	return n, rows.Err()
}

func fetchZones(ctx context.Context, pool Querier, sql string, withChecked bool) ([]ZoneItem, error) {
	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("web: zones: %w", err)
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
			return nil, fmt.Errorf("web: zones: read: %w", err)
		}
		out = append(out, z)
	}
	return out, rows.Err()
}

// sizeCloud — a log scale of the cloud label sizes: 0.875..1.5 rem by
// log(1+count)/log(1+max) — the order-of-magnitude difference between
// .com and a small zone is compressed without losing rank.
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
	// The subdomain reference table — the decoding of the found_on_mask
	// badges.
	subs := map[int16]string{}
	srows, err := pool.Query(ctx, subdomainsSQL)
	if err != nil {
		return nil, fmt.Errorf("web: the subdomain reference table: %w", err)
	}
	for srows.Next() {
		var id int16
		var label string
		if err := srows.Scan(&id, &label); err != nil {
			srows.Close()
			return nil, fmt.Errorf("web: the subdomain reference table: read: %w", err)
		}
		subs[id] = label
	}
	srows.Close()
	if err := srows.Err(); err != nil {
		return nil, fmt.Errorf("web: the subdomain reference table: %w", err)
	}

	rows, err := pool.Query(ctx, latestSQL)
	if err != nil {
		return nil, fmt.Errorf("web: the latest finds: %w", err)
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
			return nil, fmt.Errorf("web: the latest finds: read: %w", err)
		}
		m := model.DNSMask(mask)
		for _, id := range m.IDs() {
			label, ok := subs[int16(id)]
			if !ok || label == "" {
				continue // the apex (bit 0) is not marked with a badge
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
		it.UpdatedText = updated.UTC().Format("Jan 02, 2006")
		out = append(out, it)
	}
	return out, rows.Err()
}

// displayUnicode — the domain name in Unicode for display (punycode
// labels are decoded, ASCII as is; a decode error — the original
// string).
func displayUnicode(name string) string {
	if u, err := idna.Lookup.ToUnicode(name); err == nil {
		return u
	}
	return name
}

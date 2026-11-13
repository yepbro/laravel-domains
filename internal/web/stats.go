// The landing stats API (L-03, TZ-ld-landing §7.1): GET
// /api/v1/landing/stats — the JSON equivalent of the SSR landing data for
// the static landing of the ld-landing repo (the page HTML is built by
// vite-ssg, the numbers arrive with a client fetch — "always fresh" over
// a rebuildable cache). The data pass is the one and the same
// FetchLandingData of data.go (the T-120/T-118 caches; no duplicated
// queries); this file only assembles the wire view model: zone/domain
// names are strings, the counters are numbers, the internal structures
// (model.DNSMask, Badge, ZoneItem) are not leaked as is.
//
// The response is public and immutable for a minute — Cache-Control:
// public, max-age=60 (the data pass is cheap, but a static landing
// hammering it on every visit does not make it fresher). A DB failure
// is a 500 without details, in the uniform JSON error body of the
// /api/* surface — a public endpoint does not degrade halfway and does
// not reveal internals.
package web

import (
	"encoding/json"
	"math"
	"net/http"
)

// statsResponse — the response body of GET /api/v1/landing/stats: the
// statistics cards, the top-50 zone cloud, the top-10 zone table, the
// latest finds, the subdomain reference table. Empty collections are []
// (never null) — a uniform contract for the TypeScript consumer.
type statsResponse struct {
	Stats      statsCounters    `json:"stats"`
	Cloud      []statsCloudZone `json:"cloud"`
	Table      []statsTableZone `json:"table"`
	Latest     []statsFind      `json:"latest"`
	Subdomains []statsSubdomain `json:"subdomains"`
}

// statsCounters — the statistics cards (the landing hero counters): the
// active zones, the domains_total/domains_checked/domains_laravel sums
// over the zones reference table, the laravel_history finds of the last
// 24 hours.
type statsCounters struct {
	Zones          int64 `json:"zones"`
	Total          int64 `json:"total"`
	Checked        int64 `json:"checked"`
	Laravel        int64 `json:"laravel"`
	LaravelLast24h int64 `json:"laravel_last_24h"`
}

// statsCloudZone — a zone of the cloud (top-50 by domains_laravel): the
// name (ASCII/punycode, as stored) and the Unicode display form ("" for
// ASCII zones — omitted), the laravel count (the tooltip), and the
// precomputed log-scale label size in rem (the same 0.875..1.5 range
// the SSR template rendered with %.3f — rounded here so the static
// landing does not reimplement the scale).
type statsCloudZone struct {
	Name        string  `json:"name"`
	NameUnicode string  `json:"name_unicode,omitempty"`
	Laravel     int64   `json:"laravel"`
	Size        float64 `json:"size"`
}

// statsTableZone — a row of the top-10 table: the checked/laravel
// counters and the laravel share of the checked ones, % (two decimals,
// as rendered).
type statsTableZone struct {
	Name        string  `json:"name"`
	NameUnicode string  `json:"name_unicode,omitempty"`
	Checked     int64   `json:"checked"`
	Laravel     int64   `json:"laravel"`
	Percent     float64 `json:"percent"`
}

// statsFind — a latest find: the domain, the subdomain badges (label +
// the ready host label.domain for the link — `badges`, not
// `subdomains`: within this document "subdomains" is the top-level
// reference table), the version range text ("9.19–12", "≥ 9.19" — ""
// when undetermined, omitted) and the preformatted update date
// ("Jan 02, 2006" — the display form of the SSR page, the locale
// formatting stays on the consumer).
type statsFind struct {
	Name    string       `json:"name"`
	Badges  []statsBadge `json:"badges"`
	Version string       `json:"versions,omitempty"`
	Updated string       `json:"updated"`
}

// statsBadge — a find's subdomain badge.
type statsBadge struct {
	Label string `json:"label"`
	Host  string `json:"host"`
}

// statsSubdomain — an entry of the subdomain reference table: what the
// pipeline checks on each domain (the id is the found_on_mask bit; the
// apex — id 0 with an empty label — is included for the completeness of
// the directory).
type statsSubdomain struct {
	ID    int16  `json:"id"`
	Label string `json:"label"`
}

// newStatsResponse — the view model from the data pass.
func newStatsResponse(d LandingData) statsResponse {
	resp := statsResponse{
		Cloud:      make([]statsCloudZone, 0, len(d.Cloud)),
		Table:      make([]statsTableZone, 0, len(d.Table)),
		Latest:     make([]statsFind, 0, len(d.Latest)),
		Subdomains: make([]statsSubdomain, 0, len(d.Subdomains)),
	}
	resp.Stats = statsCounters{
		Zones:          d.Stats.Zones,
		Total:          d.Stats.Total,
		Checked:        d.Stats.Checked,
		Laravel:        d.Stats.Laravel,
		LaravelLast24h: d.Stats.Last24h,
	}
	for _, z := range d.Cloud {
		resp.Cloud = append(resp.Cloud, statsCloudZone{
			Name:        z.Name,
			NameUnicode: z.NameUnicode,
			Laravel:     z.Laravel,
			Size:        math.Round(z.Size*1000) / 1000, // %.3f of the template
		})
	}
	for _, z := range d.Table {
		resp.Table = append(resp.Table, statsTableZone{
			Name:        z.Name,
			NameUnicode: z.NameUnicode,
			Checked:     z.Checked,
			Laravel:     z.Laravel,
			Percent:     z.Percent,
		})
	}
	for _, it := range d.Latest {
		find := statsFind{
			Name:    it.Name,
			Badges:  make([]statsBadge, 0, len(it.Subdomains)),
			Version: it.VerText,
			Updated: it.UpdatedText,
		}
		for _, b := range it.Subdomains {
			find.Badges = append(find.Badges, statsBadge(b))
		}
		resp.Latest = append(resp.Latest, find)
	}
	for _, s := range d.Subdomains {
		resp.Subdomains = append(resp.Subdomains, statsSubdomain(s))
	}
	return resp
}

// handleLandingStats — GET /api/v1/landing/stats: the JSON of the very
// same data pass as the SSR landing (FetchLandingData; non-GET methods
// do not reach the handler — the router answers 405). A DB failure — a
// 500 without revealing details: the body is the uniform JSON error
// format of the /api/* surface (checkWriteError, T-227) — the consumer
// is the TypeScript client of ld-landing, not a browser page, so the
// text/plain of handleLanding does not fit.
func (s *Server) handleLandingStats(w http.ResponseWriter, r *http.Request) {
	data, err := FetchLandingData(r.Context(), s.db)
	if err != nil {
		s.opt.Log.Warn("landing stats", "err", err)
		checkWriteError(w, "unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=60")
	if err := json.NewEncoder(w).Encode(newStatsResponse(data)); err != nil {
		// The headers are gone — the status cannot be changed; log and
		// stop (a client disconnect mid-encode — the handleLanding
		// render parity).
		s.opt.Log.Warn("landing stats encode", "err", err)
	}
}

// ---------------------------------------------------------------- near
//
// A command for triaging the Laravel detection gray zone (T-115, following
// the larasites analysis): laravel_findings stores all rule firings
// regardless of the threshold (T-071) — the gray zone (min_score/2 <= score
// < min_score, "near-firings") could only be seen by hand-written SQL
// before this command. The purpose — calibration material for weights and
// the threshold (T-116/T-117) and triage of contested cases: it prints the
// domain, the fired signals and the evidence — like the Laravel section of
// crawlerctl domain, but across many domains.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"crawler/internal/analyzer"
	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/idn"
	"crawler/internal/model"
)

// nearHelp — the crawlerctl near --help text.
const nearHelp = `crawlerctl near — находки Laravel в серой зоне порога (T-115)

Использование:
  crawlerctl near [--zone ИМЯ] [--min-score F]

Выводит находки laravel_findings с min-score/2 <= score < min-score
(«почти-срабатывания»: признаки есть, порог is_laravel не пройден) —
домен, субдомен, сработавшие правила с evidence. Материал калибровки
весов правил и порога: стабильная серая зона — норма, всплеск после
смены правил — сигнал деградации точности.

Порог по умолчанию — analyzer.laravel_min_score из конфигурации (боевой
порог записи в laravel_domains, как test-rules); без конфигурации — 0.6.
Явный --min-score переопределяет оба источника. Зона — punycode-имя
справочника zones (unicode приводится к punycode).
Коды возврата: 0 — успех, 1 — ошибка, 2 — использование.
`

// nearRow — a gray-zone selection row: domain + finding (subdomain×variant).
type nearRow struct {
	ZoneID      int16
	DomainID    int64
	Name        string
	SubdomainID int16
	Subdomain   string
	Variant     int16
	Score       float32
	VerMin      string
	VerMax      string
	IsLaravel   bool
	Signals     []domainSignal // the laravel_findings.signals format (T-071)
}

// nearSelectSQL — the gray-zone selection: findings JOINed with domains and
// zones (the domain name for output), subdomains via LEFT JOIN (the bit is
// bound to the id forever, but with corrupted data the reference row can in
// theory be missing — COALESCE is more honest than a crash). The zone is a
// filter parameter (NULL — all zones): an operator value, not schema logic.
// The order is score DESC (the ones closest to the threshold first — the
// main triage material), the domain key at the tail determines the batch.
const nearSelectSQL = "SELECT f.zone_id, f.domain_id, d.label || '.' || z.name AS name, " +
	"f.subdomain_id, COALESCE(s.label, '') AS subdomain, f.variant, " +
	"f.score, COALESCE(f.ver_min, ''), COALESCE(f.ver_max, ''), f.is_laravel, f.signals " +
	"FROM laravel_findings AS f " +
	"JOIN domains AS d ON d.zone_id = f.zone_id AND d.id = f.domain_id " +
	"JOIN zones AS z ON z.id = f.zone_id " +
	"LEFT JOIN subdomains AS s ON s.id = f.subdomain_id " +
	"WHERE ($1::int2 IS NULL OR f.zone_id = $1) " +
	"AND f.score >= $2::real AND f.score < $3::real " +
	"ORDER BY f.score DESC, f.zone_id, f.domain_id, f.subdomain_id, f.variant"

func cmdNear(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("near")
	zone := fs.String("zone", "", "зона находок (punycode; пусто — все)")
	minScore := fs.Float64("min-score", 0.6,
		"порог is_laravel (по умолчанию analyzer.laravel_min_score из конфигурации, без неё 0.6)")
	if handled, err := parseFlags(fs, nearHelp, args, stdout); handled {
		return err
	}
	zoneName := ""
	if *zone != "" {
		// Input validation — through the same pipeline as the incoming
		// directories (internal/idn.NormalizeZone): the zone is stored in punycode.
		normalized, err := idn.NormalizeZone(*zone)
		if err != nil {
			return usagef("near: некорректное имя зоны %q: %v", *zone, err)
		}
		zoneName = normalized
	}
	if fs.NArg() > 0 {
		return usagef("near: лишние аргументы %v (команда принимает только --zone и --min-score)", fs.Args())
	}
	if !(*minScore > 0 && *minScore < 1) {
		return usagef("near: порог %v вне (0, 1)", *minScore)
	}

	// The default threshold is kept in sync with the production configuration
	// (CLI-06, the test-rules style): the configuration is mandatory here
	// (the DSN is needed), so that source is more reliable than the default —
	// an explicit flag takes precedence over both.
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("near: %w", err)
	}
	if !flagExplicit(fs, "min-score") {
		*minScore = cfg.Analyzer.LaravelMinScore
	}

	ctx, stop := signalCtx()
	defer stop()

	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2           // connection budget of spec 6.3 — as the services
	poolOpts.AppName = "crawlerctl" // label in pg_stat_activity (T-177)
	pool, err := db.Open(ctx, poolOpts)
	if err != nil {
		return fmt.Errorf("near: %w", err)
	}
	defer pool.Close()

	zoneDesc := "все"
	var zoneID *int16
	if zoneName != "" {
		id, found, err := analyzer.ZoneIDByName(ctx, pool, zoneName)
		if err != nil {
			return fmt.Errorf("near: %w", err)
		}
		if !found {
			return fmt.Errorf("near: зона %s не найдена в справочнике (зоны создаёт ingest при первом файле)", zoneName)
		}
		zoneID, zoneDesc = &id, zoneName
	}

	rows, err := nearFetch(ctx, pool, zoneID, *minScore)
	if err != nil {
		return fmt.Errorf("near: %w", err)
	}

	out(stdout, "near: серая зона [%.3f .. %.3f), зона: %s, находок: %d\n\n",
		*minScore/2, *minScore, zoneDesc, len(rows))
	domains := 0
	prevDomain := int64(-1)
	for _, r := range rows {
		if r.DomainID != prevDomain {
			domains++
			prevDomain = r.DomainID
		}
		nearPrintRow(stdout, r)
	}
	if len(rows) == 0 {
		out(stdout, "  (пусто — слабых находок в диапазоне нет)\n")
		return nil
	}
	out(stdout, "\nnear: доменов: %d\n", domains)
	return nil
}

// nearFetch runs the selection and parses the signals of each row.
func nearFetch(ctx context.Context, pool db.DB, zoneID *int16, minScore float64) ([]nearRow, error) {
	var zoneArg any
	if zoneID != nil {
		zoneArg = *zoneID
	}
	rs, err := pool.Query(ctx, nearSelectSQL, zoneArg, float32(minScore/2), float32(minScore))
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	var res []nearRow
	for rs.Next() {
		var (
			r       nearRow
			signals []byte
		)
		if err := rs.Scan(&r.ZoneID, &r.DomainID, &r.Name, &r.SubdomainID,
			&r.Subdomain, &r.Variant, &r.Score, &r.VerMin, &r.VerMax,
			&r.IsLaravel, &signals); err != nil {
			return nil, fmt.Errorf("чтение находки: %w", err)
		}
		if len(signals) > 0 {
			if err := json.Unmarshal(signals, &r.Signals); err != nil {
				return nil, fmt.Errorf("signals домена %s: %w", r.Name, err)
			}
		}
		res = append(res, r)
	}
	if err := rs.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

// nearPrintRow — prints one finding: a domain line + signals with evidence
// (the style of the Laravel section of crawlerctl domain / test-rules).
func nearPrintRow(w io.Writer, r nearRow) {
	sub := r.Subdomain
	if sub == "" {
		sub = "(апекс)"
	}
	variant := "main"
	if model.Variant(r.Variant) == model.VariantHTTP {
		variant = "http"
	}
	ver := "-"
	if r.VerMin != "" || r.VerMax != "" {
		ver = fmt.Sprintf("[%s..%s]", orDash(r.VerMin), orDash(r.VerMax))
	}
	verdict := ""
	if r.IsLaravel {
		verdict = "  is_laravel!"
	}
	out(w, "%s  %s/%s  score=%.3f  ver=%s  правил=%d%s\n",
		r.Name, sub, variant, r.Score, ver, len(r.Signals), verdict)
	for _, s := range r.Signals {
		out(w, "    - %-28s w=%.2f  «%s»\n", s.Rule, s.Weight, s.Evidence)
	}
}

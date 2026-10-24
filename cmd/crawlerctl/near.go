// ---------------------------------------------------------------- near
//
// Команда разбора серой зоны Laravel-детекции (T-115, по итогам анализа
// larasites): laravel_findings хранит все срабатывания правил независимо
// от порога (T-071) — серая зона (min_score/2 <= score < min_score,
// «почти-срабатывания») до этой команды видна только SQL-запросом руками.
// Назначение — материал калибровки весов/порога (T-116/T-117) и разбор
// спорных случаев: вывод домена, сработавших сигналов и evidence — как
// раздел Laravel crawlerctl domain, но по множеству доменов.

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

// nearHelp — справка crawlerctl near --help.
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

// nearRow — строка выборки серой зоны: домен + находка (субдомен×вариант).
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
	Signals     []domainSignal // формат laravel_findings.signals (T-071)
}

// nearSelectSQL — выборка серой зоны: findings сJOIN доменов и зон (имя
// домена для вывода), субдомены — LEFT JOIN (бит закреплён за id навсегда,
// но строка справочника в теории может отсутствовать у порченых данных —
// COALESCE честнее падения). Зона — параметр-фильтр (NULL — все зоны):
// значение оператора, не код схемы. Порядок — score DESC (самые близкие
// к порогу первыми — главный материал разбора), доменный ключ в хвосте
// детерминирует пачку.
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
		// Валидация входа — тем же конвейером, что и каталоги incoming
		// (internal/idn.NormalizeZone): зона хранится в punycode.
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

	// Порог по умолчанию синхронизирован с боевой конфигурацией (CLI-06,
	// стиль test-rules): конфигурация здесь обязательна (нужен DSN), так
	// что источник надёжнее дефолта — явный флаг важнее обоих.
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
	poolOpts.PoolSize = 2           // бюджет соединений ТЗ 6.3 — у сервисов
	poolOpts.AppName = "crawlerctl" // метка в pg_stat_activity (T-177)
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

// nearFetch выполняет выборку и разбирает signals каждой строки.
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

// nearPrintRow — вывод одной находки: строка домена + сигналы с evidence
// (стиль раздела Laravel crawlerctl domain / test-rules).
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

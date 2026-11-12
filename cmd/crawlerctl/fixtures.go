// ---------------------------------------------------------------- fixtures
//
// crawlerctl fixtures (T-242, the render engine selection task spec
// tasks/TZ_render_engines.md §6.1 items 1–2): export of a domain fixture
// from the production DB for the local render engine comparison bench
// (§5.1) and smoke runs of pinned-version changes (§6.1 item 6). A
// one-off stage tool: the export reads the DB; dump import and task
// publishing — fixtures import (T-243, fixtures_import.go); the content
// store is not exported — the DOM on the bench is written by the render
// itself, the baseline is captured there too.
//
// The selection (§6.1 item 1) — subdomain_results rows that were real
// q:render tasks: the base predicate final_url IS NOT NULL is the render
// target criterion of the rework pass selection (renderPendingRowsSQL,
// T-057: the SPA verdict passed, the HTTP episode record was published
// to q:render). Strata:
//
//	ok ~2000     rendered — a successful chromium render (the bench
//	             baseline);
//	failed ~500  render_failed_at IS NOT NULL within the last --weeks
//	             weeks — finalizations without a render: tarpits
//	             T-148/timeouts/task class T-172 (potential fallback
//	             clients) AND engine-independent cert T-159/DNS T-211
//	             (not clients, FR-7) — the class is not stored in the
//	             row and cannot be split (see the format contract
//	             below);
//	heavy ~500   rendered with the largest content (contents.raw_len —
//	             the DOM size reference) — SPA-heavy.
//
// ok and heavy do not intersect (heavy is selected first, ok excludes
// its keys with an anti-join), failed does not intersect them by the
// T-210 marker invariant ("render_failed_at set ⇒ rendered = false");
// duplicate keys are additionally cut off by the selection collector.
// Zone proportions — a zone's quota equals its share of the stratum
// (the largest remainder method); within a zone the rows are ordered by
// a deterministic pseudo-random md5(seed:key) hash — re-exporting with
// the same DB state and the same --seed reproduces the selection
// (export idempotency). Fewer available rows than the quota (a dev DB,
// a fresh production) — not an error: all are taken, and the degradation
// is honestly recorded in the output and the manifest.
//
// The dump format (fixtures format v1) — a directory of five files:
//
//	zones.ndjson              rows of the zones reference table: the
//	                          columns id/name/name_unicode/is_active/
//	                          domains_total/domains_checked/updated_at;
//	                          the live zone progress counters
//	                          (domains_laravel, domains_pending,
//	                          domains_queued) are not exported — the
//	                          bench's monitor recomputes them (an
//	                          explicit column list, not "all": an
//	                          "all columns" wording would disagree with
//	                          the schema on every migration)
//	domains.ndjson            rows of domains for the selected domains
//	                          (all columns — domain identity and state)
//	subdomain_results.ndjson  the selected rows: identifiers, render task
//	                          fields (final_url — the render target,
//	                          cookies/headers — the episode context,
//	                          alias_* — T-055 verdict inheritance) and
//	                          stratum diagnostics (rendered,
//	                          render_failed_at, error, response_ms) +
//	                          the service field stratum
//	smoke.ndjson              keys of the smoke subsample (zone_id,
//	                          domain_id, name) — domains of the reduced
//	                          §6.1 item 6 run
//	manifest.json             metadata: the format version, seed, quotas
//	                          and actual stratum/zone counters,
//	                          degradations
//
// NDJSON: one JSON object per line, UTF-8, without HTML escaping. Row
// identifiers are kept original — re-import is idempotent by primary
// keys (INSERT … ON CONFLICT — the T-243 import). Values: bytea → base64
// (content_hash — 24 chars); timestamptz and date → RFC 3339; jsonb
// (headers/cookies) → nested JSON as-is; NULL of any column → null. The
// addrs/cname/tls/redirects columns are not exported: the render task
// does not need them, de-identification per §6.1 item 2 (final_url is
// public by the task definition; the spec explicitly asks to export
// cookies/headers).
//
// The failed stratum — all finalizations without a render by the T-210
// marker: tarpit T-148, task class T-172, cert T-159, DNS T-211. The
// finalization class is not stored in the subdomain_results row (the
// error column is about the HTTP phase), so the engine-independent
// cert/DNS (not fallback clients, FR-7) cannot be separated without a
// schema change — their share is visible on the bench via the
// finalization reasons (the taskFinalizeReason dictionary, §4.6);
// recorded for interpreting the bench results.
//
// Each file is written to a temporary name and renamed after a
// successful close; manifest.json appears last — its presence means a
// completed dump (an interrupted export leaves no half-files under the
// final names). The previous manifest.json is removed before writing the
// first data file of a re-export — a run that fails mid-write does not
// leave an old manifest next to the new files.
//
// Re-export idempotency: the SELECTION COMPOSITION and the fields of the
// subdomain_results/smoke rows are deterministic (with unchanged data
// and the same --seed — byte for byte). The domains/zones rows are a
// snapshot of the state at export time: live fields (domains.status/
// check_state/flags/dns_mask/queued_at/last_checked_at, zone counters
// and updated_at, the manifest's created_at) change between runs
// together with the DB; this does not hinder the T-243 import (row keys
// are stable), the bench monitor recomputes the zone counters.
//
// The T-243 import (schema notes): zones.id — GENERATED ALWAYS AS
// IDENTITY (inserting original ids — OVERRIDING SYSTEM VALUE),
// domains.id — nextval('domain_id_seq') (after the import — setval above
// the maximum exported id, otherwise the bench's new domains would start
// at 1 and collide with the fixture).

package main

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"crawler/internal/db"
)

// fixturesHelp — help text for crawlerctl fixtures [--help]; it is also
// printed for fixtures <action> --help.
const fixturesHelp = `crawlerctl fixtures — фикстуры стенда сравнения движков рендера (T-242/T-243/T-244)

Использование:
  crawlerctl fixtures export --out КАТАЛОГ [--ok N] [--failed N]
                              [--heavy N] [--smoke N] [--weeks N]
                              [--seed N]
  crawlerctl fixtures import --dir КАТАЛОГ [--force] [--publish all|smoke]
  crawlerctl fixtures snapshot --dir КАТАЛОГ --label ИМЯ --engine ИМЯ
                               [--metrics URL] [--engine-rss-max БАЙТ]
                               [--engine-crashes N] [--out ФАЙЛ] [--no-dom]
  crawlerctl fixtures report --baseline ФАЙЛ --candidate ФАЙЛ
                               [--out ФАЙЛ] [--memory-budget БАЙТ]
                               [--fail-exit]

Выгрузка фикстуры доменов с БД (ТЗ рендера §6.1 п. 1–2): выборка строк
subdomain_results — реальных задач q:render (final_url после SPA-вердикта
T-057) — стратами ok (~2000 случайных успешных у chromium) / failed
(~500 проблемных последних недель: финализации без рендера по маркеру
render_failed_at T-210 — тарпиты/класс задачи T-172, включая движко-
независимые cert/DNS: класс в строке не хранится, их доля видна на стенде
по причинам финализаций) / heavy (~500 SPA-тяжёлых — самый крупный
контент), пропорционально долям зон. Плюс smoke-подвыборка доменов (~300)
для сокращённых прогонов смены версий движков. Контент-хранилище не
выгружается (DOM пишет рендер стенда).

  --out КАТАЛОГ  каталог дампа (обязателен); создаётся, существующие
                 файлы дампа перезаписываются
  --ok N         квота успешных (по умолчанию 2000)
  --failed N     квота проблемных (по умолчанию 500)
  --heavy N      квота SPA-тяжёлых (по умолчанию 500)
  --smoke N      число smoke-доменов (по умолчанию 300)
  --weeks N      окно свежести проблемных, недели (по умолчанию 4)
  --seed N       seed детерминированной выборки (по умолчанию 242):
                 тот же seed и те же данные — те же строки дампа

import — идемпотентный накат дампа формата v1 в тестовую БД (§6.1 п. 3)
и публикация задач фикстуры прямо в q:render (§6.1 п. 4) — рендер-этап
тестируется изолированно, без DNS/HTTP. Накат — запись без отката
(перезаписывает зоны/домены/строки результатов по PK и выметает
contents): без --force — dry-run, печатаются цель (user@host:port/dbname
из DSN, без пароля — сверьте, что это стенд, а не прод) и полный план
(строки по таблицам вставка/обновление, дубли ключей, выметаемый
контент) без единой записи; --force — накат, цель и план печатаются
перед первой записью. Накат идёт по первичным ключам
из ключей дампа (повторный накат не дублирует, а восстанавливает
состояние дампа — прогоны сравниваются на одном старте); зоны входят
мимо identity (OVERRIDING SYSTEM VALUE), последовательности zones.id и
domain_id_seq выставляются выше максимума; контентные ссылки дампа
сбрасываются (contents не выгружался — DOM пишет сам стенд). БД должна
быть чистой (миграции дев-стека без данных): зона/домен с тем же
именем, но другим id — конфликт, накат останавливается до записи.

  --dir КАТАЛОГ   каталог дампа (обязателен; формат v1 — export)
  --force         выполнение наката (без флага — dry-run: цель и план
                  без записи; сверяйте цель — инструмент тестовой БД)
  --publish РЕЖИМ публикация задач фикстуры в q:render (только с
                  --force): all — все записи
                  дампа, smoke — домены smoke-подвыборки. Публикация
                  идёт в Redis конфигурации (redis.addr) — воркер стенда
                  должен читать тот же инстанс. Повторный запуск с
                  --publish добавляет записи
                  заново: между прогонами очищайте q:render (инструкция —
                  docs/dev-environment.md)

Дампа не хватает строк страты (мало данных) — берутся все доступные,
деградация отмечается в выводе и manifest.json. Формат дампа v1 —
контракт export→import, подробно: doc-комментарии cmd/crawlerctl/
fixtures.go и fixtures_import.go.

snapshot — фиксация результатов одного прогона (§6.1 п. 5) в JSON-снимок
вход отчёта: строки фикстуры из тестовой БД (успешные рендеры, длина/
хэш/узлы/скрипты DOM, вердикты анализатора), границы q:analyze из Redis
(темп) и — с --metrics — счётчики этапа render-worker (причины
финализаций, время страницы). Снимается ПОСЛЕ прогона (пустая q:render,
фолбэк доработан) и ДО повторного import (накат сбрасывает строки и
выметает контент — по БД задним числом прогон не прочесть). Между
прогонами перезапускайте render-worker: счётчики /metrics тогда равны
ровно прогону (иначе отчёт пометит их накопительными и деградирует
завязанные вердикты).

  --dir КАТАЛОГ        каталог дампа фикстуры (ключи прогона; обязателен)
  --label ИМЯ          метка прогона (обязательна; файл —
                       <dir>/<label>.snapshot.json)
  --engine ИМЯ         движок прогона: chromium|moli|lightpanda (обязателен)
  --metrics URL        URL /metrics render-worker стенда (пусто — без
                       метрик этапа; например http://render-worker:9105/metrics)
  --engine-rss-max N   макс. RSS контейнера движка за прогон, байты
                       (периодический docker stats --no-stream во время
                       прогона; 0 — не задан: в dev-стеке экспортёра
                       памяти движков нет, отчёт без него деградирует
                       по порогу памяти)
  --engine-crashes N   падения ядра движка за прогон (>= 0 — задано)
  --no-dom             не читать DOM хранилища (без узлов/скриптов)

report — сравнение снапшотов по метрикам ТЗ §5.1 п. 3 (успешность,
причины финализаций, темп ср./p95, память, диф детекции Laravel против
baseline-chromium по протоколу T-213, структурные метрики DOM —
нормализованное сравнение, побайтовый диф не применяется) с явными
строками вердикта против порогов §5.1 п. 4 (детекция <1%, успешность,
темп, память <⅓ бюджета chromium, 0 crashes). Отсутствующий источник —
строка «НЕТ ДАННЫХ», отчёт не выдумывает значения.

  --baseline ФАЙЛ    снапшот baseline-chromium (обязателен)
  --candidate ФАЙЛ   снапшот движка-кандидата (обязателен)
  --out ФАЙЛ         записать отчёт файлом (для протокола T-247)
  --memory-budget N  бюджет памяти chromium для порога ⅓ (по умолчанию
                     2 GiB — прод-бюджет рендер-контура, ТЗ 3.2)
  --fail-exit        код 1 при любом вердикте НЕ ПРОЙДЕН (для скриптов)

Подключение к БД — как у остальных команд: postgres.dsn конфигурации
(CRAWLER_POSTGRES_DSN / APP_PG_DSN). export читает БД без записи;
import пишет в тестовую БД и (с --publish) в Redis; snapshot читает БД,
Redis (redis.addr) и хранилище контента (content.dir) без записи;
report работает по файлам снапшотов без инфраструктуры.

Коды возврата: 0 — успех, 1 — ошибка, 2 — использование.
`

// Quota defaults — the spec §6.1 item 1 plan (3000 rows: ~2000/500/500)
// and smoke ~300 domains (item 6); the failed freshness window — the
// spec's "recent weeks". The seed is an arbitrary constant: a fixed
// value makes the default export reproducible (the same dump for the
// same data).
const (
	fixturesFormatVersion = 1
	fixturesDefaultOK     = 2000
	fixturesDefaultFailed = 500
	fixturesDefaultHeavy  = 500
	fixturesDefaultSmoke  = 300
	fixturesDefaultWeeks  = 4
	fixturesDefaultSeed   = 242
)

// ---------------------------------------------------------------- SQL

// fixturesResultCols — the subdomain_results selection columns: row
// identifiers, render task fields and stratum diagnostics (the format v1
// contract — the fixtures block doc comment). r.* — the alias of the ok
// stratum's anti-join.
const fixturesResultCols = "r.zone_id, r.domain_id, r.subdomain_id, r.variant, " +
	"r.scheme, r.http_status, r.final_url, r.alias_kind, r.alias_subdomain_id, " +
	"r.headers, r.cookies, r.rendered, r.content_hash, r.error, r.response_ms, " +
	"r.checked_at, r.rules_version, r.render_failed_at"

// fixturesKeyHash — a deterministic pseudo-random row order within a
// zone: md5 of the seed and the row key (the seed parameter number is
// substituted in place). Unlike ORDER BY random() it depends on neither
// the session nor the server version: re-exporting with the same --seed
// reproduces the selection. Keys are cast to text explicitly —
// concatenating numbers with text is ambiguous (12||34 = 1234); the ':'
// separator and ::text make the hash stable.
const fixturesKeyHash = "md5($%d::text || ':' || r.zone_id::text || ':' || r.domain_id::text || ':' || r.subdomain_id::text || ':' || r.variant::text)"

// Stratum row counts per zone (quotas proportional to shares): aggregates
// over the partitioned table are cut down to partitions. The base of all
// three — a "real q:render task" (final_url IS NOT NULL).
const (
	// fixturesCountOkSQL — rendered (a successful chromium render); it
	// also includes heavy candidates: ok availability is computed after
	// subtracting the taken heavy (heavy ⊂ rendered).
	fixturesCountOkSQL = "SELECT zone_id, count(*) FROM subdomain_results " +
		"WHERE final_url IS NOT NULL AND rendered GROUP BY zone_id"

	// fixturesCountFailedSQL — the T-210 marker within the --weeks
	// freshness window. Freshness is by render_failed_at (the stamp of
	// the FIRST finalization, coalesce T-210), not by the episode's
	// checked_at: an old episode record flushed out of the backlog is a
	// fresh marker (review T-242 P2); the spec's "recent weeks" is about
	// finalizations. make_interval — a named argument, without casting
	// strings to an interval.
	fixturesCountFailedSQL = "SELECT zone_id, count(*) FROM subdomain_results " +
		"WHERE final_url IS NOT NULL AND render_failed_at IS NOT NULL " +
		"AND render_failed_at >= now() - make_interval(weeks => $1::int) GROUP BY zone_id"

	// fixturesCountHeavySQL — rendered with live content: the DOM size is
	// known only for rows referencing contents (retention T-166 cleans
	// the content_hash of non-finds — a row without content is not
	// heavy).
	fixturesCountHeavySQL = "SELECT r.zone_id, count(*) FROM subdomain_results AS r " +
		"JOIN contents AS c ON c.hash = r.content_hash " +
		"WHERE r.final_url IS NOT NULL AND r.rendered GROUP BY r.zone_id"
)

// Stratum selection within one zone: LIMIT — the zone's quota (missing
// rows are degradation, not an error). heavy goes first (it feeds the ok
// anti-join).
var (
	fixturesSelectHeavySQL = "SELECT " + fixturesResultCols + " FROM subdomain_results AS r " +
		"JOIN contents AS c ON c.hash = r.content_hash " +
		"WHERE r.zone_id = $1::int2 AND r.final_url IS NOT NULL AND r.rendered " +
		"ORDER BY c.raw_len DESC, " + fmt.Sprintf(fixturesKeyHash, 2) + " LIMIT $3::int8"

	// fixturesSelectOkSQL — random successful rows: an anti-join against
	// the heavy keys (unnest arrays — the batch operation style of
	// internal/db) keeps the strata from intersecting — a row never
	// enters the dump twice. Empty arrays (no heavy) — the anti-join
	// lets everything through (unnest of 0 rows).
	fixturesSelectOkSQL = "SELECT " + fixturesResultCols + " FROM subdomain_results AS r " +
		"LEFT JOIN unnest($2::int2[], $3::int8[], $4::int2[], $5::int2[]) " +
		"AS ex(zone_id, domain_id, subdomain_id, variant) " +
		"ON r.zone_id = ex.zone_id AND r.domain_id = ex.domain_id " +
		"AND r.subdomain_id = ex.subdomain_id AND r.variant = ex.variant " +
		"WHERE r.zone_id = $1::int2 AND r.final_url IS NOT NULL AND r.rendered " +
		"AND ex.zone_id IS NULL " +
		"ORDER BY " + fmt.Sprintf(fixturesKeyHash, 6) + " LIMIT $7::int8"

	fixturesSelectFailedSQL = "SELECT " + fixturesResultCols + " FROM subdomain_results AS r " +
		"WHERE r.zone_id = $1::int2 AND r.final_url IS NOT NULL " +
		"AND r.render_failed_at IS NOT NULL " +
		"AND r.render_failed_at >= now() - make_interval(weeks => $2::int) " +
		"ORDER BY " + fmt.Sprintf(fixturesKeyHash, 3) + " LIMIT $4::int8"

	// fixturesDomainsSQL — domains of the selected rows (DISTINCT over
	// the unnest keys): whole table rows — domain identity and state for
	// the bench.
	fixturesDomainsSQL = "SELECT d.zone_id, d.id, d.label, d.status, d.check_state, " +
		"d.flags, d.dns_mask, d.queued_at, d.last_checked_at, d.temp_attempts " +
		"FROM domains AS d JOIN (SELECT DISTINCT k.zone_id, k.domain_id " +
		"FROM unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id)) AS k " +
		"ON d.zone_id = k.zone_id AND d.id = k.domain_id " +
		"ORDER BY d.zone_id, d.id"

	// fixturesZonesSQL — the export zones (id = ANY over an unnest-
	// compatible array): the reference table is small, a point select.
	fixturesZonesSQL = "SELECT id, name, name_unicode, is_active, domains_total, " +
		"domains_checked, updated_at FROM zones WHERE id = ANY($1::int2[]) ORDER BY id"
)

// ---------------------------------------------------------------- data

// fixturesZone — a zones row of the dump: the format v1 columns (see the
// contract — the live counters domains_laravel/pending/queued are not
// exported).
type fixturesZone struct {
	ID             int16      `json:"id"`
	Name           string     `json:"name"` // punycode
	NameUnicode    *string    `json:"name_unicode"`
	IsActive       bool       `json:"is_active"`
	DomainsTotal   int64      `json:"domains_total"`
	DomainsChecked int64      `json:"domains_checked"`
	UpdatedAt      *time.Time `json:"updated_at"`
}

// fixturesDomain — a domains row of the dump (all table columns): the
// row keys are original (zone_id, id), import idempotency is by PK.
type fixturesDomain struct {
	ZoneID        int16      `json:"zone_id"`
	ID            int64      `json:"id"`
	Label         string     `json:"label"` // without the zone, punycode
	Status        int16      `json:"status"`
	CheckState    int16      `json:"check_state"`
	Flags         int16      `json:"flags"`    // bit 0 is_idn, 1 render_pending, …
	DNSMask       int64      `json:"dns_mask"` // resolving subdomains
	QueuedAt      *time.Time `json:"queued_at"`
	LastCheckedAt *time.Time `json:"last_checked_at"`
	TempAttempts  int16      `json:"temp_attempts"`
}

// fixturesResult — a subdomain_results row of the dump: the columns the
// render task needs (the T-243 contract) plus stratum — the stratum
// label of the selection (ok|failed|heavy; a fixture service field that
// does not exist in the table — dropped on import). bytea content_hash →
// base64, NULL columns → null, jsonb headers/cookies → nested JSON.
type fixturesResult struct {
	ZoneID           int16           `json:"zone_id"`
	DomainID         int64           `json:"domain_id"`
	SubdomainID      int16           `json:"subdomain_id"`
	Variant          int16           `json:"variant"` // 0 main, 1 http
	Scheme           int16           `json:"scheme"`  // 1 http, 2 https
	HTTPStatus       *int16          `json:"http_status"`
	FinalURL         *string         `json:"final_url"` // the render target
	AliasKind        int16           `json:"alias_kind"`
	AliasSubdomainID *int16          `json:"alias_subdomain_id"`
	Headers          json.RawMessage `json:"headers"` // the episode's jsonb
	Cookies          json.RawMessage `json:"cookies"` // the episode's jsonb
	Rendered         bool            `json:"rendered"`
	ContentHash      []byte          `json:"content_hash"` // 16 bytes, base64
	Error            *int16          `json:"error"`
	ResponseMs       *int32          `json:"response_ms"`
	CheckedAt        time.Time       `json:"checked_at"`
	RulesVersion     *int32          `json:"rules_version"`
	RenderFailedAt   *time.Time      `json:"render_failed_at"`
	Stratum          string          `json:"stratum"`
}

// fixturesSmokeDomain — a smoke.ndjson row: the subsample domain key and
// the full name (label.zone) for task publishing and bench logs.
type fixturesSmokeDomain struct {
	ZoneID   int16  `json:"zone_id"`
	DomainID int64  `json:"domain_id"`
	Name     string `json:"name"`
}

// fixturesStratumStat — stratum counters in the manifest: the quota,
// available (the sum of the per-zone counts), actually taken, degradation
// (taken < quota) and the per-zone breakdown of what was taken.
type fixturesStratumStat struct {
	Quota     int            `json:"quota"`
	Available int            `json:"available"`
	Taken     int            `json:"taken"`
	Degraded  bool           `json:"degraded"`
	Zones     map[string]int `json:"zones"`
}

// fixturesManifest — the dump's manifest.json: the format version, the
// selection parameters and the counters. created_at — the run time (the
// only non-deterministic field); everything else is reproduced with the
// same --seed and data.
type fixturesManifest struct {
	FormatVersion int                            `json:"format_version"`
	CreatedAt     time.Time                      `json:"created_at"`
	Seed          int64                          `json:"seed"`
	Weeks         int                            `json:"failed_window_weeks"`
	Strata        map[string]fixturesStratumStat `json:"strata"`
	SmokeQuota    int                            `json:"smoke_quota"`
	SmokeDomains  int                            `json:"smoke_domains"`
	Files         map[string]int                 `json:"files"`
}

// fixturesKey — the subdomain_results primary key (migration 000002).
type fixturesKey struct {
	zone      int16
	domain    int64
	subdomain int16
	variant   int16
}

// fixturesCollect — the selection collector: duplicate keys (stratum
// intersections on bad data, outside the T-210 invariants) are dropped
// with a count — the dump stays a set of unique rows and re-import is
// idempotent.
type fixturesCollect struct {
	rows []fixturesResult
	seen map[fixturesKey]bool
	dups int
}

func newFixturesCollect() *fixturesCollect {
	return &fixturesCollect{seen: make(map[fixturesKey]bool)}
}

// add adds stratum rows (stratum — the label), cutting off duplicates.
func (c *fixturesCollect) add(stratum string, rows []fixturesResult) {
	for _, r := range rows {
		r.Stratum = stratum
		k := fixturesKey{r.ZoneID, r.DomainID, r.SubdomainID, r.Variant}
		if c.seen[k] {
			c.dups++
			continue
		}
		c.seen[k] = true
		c.rows = append(c.rows, r)
	}
}

// ---------------------------------------------------------------- quotas

// fixturesAllocateQuotas distributes total rows between zones
// proportionally to their shares (counts) by the largest remainder
// method: the integer parts of the shares plus a remainder top-up by
// descending fractional part; on a tie — by ascending zone_id
// (determinism). A zone's quota is never above what is available
// (total ≤ the sum); total above the sum or zero counts — quotas over
// what is available (degradation is resolved at selection time: LIMIT
// returns less). The sum of quotas equals min(total, the sum of counts);
// zero zones do not enter the result.
func fixturesAllocateQuotas(counts map[int16]int64, total int) map[int16]int {
	var sum int64
	for _, n := range counts {
		sum += n
	}
	if total <= 0 || sum == 0 {
		return nil
	}
	if int64(total) > sum {
		total = int(sum)
	}

	zones := make([]int16, 0, len(counts))
	for z := range counts {
		if counts[z] > 0 {
			zones = append(zones, z)
		}
	}
	sort.Slice(zones, func(i, j int) bool { return zones[i] < zones[j] })

	quotas := make(map[int16]int, len(zones))
	rest := total
	for _, z := range zones {
		exact := float64(total) * float64(counts[z]) / float64(sum)
		base := int64(exact)
		if base > counts[z] {
			base = counts[z]
		}
		quotas[z] = int(base)
		rest -= int(base)
	}
	if rest > 0 {
		type share struct {
			zone int16
			frac float64
		}
		shares := make([]share, 0, len(zones))
		for _, z := range zones {
			exact := float64(total) * float64(counts[z]) / float64(sum)
			shares = append(shares, share{zone: z, frac: exact - float64(int64(exact))})
		}
		sort.SliceStable(shares, func(i, j int) bool {
			if shares[i].frac != shares[j].frac {
				return shares[i].frac > shares[j].frac
			}
			return shares[i].zone < shares[j].zone // determinism on equal shares
		})
		for i := 0; i < len(shares) && rest > 0; i++ {
			if int64(quotas[shares[i].zone]) < counts[shares[i].zone] {
				quotas[shares[i].zone]++
				rest--
			}
		}
	}
	return quotas
}

// ---------------------------------------------------------------- selection

// fixturesZoneCounts — stratum row counters per zone (the aggregates of
// fixturesCount*SQL). Zones without rows do not enter the result.
func fixturesZoneCounts(ctx context.Context, pool db.DB, sql string, args ...any) (map[int16]int64, error) {
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("подсчёт страты по зонам: %w", err)
	}
	defer rows.Close()
	counts := make(map[int16]int64)
	for rows.Next() {
		var zone int16
		var n int64
		if err := rows.Scan(&zone, &n); err != nil {
			return nil, fmt.Errorf("подсчёт страты по зонам: чтение: %w", err)
		}
		counts[zone] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("подсчёт страты по зонам: %w", err)
	}
	return counts, nil
}

// fixturesScanResults reads stratum selection rows (the column order is
// fixturesResultCols).
func fixturesScanResults(rows pgx.Rows) ([]fixturesResult, error) {
	var out []fixturesResult
	for rows.Next() {
		var r fixturesResult
		if err := rows.Scan(&r.ZoneID, &r.DomainID, &r.SubdomainID, &r.Variant,
			&r.Scheme, &r.HTTPStatus, &r.FinalURL, &r.AliasKind, &r.AliasSubdomainID,
			&r.Headers, &r.Cookies, &r.Rendered, &r.ContentHash, &r.Error,
			&r.ResponseMs, &r.CheckedAt, &r.RulesVersion, &r.RenderFailedAt); err != nil {
			return nil, fmt.Errorf("чтение строк страты: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("чтение строк страты: %w", err)
	}
	return out, nil
}

// fixturesSortedZones — an ascending zone list (a deterministic traversal
// of the selection: partitions are addressed in turn; the order only
// affects the row order in the dump, but it is pinned too).
func fixturesSortedZones(quotas map[int16]int) []int16 {
	zones := make([]int16, 0, len(quotas))
	for z, q := range quotas {
		if q > 0 {
			zones = append(zones, z)
		}
	}
	sort.Slice(zones, func(i, j int) bool { return zones[i] < zones[j] })
	return zones
}

// fixturesPickHeavy — the heavy stratum per zone: the largest content
// (raw_len DESC), the zone's quota as LIMIT; the tie-break is a
// deterministic hash.
func fixturesPickHeavy(ctx context.Context, pool db.DB, quotas map[int16]int, seed string) ([]fixturesResult, error) {
	var out []fixturesResult
	for _, z := range fixturesSortedZones(quotas) {
		rows, err := pool.Query(ctx, fixturesSelectHeavySQL, z, seed, int64(quotas[z]))
		if err != nil {
			return nil, fmt.Errorf("выборка heavy (зона %d): %w", z, err)
		}
		part, err := fixturesScanResults(rows)
		rows.Close()
		if err != nil {
			return nil, fmt.Errorf("выборка heavy (зона %d): %w", z, err)
		}
		out = append(out, part...)
	}
	return out, nil
}

// fixturesPickOk — the ok stratum per zone: random successful rows with
// the heavy keys excluded by an anti-join (unnest arrays from the
// already read rows).
func fixturesPickOk(ctx context.Context, pool db.DB, quotas map[int16]int, seed string, heavy []fixturesResult) ([]fixturesResult, error) {
	hz, hd, hs, hv := fixturesKeyArrays(heavy)
	var out []fixturesResult
	for _, z := range fixturesSortedZones(quotas) {
		rows, err := pool.Query(ctx, fixturesSelectOkSQL,
			z, hz, hd, hs, hv, seed, int64(quotas[z]))
		if err != nil {
			return nil, fmt.Errorf("выборка ok (зона %d): %w", z, err)
		}
		part, err := fixturesScanResults(rows)
		rows.Close()
		if err != nil {
			return nil, fmt.Errorf("выборка ok (зона %d): %w", z, err)
		}
		out = append(out, part...)
	}
	return out, nil
}

// fixturesPickFailed — the failed stratum per zone: the T-210 marker in
// the weeks freshness window, a random order (a deterministic hash).
func fixturesPickFailed(ctx context.Context, pool db.DB, quotas map[int16]int, seed string, weeks int) ([]fixturesResult, error) {
	var out []fixturesResult
	for _, z := range fixturesSortedZones(quotas) {
		rows, err := pool.Query(ctx, fixturesSelectFailedSQL, z, weeks, seed, int64(quotas[z]))
		if err != nil {
			return nil, fmt.Errorf("выборка failed (зона %d): %w", z, err)
		}
		part, err := fixturesScanResults(rows)
		rows.Close()
		if err != nil {
			return nil, fmt.Errorf("выборка failed (зона %d): %w", z, err)
		}
		out = append(out, part...)
	}
	return out, nil
}

// fixturesKeyArrays — the key columns of the rows (parallel arrays for
// the unnest of the ok anti-join against heavy).
func fixturesKeyArrays(rows []fixturesResult) (zones []int16, domains []int64, subs []int16, variants []int16) {
	zones = make([]int16, len(rows))
	domains = make([]int64, len(rows))
	subs = make([]int16, len(rows))
	variants = make([]int16, len(rows))
	for i, r := range rows {
		zones[i], domains[i], subs[i], variants[i] = r.ZoneID, r.DomainID, r.SubdomainID, r.Variant
	}
	return zones, domains, subs, variants
}

// fixturesFetchDomains — domains of the selected rows by keys (DISTINCT
// inside the SQL), ordered by zone_id, id.
func fixturesFetchDomains(ctx context.Context, pool db.DB, rows []fixturesResult) ([]fixturesDomain, error) {
	zones, domains, _, _ := fixturesKeyArrays(rows)
	out := []fixturesDomain{} // not nil: an empty selection = an empty file, not an absence
	rrows, err := pool.Query(ctx, fixturesDomainsSQL, zones, domains)
	if err != nil {
		return nil, fmt.Errorf("выборка доменов фикстуры: %w", err)
	}
	defer rrows.Close()
	for rrows.Next() {
		var d fixturesDomain
		if err := rrows.Scan(&d.ZoneID, &d.ID, &d.Label, &d.Status, &d.CheckState,
			&d.Flags, &d.DNSMask, &d.QueuedAt, &d.LastCheckedAt, &d.TempAttempts); err != nil {
			return nil, fmt.Errorf("выборка доменов фикстуры: чтение: %w", err)
		}
		out = append(out, d)
	}
	if err := rrows.Err(); err != nil {
		return nil, fmt.Errorf("выборка доменов фикстуры: %w", err)
	}
	return out, nil
}

// fixturesFetchZones — the export zones by the domains' ids.
func fixturesFetchZones(ctx context.Context, pool db.DB, domains []fixturesDomain) ([]fixturesZone, error) {
	seen := make(map[int16]bool)
	ids := make([]int16, 0, len(domains))
	for _, d := range domains {
		if !seen[d.ZoneID] {
			seen[d.ZoneID] = true
			ids = append(ids, d.ZoneID)
		}
	}
	out := []fixturesZone{}
	rows, err := pool.Query(ctx, fixturesZonesSQL, ids)
	if err != nil {
		return nil, fmt.Errorf("выборка зон фикстуры: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var z fixturesZone
		if err := rows.Scan(&z.ID, &z.Name, &z.NameUnicode, &z.IsActive,
			&z.DomainsTotal, &z.DomainsChecked, &z.UpdatedAt); err != nil {
			return nil, fmt.Errorf("выборка зон фикстуры: чтение: %w", err)
		}
		out = append(out, z)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("выборка зон фикстуры: %w", err)
	}
	return out, nil
}

// ---------------------------------------------------------------- smoke

// fixturesSmokePick — the smoke domain subsample (§6.1 item 6): unique
// domains of the selection in a deterministic pseudo-random order — the
// same hash recipe as the SQL selection (md5(seed:key)), the first
// limit. name — the full name (label '.' zone, punycode) for publishing
// the bench's tasks.
func fixturesSmokePick(rows []fixturesResult, domains []fixturesDomain, zones []fixturesZone, seed string, limit int) []fixturesSmokeDomain {
	if limit <= 0 || len(rows) == 0 {
		return nil
	}
	labels := make(map[fixturesDomainKey]string, len(domains))
	for _, d := range domains {
		labels[fixturesDomainKey{d.ZoneID, d.ID}] = d.Label
	}
	zoneNames := make(map[int16]string, len(zones))
	for _, z := range zones {
		zoneNames[z.ID] = z.Name
	}

	// Unique domains of the selection (the appearance order does not
	// matter — the deterministic hash sort below).
	type hashed struct {
		key  fixturesDomainKey
		hash string
	}
	seen := make(map[fixturesDomainKey]bool)
	uniq := make([]hashed, 0, len(rows))
	for _, r := range rows {
		k := fixturesDomainKey{r.ZoneID, r.DomainID}
		if seen[k] {
			continue
		}
		seen[k] = true
		uniq = append(uniq, hashed{key: k, hash: fixturesDomainHash(seed, k)})
	}
	sort.Slice(uniq, func(i, j int) bool {
		if uniq[i].hash != uniq[j].hash {
			return uniq[i].hash < uniq[j].hash
		}
		if uniq[i].key.zone != uniq[j].key.zone {
			return uniq[i].key.zone < uniq[j].key.zone
		}
		return uniq[i].key.domain < uniq[j].key.domain
	})
	if limit > len(uniq) {
		limit = len(uniq)
	}
	out := make([]fixturesSmokeDomain, 0, limit)
	for _, h := range uniq[:limit] {
		name := labels[h.key] + "." + zoneNames[h.key.zone]
		out = append(out, fixturesSmokeDomain{ZoneID: h.key.zone, DomainID: h.key.domain, Name: name})
	}
	return out
}

// fixturesDomainKey — the domain key (the domains PK).
type fixturesDomainKey struct {
	zone   int16
	domain int64
}

// fixturesDomainHash — the SQL selection's hash recipe for a domain
// (md5 hex): a stable smoke order between runs with the same seed.
func fixturesDomainHash(seed string, k fixturesDomainKey) string {
	sum := md5.Sum([]byte(seed + ":" + strconv.Itoa(int(k.zone)) + ":" + strconv.FormatInt(k.domain, 10)))
	return hex.EncodeToString(sum[:])
}

// ---------------------------------------------------------------- writing

// fixturesEncodeAll encodes NDJSON rows: one object per line, without
// HTML escaping (the headers/cookies content is read as-is).
func fixturesEncodeAll[T any](w io.Writer, rows []T) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for i := range rows {
		if err := enc.Encode(&rows[i]); err != nil {
			return fmt.Errorf("строка %d: %w", i+1, err)
		}
	}
	return nil
}

// fixturesWriteFile — an atomic dump file write: the content goes to
// name+".tmp" and is renamed (an interrupted export leaves no half-files
// under the final names; a re-export overwrites). Mode 0600 — the dump
// carries episode cookies/headers (the spec explicitly asks to export
// them); reading them by another host user is not expected.
func fixturesWriteFile(dir, name string, write func(*bufio.Writer) error) error {
	full := filepath.Join(dir, name)
	tmp := full + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("%s: %w", tmp, err)
	}
	w := bufio.NewWriter(f)
	if err := write(w); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("%s: запись: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("%s: закрытие: %w", tmp, err)
	}
	if err := os.Rename(tmp, full); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("%s: переименование: %w", full, err)
	}
	return nil
}

// ---------------------------------------------------------------- export

// fixturesParams — the export parameters (the fixtures export flags; the
// defaults are the constants above). A zero stratum is excluded from the
// selection.
type fixturesParams struct {
	OutDir string
	OK     int
	Failed int
	Heavy  int
	Smoke  int
	Weeks  int
	Seed   int64
}

// fixturesExportRun — the fixture export: stratum counts per zone →
// quotas (proportional to shares) → the heavy selection → ok (the heavy
// anti-join) → failed → domains/zones → smoke → files (the manifest
// last).
func fixturesExportRun(ctx context.Context, pool db.DB, p fixturesParams, stdout io.Writer) (*fixturesManifest, error) {
	seedText := strconv.FormatInt(p.Seed, 10)

	// heavy: the content size is the priority, selected first.
	heavyCounts, err := fixturesZoneCounts(ctx, pool, fixturesCountHeavySQL)
	if err != nil {
		return nil, err
	}
	heavyRows, err := fixturesPickHeavy(ctx, pool, fixturesAllocateQuotas(heavyCounts, p.Heavy), seedText)
	if err != nil {
		return nil, err
	}

	// ok: rendered minus the taken heavy (heavy ⊂ rendered — the
	// availability is honestly reduced by their number in each zone).
	okCounts, err := fixturesZoneCounts(ctx, pool, fixturesCountOkSQL)
	if err != nil {
		return nil, err
	}
	heavyPerZone := make(map[int16]int64)
	for _, r := range heavyRows {
		heavyPerZone[r.ZoneID]++
	}
	for z, n := range heavyPerZone {
		okCounts[z] -= n
		if okCounts[z] < 0 {
			okCounts[z] = 0 // guard against count/selection desync by a write race
		}
	}
	okRows, err := fixturesPickOk(ctx, pool, fixturesAllocateQuotas(okCounts, p.OK), seedText, heavyRows)
	if err != nil {
		return nil, err
	}

	// failed: the T-210 marker in the freshness window.
	failedCounts, err := fixturesZoneCounts(ctx, pool, fixturesCountFailedSQL, p.Weeks)
	if err != nil {
		return nil, err
	}
	failedRows, err := fixturesPickFailed(ctx, pool, fixturesAllocateQuotas(failedCounts, p.Failed), seedText, p.Weeks)
	if err != nil {
		return nil, err
	}

	collect := newFixturesCollect()
	collect.add("heavy", heavyRows)
	collect.add("ok", okRows)
	collect.add("failed", failedRows)

	domains, err := fixturesFetchDomains(ctx, pool, collect.rows)
	if err != nil {
		return nil, err
	}
	zones, err := fixturesFetchZones(ctx, pool, domains)
	if err != nil {
		return nil, err
	}
	zoneNames := make(map[int16]string, len(zones))
	for _, z := range zones {
		zoneNames[z.ID] = z.Name
	}
	smoke := fixturesSmokePick(collect.rows, domains, zones, seedText, p.Smoke)

	// Manifest: stratum counters (quota/available/taken/degradation/zones).
	stat := func(quota, available int, rows []fixturesResult) fixturesStratumStat {
		perZone := make(map[string]int)
		for _, r := range rows {
			perZone[zoneNames[r.ZoneID]]++
		}
		return fixturesStratumStat{
			Quota:     quota,
			Available: available,
			Taken:     len(rows),
			Degraded:  len(rows) < quota,
			Zones:     perZone,
		}
	}
	sumCounts := func(counts map[int16]int64) int {
		total := 0
		for _, n := range counts {
			total += int(n)
		}
		return total
	}
	manifest := &fixturesManifest{
		FormatVersion: fixturesFormatVersion,
		CreatedAt:     time.Now().UTC(),
		Seed:          p.Seed,
		Weeks:         p.Weeks,
		Strata: map[string]fixturesStratumStat{
			"ok":     stat(p.OK, sumCounts(okCounts), okRows),
			"failed": stat(p.Failed, sumCounts(failedCounts), failedRows),
			"heavy":  stat(p.Heavy, sumCounts(heavyCounts), heavyRows),
		},
		SmokeQuota:   p.Smoke,
		SmokeDomains: len(smoke),
		Files:        make(map[string]int),
	}
	if collect.dups > 0 {
		// Duplicates outside the T-210 invariants: they did not enter the
		// dump, the operator sees the fact (the source data needs
		// analysis).
		out(stdout, "дубли ключей между стратами (пропущены): %d\n", collect.dups)
	}

	// The dump directory is private (0700): the files carry episode
	// cookies/headers.
	if err := os.MkdirAll(p.OutDir, 0o700); err != nil {
		return nil, fmt.Errorf("каталог дампа %s: %w", p.OutDir, err)
	}
	// The previous manifest.json goes before the first data file is
	// written: a re-export failing midway does NOT leave the old manifest
	// next to the new files — a dump without a manifest is honestly
	// "incomplete" (the "manifest present ⇒ dump complete" contract,
	// review T-242 P1). Until that point a failure (DB queries) does not
	// touch the old complete dump.
	if err := os.Remove(filepath.Join(p.OutDir, "manifest.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("удаление прежнего манифеста: %w", err)
	}
	// Order: manifest.json last — its presence means a completed dump.
	manifest.Files["zones.ndjson"] = len(zones)
	if err := fixturesWriteFile(p.OutDir, "zones.ndjson", func(w *bufio.Writer) error {
		return fixturesEncodeAll(w, zones)
	}); err != nil {
		return nil, err
	}
	manifest.Files["domains.ndjson"] = len(domains)
	if err := fixturesWriteFile(p.OutDir, "domains.ndjson", func(w *bufio.Writer) error {
		return fixturesEncodeAll(w, domains)
	}); err != nil {
		return nil, err
	}
	manifest.Files["subdomain_results.ndjson"] = len(collect.rows)
	if err := fixturesWriteFile(p.OutDir, "subdomain_results.ndjson", func(w *bufio.Writer) error {
		return fixturesEncodeAll(w, collect.rows)
	}); err != nil {
		return nil, err
	}
	manifest.Files["smoke.ndjson"] = len(smoke)
	if err := fixturesWriteFile(p.OutDir, "smoke.ndjson", func(w *bufio.Writer) error {
		return fixturesEncodeAll(w, smoke)
	}); err != nil {
		return nil, err
	}
	manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("манифест: %w", err)
	}
	manifestJSON = append(manifestJSON, '\n')
	if err := fixturesWriteFile(p.OutDir, "manifest.json", func(w *bufio.Writer) error {
		if _, err := w.Write(manifestJSON); err != nil {
			return fmt.Errorf("манифест: запись: %w", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Report: strata with honest degradation + the file summary.
	for _, name := range []string{"ok", "failed", "heavy"} {
		s := manifest.Strata[name]
		note := ""
		if s.Degraded {
			note = " — деградация: данных меньше квоты"
		}
		out(stdout, "страта %s: квота %d, доступно %d, взято %d%s\n",
			name, s.Quota, s.Available, s.Taken, note)
	}
	if p.Smoke > 0 && len(smoke) < p.Smoke {
		out(stdout, "smoke: доменов в выборке %d из %d — деградация: уникальных доменов меньше\n", len(smoke), p.Smoke)
	} else {
		out(stdout, "smoke: доменов %d\n", len(smoke))
	}
	for _, name := range []string{"zones.ndjson", "domains.ndjson", "subdomain_results.ndjson", "smoke.ndjson"} {
		out(stdout, "  %s: %d строк\n", name, manifest.Files[name])
	}
	out(stdout, "  manifest.json: ok\n")
	out(stdout, "дамп записан: %s (формат v1, накат — T-243)\n", p.OutDir)
	return manifest, nil
}

// ---------------------------------------------------------------- command

// cmdFixtures — crawlerctl fixtures: action routing (export — T-242;
// import — T-243, fixtures_import.go; snapshot/report — T-244,
// fixtures_snapshot.go / fixtures_report.go).
func cmdFixtures(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usagef("укажите действие: crawlerctl fixtures export|import|snapshot|report")
	}
	action, rest := args[0], args[1:]
	switch action {
	case "export":
		return fixturesExportCmd(rest, stdout, stderr)
	case "import":
		return fixturesImportCmd(rest, stdout, stderr)
	case "snapshot":
		return fixturesSnapshotCmd(rest, stdout, stderr)
	case "report":
		return fixturesReportCmd(rest, stdout, stderr)
	case "-h", "--help", "help":
		out(stdout, "%s", fixturesHelp)
		return flag.ErrHelp
	default:
		return usagef("неизвестное действие %q (допустимы: export, import, snapshot, report)", action)
	}
}

// fixturesExportCmd — crawlerctl fixtures export: flag parsing and
// validation (before the DB connection — code 2), then the export
// following the pattern of the management commands (openManagePool:
// configuration + environment, pool 2). A one-off CLI: Ctrl-C/SIGTERM
// interrupt the export — the already written files are atomic
// (.tmp + rename), rerunning the command overwrites the dump.
func fixturesExportCmd(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("fixtures export")
	outDir := fs.String("out", "", "каталог дампа (обязателен)")
	okN := fs.Int("ok", fixturesDefaultOK, "квота успешных (rendered)")
	failedN := fs.Int("failed", fixturesDefaultFailed, "квота проблемных (render_failed_at)")
	heavyN := fs.Int("heavy", fixturesDefaultHeavy, "квота SPA-тяжёлых (крупный контент)")
	smokeN := fs.Int("smoke", fixturesDefaultSmoke, "число smoke-доменов")
	weeks := fs.Int("weeks", fixturesDefaultWeeks, "окно свежести проблемных, недели")
	seed := fs.Int64("seed", fixturesDefaultSeed, "seed детерминированной выборки")
	if handled, err := parseFlags(fs, fixturesHelp, args, stdout); handled {
		return err
	}
	if strings.TrimSpace(*outDir) == "" {
		return usagef("укажите каталог дампа: crawlerctl fixtures export --out /path/to/fixtures")
	}
	if fs.NArg() > 0 {
		return usagef("fixtures export: лишние аргументы %v (команда принимает только флаги)", fs.Args())
	}
	for name, v := range map[string]int{"--ok": *okN, "--failed": *failedN, "--heavy": *heavyN, "--smoke": *smokeN} {
		if v < 0 {
			return usagef("fixtures export: %s: отрицательное значение %d", name, v)
		}
	}
	if *okN+*failedN+*heavyN == 0 {
		return usagef("fixtures export: сумма квот страт нулевая (--ok/--failed/--heavy)")
	}
	if *weeks < 1 {
		return usagef("fixtures export: --weeks: окно свежести не меньше 1 недели (получено %d)", *weeks)
	}

	ctx, stop := signalCtx()
	defer stop()

	pool, err := openManagePool(ctx)
	if err != nil {
		return fmt.Errorf("fixtures export: %w", err)
	}
	defer pool.Close()

	params := fixturesParams{
		OutDir: *outDir,
		OK:     *okN,
		Failed: *failedN,
		Heavy:  *heavyN,
		Smoke:  *smokeN,
		Weeks:  *weeks,
		Seed:   *seed,
	}
	if _, err := fixturesExportRun(ctx, pool, params, stdout); err != nil {
		return fmt.Errorf("fixtures export: %w", err)
	}
	return nil
}

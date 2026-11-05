// ---------------------------------------------------------------- fixtures
//
// crawlerctl fixtures (T-242, ТЗ задач выбора движка рендера
// tasks/TZ_render_engines.md §6.1 п. 1–2): выгрузка фикстуры доменов
// с боевой БД для локального стенда сравнения движков рендера (§5.1) и
// smoke-прогонов смены pinned-версий (§6.1 п. 6). Одноразовый инструмент
// этапа: выгрузка читает БД, накат дампа в тестовую БД — задача T-243
// (действие import добавит туда же); контент-хранилище не выгружается —
// DOM на стенде пишет сам рендер, baseline снимается там же.
//
// Выборка (§6.1 п. 1) — строки subdomain_results, являвшиеся реальными
// задачами q:render: базовый предикат final_url IS NOT NULL — критерий
// цели рендера выборки прохода доработки (renderPendingRowsSQL, T-057:
// SPA-вердикт пройден, запись эпизода HTTP публиковалась в q:render).
// Страты:
//
//	ok ~2000     rendered — успешный рендер chromium (baseline стенда);
//	failed ~500  render_failed_at IS NOT NULL последних --weeks недель —
//	             финализации без рендера: тарпиты T-148/таймауты/класс
//	             задачи T-172 (потенциальные клиенты фолбэка) И движко-
//	             независимые cert T-159/DNS T-211 (не клиенты, ФТ-7) —
//	             класс в строке не хранится, разделить нельзя (см.
//	             контракт формата ниже);
//	heavy ~500   rendered с самым крупным контентом (contents.raw_len —
//	             ориентир размера DOM) — SPA-тяжёлые.
//
// ok и heavy не пересекаются (heavy выбирается первым, ok исключает его
// ключи анти-join'ом), failed не пересекается с ними по инварианту
// маркера T-210 («render_failed_at установлен ⇒ rendered = false»);
// дубли ключей дополнительно отсекает коллектор выборки. Пропорции зон —
// квота зоны равна её доле в страте (метод наибольших остатков); внутри
// зоны строки упорядочены детерминированным псевдослучайным хэшем
// md5(seed:key) — повторная выгрузка при том же состоянии БД и том же
// --seed воспроизводит выборку (идемпотентность выгрузки). Доступных
// строк меньше квоты (дев-БД, свежий прод) — не ошибка: берутся все,
// деградация честно фиксируется в выводе и манифесте.
//
// Формат дампа (fixtures format v1) — каталог из пяти файлов:
//
//	zones.ndjson              строки справочника zones: колонки
//	                          id/name/name_unicode/is_active/
//	                          domains_total/domains_checked/updated_at;
//	                          live-счётчики прогресса зон (domains_laravel,
//	                          domains_pending, domains_queued) не
//	                          выгружаются — их пересчитает monitor стенда
//	                          (явный список колонок, а не «все»: формулировка
//	                          «все колонки» расходилась бы со схемой при
//	                          каждой миграции)
//	domains.ndjson            строки domains выбранных доменов (все
//	                          колонки — идентичность домена и состояние)
//	subdomain_results.ndjson  выбранные строки: идентификаторы, поля
//	                          задачи рендера (final_url — цель рендера,
//	                          cookies/headers — контекст эпизода, alias_*
//	                          — наследование вердикта T-055) и диагностика
//	                          страт (rendered, render_failed_at, error,
//	                          response_ms) + служебное поле stratum
//	smoke.ndjson              ключи smoke-подвыборки (zone_id, domain_id,
//	                          name) — домены сокращённого прогона §6.1 п. 6
//	manifest.json             метаданные: версия формата, seed, квоты и
//	                          фактические счётчики страт/зон, деградации
//
// NDJSON: один JSON-объект на строку, UTF-8, без экранирования HTML.
// Идентификаторы строк сохраняются оригинальными — повторный накат
// идемпотентен по первичным ключам (INSERT … ON CONFLICT — накат T-243).
// Значения: bytea → base64 (content_hash — 24 символа); timestamptz и
// date → RFC 3339; jsonb (headers/cookies) → вложенный JSON как есть;
// NULL любой колонки → null. Колонки addrs/cname/tls/redirects не
// выгружаются: задаче рендера не нужны, обезличивание по §6.1 п. 2
// (final_url публичен по определению задачи, cookies/headers выгружает
// ТЗ явно).
//
// Страта failed — все финализации без рендера по маркеру T-210: тарпит
// T-148, класс задачи T-172, cert T-159, DNS T-211. Класс финализации
// в строке subdomain_results не хранится (колонка error — про HTTP-фазу),
// отделить движко-независимые cert/DNS (не клиентов фолбэка, ФТ-7) без
// изменения схемы нельзя — их доля видна на стенде по причинам
// финализаций (словарь taskFinalizeReason, §4.6); фиксация для
// интерпретации результатов стенда.
//
// Каждый файл пишется во временное имя и переименовывается после
// успешного закрытия; manifest.json появляется последним — его наличие
// означает завершённый дамп (прерванная выгрузка не оставляет
// полуфайлов под финальными именами). Прежний manifest.json удаляется до
// записи первого файла данных повторной выгрузки — упавший посреди
// записи прогон не оставляет старый манифест рядом с новыми файлами.
//
// Идемпотентность повторной выгрузки: детерминированы СОСТАВ ВЫБОРКИ и
// поля строк subdomain_results/smoke (при неизменных данных и том же
// --seed — побайтово). Строки domains/zones — снимок состояния на
// момент выгрузки: живые поля (domains.status/check_state/flags/
// dns_mask/queued_at/last_checked_at, счётчики и updated_at зон,
// created_at манифеста) между прогонами меняются вместе с БД; накату
// T-243 это не мешает (ключи строк стабильны), счётчики зон пересчитает
// monitor стенда.
//
// Накат T-243 (заметки схемы): zones.id — GENERATED ALWAYS AS IDENTITY
// (вставка оригинальных id — OVERRIDING SYSTEM VALUE), domains.id —
// nextval('domain_id_seq') (после наката — setval выше максимального
// выгруженного id, иначе новые домены стенда начнутся с 1 и столкнутся
// с фикстурой).

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

// fixturesHelp — справка crawlerctl fixtures [--help]; она же печатается
// для действий fixtures <действие> --help.
const fixturesHelp = `crawlerctl fixtures — фикстуры стенда сравнения движков рендера (T-242)

Использование:
  crawlerctl fixtures export --out КАТАЛОГ [--ok N] [--failed N]
                              [--heavy N] [--smoke N] [--weeks N]
                              [--seed N]

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

Дампа не хватает строк страты (мало данных) — берутся все доступные,
деградация отмечается в выводе и manifest.json. Формат дампа v1 —
контракт наката (T-243), подробно: doc-комментарий cmd/crawlerctl/fixtures.go.

Подключение к БД — как у остальных команд: postgres.dsn конфигурации
(CRAWLER_POSTGRES_DSN / APP_PG_DSN). Боевая БД читается без записи.

Коды возврата: 0 — успех, 1 — ошибка, 2 — использование.
`

// Дефолты квот — план ТЗ §6.1 п. 1 (3000 строк: ~2000/500/500) и smoke
// ~300 доменов (п. 6); окно свежести failed — «последних недель» ТЗ.
// Seed — произвольная константа: фиксированное значение делает выгрузку
// по умолчанию воспроизводимой (тот же дамп при тех же данных).
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

// fixturesResultCols — колонки выборки subdomain_results: идентификаторы
// строки, поля задачи рендера и диагностика страт (контракт формата v1 —
// докомментарий блока fixtures). r.* — алиас анти-join'а страты ok.
const fixturesResultCols = "r.zone_id, r.domain_id, r.subdomain_id, r.variant, " +
	"r.scheme, r.http_status, r.final_url, r.alias_kind, r.alias_subdomain_id, " +
	"r.headers, r.cookies, r.rendered, r.content_hash, r.error, r.response_ms, " +
	"r.checked_at, r.rules_version, r.render_failed_at"

// fixturesKeyHash — детерминированный псевдослучайный порядок строк зоны:
// md5 от seed и ключа строки (номер параметра seed подставляется по месту).
// В отличие от ORDER BY random() не зависит от сессии и версии сервера:
// повторная выгрузка с тем же --seed воспроизводит выборку. Ключи
// приводятся к тексту явно — конкатенация чисел с text неоднозначна
// (12||34 = 1234), разделитель ':' и ::text делают хэш устойчивым.
const fixturesKeyHash = "md5($%d::text || ':' || r.zone_id::text || ':' || r.domain_id::text || ':' || r.subdomain_id::text || ':' || r.variant::text)"

// Подсчёт строк страт по зонам (квоты пропорциональны долям): агрегаты
// по zone_id партиционированной таблицы режутся до партиций. База всех
// трёх — «реальная задача q:render» (final_url IS NOT NULL).
const (
	// fixturesCountOkSQL — rendered (успешный рендер chromium); включает
	// и heavy-кандидатов: доступность ok считается после вычета взятых
	// heavy (heavy ⊂ rendered).
	fixturesCountOkSQL = "SELECT zone_id, count(*) FROM subdomain_results " +
		"WHERE final_url IS NOT NULL AND rendered GROUP BY zone_id"

	// fixturesCountFailedSQL — маркер T-210 в окне свежести --weeks.
	// Свежесть — по render_failed_at (штамп ПЕРВОЙ финализации, coalesce
	// T-210), не по checked_at эпизода: запись старого эпизода, дотянутая
	// из backlog'а, — свежий маркер (ревью T-242 P2); «последних недель»
	// ТЗ — про финализации. make_interval — именованный аргумент, без
	// приведения строк к интервалу.
	fixturesCountFailedSQL = "SELECT zone_id, count(*) FROM subdomain_results " +
		"WHERE final_url IS NOT NULL AND render_failed_at IS NOT NULL " +
		"AND render_failed_at >= now() - make_interval(weeks => $1::int) GROUP BY zone_id"

	// fixturesCountHeavySQL — rendered с живым контентом: размер DOM
	// известен только у строк со ссылкой на contents (retention T-166
	// чистит content_hash не-находок — без контента строка не heavy).
	fixturesCountHeavySQL = "SELECT r.zone_id, count(*) FROM subdomain_results AS r " +
		"JOIN contents AS c ON c.hash = r.content_hash " +
		"WHERE r.final_url IS NOT NULL AND r.rendered GROUP BY r.zone_id"
)

// Выборка страты в одной зоне: LIMIT — квота зоны (недостающие строки —
// деградация, не ошибка). heavy идёт первой (кормит анти-join ok).
var (
	fixturesSelectHeavySQL = "SELECT " + fixturesResultCols + " FROM subdomain_results AS r " +
		"JOIN contents AS c ON c.hash = r.content_hash " +
		"WHERE r.zone_id = $1::int2 AND r.final_url IS NOT NULL AND r.rendered " +
		"ORDER BY c.raw_len DESC, " + fmt.Sprintf(fixturesKeyHash, 2) + " LIMIT $3::int8"

	// fixturesSelectOkSQL — случайные успешные: анти-join против ключей
	// heavy (unnest-массивы — стиль пакетных операций internal/db) не даёт
	// стратам пересечься — строка не попадает в дамп дважды. Пустые
	// массивы (heavy пуст) — анти-join пропускает всё (unnest 0 строк).
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

	// fixturesDomainsSQL — домены выбранных строк (DISTINCT по unnest-ключам):
	// строки таблицы целиком — идентичность домена и состояние для стенда.
	fixturesDomainsSQL = "SELECT d.zone_id, d.id, d.label, d.status, d.check_state, " +
		"d.flags, d.dns_mask, d.queued_at, d.last_checked_at, d.temp_attempts " +
		"FROM domains AS d JOIN (SELECT DISTINCT k.zone_id, k.domain_id " +
		"FROM unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id)) AS k " +
		"ON d.zone_id = k.zone_id AND d.id = k.domain_id " +
		"ORDER BY d.zone_id, d.id"

	// fixturesZonesSQL — зоны выгрузки (id = ANY по unnest-совместимому
	// массиву): справочник мал, точечный выбор.
	fixturesZonesSQL = "SELECT id, name, name_unicode, is_active, domains_total, " +
		"domains_checked, updated_at FROM zones WHERE id = ANY($1::int2[]) ORDER BY id"
)

// ---------------------------------------------------------------- данные

// fixturesZone — строка zones дампа: колонки формата v1 (см. контракт —
// live-счётчики domains_laravel/pending/queued не выгружаются).
type fixturesZone struct {
	ID             int16      `json:"id"`
	Name           string     `json:"name"` // punycode
	NameUnicode    *string    `json:"name_unicode"`
	IsActive       bool       `json:"is_active"`
	DomainsTotal   int64      `json:"domains_total"`
	DomainsChecked int64      `json:"domains_checked"`
	UpdatedAt      *time.Time `json:"updated_at"`
}

// fixturesDomain — строка domains дампа (все колонки таблицы): ключи
// строки — оригинальные (zone_id, id), идемпотентность наката — по PK.
type fixturesDomain struct {
	ZoneID        int16      `json:"zone_id"`
	ID            int64      `json:"id"`
	Label         string     `json:"label"` // без зоны, punycode
	Status        int16      `json:"status"`
	CheckState    int16      `json:"check_state"`
	Flags         int16      `json:"flags"`    // бит 0 is_idn, 1 render_pending, …
	DNSMask       int64      `json:"dns_mask"` // резолвящиеся субдомены
	QueuedAt      *time.Time `json:"queued_at"`
	LastCheckedAt *time.Time `json:"last_checked_at"`
	TempAttempts  int16      `json:"temp_attempts"`
}

// fixturesResult — строка subdomain_results дампа: колонки, необходимые
// задаче рендера (контракт T-243), плюс stratum — метка страты выборки
// (ok|failed|heavy; служебное поле фикстуры, в таблице его нет — при
// накате отбрасывается). bytea content_hash → base64, NULL-колонки →
// null, jsonb headers/cookies → вложенный JSON.
type fixturesResult struct {
	ZoneID           int16           `json:"zone_id"`
	DomainID         int64           `json:"domain_id"`
	SubdomainID      int16           `json:"subdomain_id"`
	Variant          int16           `json:"variant"` // 0 основной, 1 http
	Scheme           int16           `json:"scheme"`  // 1 http, 2 https
	HTTPStatus       *int16          `json:"http_status"`
	FinalURL         *string         `json:"final_url"` // цель рендера
	AliasKind        int16           `json:"alias_kind"`
	AliasSubdomainID *int16          `json:"alias_subdomain_id"`
	Headers          json.RawMessage `json:"headers"` // jsonb эпизода
	Cookies          json.RawMessage `json:"cookies"` // jsonb эпизода
	Rendered         bool            `json:"rendered"`
	ContentHash      []byte          `json:"content_hash"` // 16 байт, base64
	Error            *int16          `json:"error"`
	ResponseMs       *int32          `json:"response_ms"`
	CheckedAt        time.Time       `json:"checked_at"`
	RulesVersion     *int32          `json:"rules_version"`
	RenderFailedAt   *time.Time      `json:"render_failed_at"`
	Stratum          string          `json:"stratum"`
}

// fixturesSmokeDomain — строка smoke.ndjson: ключ домена подвыборки и
// полное имя (label.zone) для публикации задач и логов стенда.
type fixturesSmokeDomain struct {
	ZoneID   int16  `json:"zone_id"`
	DomainID int64  `json:"domain_id"`
	Name     string `json:"name"`
}

// fixturesStratumStat — счётчики страты в манифесте: квота, доступно
// (сумма подсчёта по зонам), взято фактически, деградация (взято < квоты)
// и разбивка взятия по зонам.
type fixturesStratumStat struct {
	Quota     int            `json:"quota"`
	Available int            `json:"available"`
	Taken     int            `json:"taken"`
	Degraded  bool           `json:"degraded"`
	Zones     map[string]int `json:"zones"`
}

// fixturesManifest — manifest.json дампа: версия формата, параметры
// выборки и счётчики. created_at — время прогона (единственное недетерми-
// нированное поле); остальное при том же --seed и данных воспроизводится.
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

// fixturesKey — первичный ключ subdomain_results (миграция 000002).
type fixturesKey struct {
	zone      int16
	domain    int64
	subdomain int16
	variant   int16
}

// fixturesCollect — коллектор выборки: дубли ключей (пересечение страт на
// битых данных, вне инвариантов T-210) отбрасываются с подсчётом — дамп
// остаётся множеством уникальных строк, повторный накат идемпотентен.
type fixturesCollect struct {
	rows []fixturesResult
	seen map[fixturesKey]bool
	dups int
}

func newFixturesCollect() *fixturesCollect {
	return &fixturesCollect{seen: make(map[fixturesKey]bool)}
}

// add добавляет строки страты (stratum — метка) с отсечением дублей.
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

// ---------------------------------------------------------------- квоты

// fixturesAllocateQuotas распределяет total строк между зонами
// пропорционально их долям (counts) методом наибольших остатков: целые
// части долей plus добор остатка по убыванию дробной части, при равенстве
// — по возрастанию zone_id (детерминированность). Квота зоны не выше
// доступного (total ≤ суммы); total больше суммы или нулевые counts —
// квоты по доступному (деградация решается на выборке: LIMIT вернёт
// меньше). Сумма квот равна min(total, сумма counts), нулевые зоны
// в результат не входят.
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
			return shares[i].zone < shares[j].zone // детерминизм при равных долях
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

// ---------------------------------------------------------------- выборка

// fixturesZoneCounts — счётчики строк страты по зонам (агрегаты
// fixturesCount*SQL). Зоны без строк в результат не входят.
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

// fixturesScanResults читает строки выборки страты (порядок колонок —
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

// fixturesSortedZones — возрастающий список зон (детерминированный обход
// выборки: партиции адресуются по очереди, порядок влияет только на
// порядок строк в дампе, но фиксируется и он).
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

// fixturesPickHeavy — топheavy-страта по зонам: крупнейший контент
// (raw_len DESC), квота зоны — LIMIT; tie-break — детерминированный хэш.
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

// fixturesPickOk — страта ok по зонам: случайные успешные, ключи heavy
// исключены анти-join'ом (unnest-массивы из уже прочитанных строк).
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

// fixturesPickFailed — страта failed по зонам: маркер T-210 в окне
// свежести weeks, случайный порядок (детерминированный хэш).
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

// fixturesKeyArrays — колонки ключей строк (параллельные массивы для
// unnest анти-join'а ok против heavy).
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

// fixturesFetchDomains — домены выбранных строк по ключам (DISTINCT
// внутри SQL), порядок zone_id, id.
func fixturesFetchDomains(ctx context.Context, pool db.DB, rows []fixturesResult) ([]fixturesDomain, error) {
	zones, domains, _, _ := fixturesKeyArrays(rows)
	out := []fixturesDomain{} // не nil: пустая выборка = пустой файл, не отсутствие
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

// fixturesFetchZones — зоны выгрузки по id доменов.
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

// fixturesSmokePick — smoke-подвыборка доменов (§6.1 п. 6): уникальные
// домены выборки, детерминированный псевдослучайный порядок — тот же
// хэш-рецепт, что у SQL-выборки (md5(seed:key)), первые limit. name —
// полное имя (label '.' зона, punycode) для публикации задач стенда.
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

	// Уникальные домены выборки (порядок появления не важен — ниже
	// детерминированная сортировка хэшем).
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

// fixturesDomainKey — ключ домена (PK domains).
type fixturesDomainKey struct {
	zone   int16
	domain int64
}

// fixturesDomainHash — хэш-рецепт SQL-выборки для домена (md5 hex):
// стабильный порядок smoke между прогонами с тем же seed.
func fixturesDomainHash(seed string, k fixturesDomainKey) string {
	sum := md5.Sum([]byte(seed + ":" + strconv.Itoa(int(k.zone)) + ":" + strconv.FormatInt(k.domain, 10)))
	return hex.EncodeToString(sum[:])
}

// ---------------------------------------------------------------- запись

// fixturesEncodeAll кодирует строки NDJSON: по объекту на строку,
// без экранирования HTML (контент заголовков/cookies читается как есть).
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

// fixturesWriteFile — атомарная запись файла дампа: содержимое пишется в
// name+".tmp" и переименовывается (прерванная выгрузка не оставляет
// полуфайлов под финальными именами; повторная выгрузка перезаписывает).
// Режим 0600 — дамп несёт cookies/headers эпизодов (ТЗ выгружает их
// явно), чтение чужим пользователем хоста не предполагается.
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

// ---------------------------------------------------------------- выгрузка

// fixturesParams — параметры выгрузки (флаги fixtures export; дефолты —
// константы выше). Ноль страты исключает её из выборки.
type fixturesParams struct {
	OutDir string
	OK     int
	Failed int
	Heavy  int
	Smoke  int
	Weeks  int
	Seed   int64
}

// fixturesExportRun — выгрузка фикстуры: подсчёт страт по зонам → квоты
// (пропорционально долям) → выборка heavy → ok (анти-join heavy) →
// failed → домены/зоны → smoke → файлы (manifest последним).
func fixturesExportRun(ctx context.Context, pool db.DB, p fixturesParams, stdout io.Writer) (*fixturesManifest, error) {
	seedText := strconv.FormatInt(p.Seed, 10)

	// heavy: размер контента — приоритет, выбирается первой.
	heavyCounts, err := fixturesZoneCounts(ctx, pool, fixturesCountHeavySQL)
	if err != nil {
		return nil, err
	}
	heavyRows, err := fixturesPickHeavy(ctx, pool, fixturesAllocateQuotas(heavyCounts, p.Heavy), seedText)
	if err != nil {
		return nil, err
	}

	// ok: rendered минус взятые heavy (heavy ⊂ rendered — доступность
	// честно уменьшается на их число в каждой зоне).
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
			okCounts[z] = 0 // защита от рассинхрона счёта и выборки гонкой записи
		}
	}
	okRows, err := fixturesPickOk(ctx, pool, fixturesAllocateQuotas(okCounts, p.OK), seedText, heavyRows)
	if err != nil {
		return nil, err
	}

	// failed: маркер T-210 в окне свежести.
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

	// Манифест: счётчики страт (квота/доступно/взято/деградация/зоны).
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
		// Дубли вне инвариантов T-210: в дамп не попали, факт виден
		// оператору (данные источника требуют разбора).
		out(stdout, "дубли ключей между стратами (пропущены): %d\n", collect.dups)
	}

	// Каталог дампа приватен (0700): файлы несут cookies/headers эпизодов.
	if err := os.MkdirAll(p.OutDir, 0o700); err != nil {
		return nil, fmt.Errorf("каталог дампа %s: %w", p.OutDir, err)
	}
	// Прежний manifest.json уходит до записи первого файла данных: падение
	// повторной выгрузки на середине НЕ оставляет старый манифест рядом
	// с новыми файлами — дамп без манифеста честно «незавершён» (контракт
	// «manifest есть ⇒ дамп завершён», ревью T-242 P1). До этого момента
	// падение (запросы к БД) старый полный дамп не трогает.
	if err := os.Remove(filepath.Join(p.OutDir, "manifest.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("удаление прежнего манифеста: %w", err)
	}
	// Порядок: manifest.json последним — его наличие означает завершённый дамп.
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

	// Отчёт: страты с честной деградацией + сводка файлов.
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

// ---------------------------------------------------------------- команда

// cmdFixtures — crawlerctl fixtures: маршрутизация действий (export —
// T-242; import/накат — T-243 добавит сюда же).
func cmdFixtures(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usagef("укажите действие: crawlerctl fixtures export")
	}
	action, rest := args[0], args[1:]
	switch action {
	case "export":
		return fixturesExportCmd(rest, stdout, stderr)
	case "-h", "--help", "help":
		out(stdout, "%s", fixturesHelp)
		return flag.ErrHelp
	default:
		return usagef("неизвестное действие %q (допустимы: export)", action)
	}
}

// fixturesExportCmd — crawlerctl fixtures export: разбор флагов и
// валидация (до соединения с БД — код 2), затем выгрузка по образцу
// команд управления (openManagePool: конфигурация + окружение, пул 2).
// Одноразовый CLI: Ctrl-C/SIGTERM прерывают выгрузку — уже записанные
// файлы атомарны (.tmp + rename), повтор команды перезаписывает дамп.
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

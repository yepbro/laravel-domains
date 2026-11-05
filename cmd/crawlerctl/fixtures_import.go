// ---------------------------------------------------------------- накат
//
// crawlerctl fixtures import (T-243, ТЗ задач выбора движка рендера
// tasks/TZ_render_engines.md §6.1 п. 3–4): идемпотентный накат дампа
// формата v1 (fixtures export, T-242) в тестовую БД стенда и публикация
// задач фикстуры прямо в q:render — рендер-этап тестируется изолированно,
// без DNS/HTTP. Тестовая БД изолирована от прода (§6.1 п. 3): миграции
// дев-стека применяются отдельно, сюда — только строки дампа.
//
// Идемпотентность наката — по первичным ключам из ключей дампа (строки
// T-242 сохраняют оригинальные идентификаторы): INSERT … ON CONFLICT (PK)
// DO UPDATE — повторный накат не дублирует строки, а восстанавливает
// состояние дампа (прогон воркера меняет строки — rendered, ссылки на
// контент, маркеры финализации; повторный накат возвращает фикстуру к
// исходному виду, два прогона сравниваются на одном старте). Единственное
// сознательное отличие от дампа: content_hash сбрасывается в NULL —
// контент-хранилище не выгружалось (T-242), продовая ссылка на стенде
// висячая и ломала бы запись рендера (SwapRefTx, ErrRefIntegrity); DOM
// на стенде пишет сам рендер. Последовательности — по заметкам контракта
// T-242 (докомментарий fixtures.go): zones.id — GENERATED ALWAYS AS
// IDENTITY, оригинальные id входят через OVERRIDING SYSTEM VALUE;
// domains.id — nextval('domain_id_seq'); после наката обе
// последовательности выставляются setval'ем выше максимума (GREATEST из
// максимума таблицы и дампа) — новые зоны/домены стенда не начинаются с 1
// и не сталкиваются с фикстурой.
//
// Естественные ключи (zones.name, domains (zone_id, label)) не входят в
// конфликтный набор: накат по PK предполагает чистую тестовую БД (миграции
// без данных, §6.1 п. 3); зона/домен с тем же именем, но другим id —
// конфликт UNIQUE, который проверяется до записи понятной ошибкой (не
// внутренностями pgcode 23505 от INSERT).
//
// Публикация (--publish): по одному сообщению queue.Result на строку дампа
// (тот же контракт, что публикуют HTTP-этап и проход доработки в q:render,
// renderq.go; Fresh не ставится — его задаёт сам воркер при публикации
// q:analyze, worker.go) в стрим q:render конфигурации стенда. Режим all —
// все записи дампа, smoke — только домены smoke-подвыборки (§6.1 п. 6).
// Дубли ключей внутри выборки отсекаются с подсчётом (дамп T-242 —
// множество, защита от собранных вручную); повторный запуск import с
// --publish добавляет записи в стрим заново — между прогонами стрим
// очищается (инструкция — docs/dev-environment.md).

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// fixturesImportBatch — строк дампа в одном INSERT (unnest-массивы): все
// колонки таблиц — один параметр-массив на колонку (стиль recordInsertSQL,
// T-058); 500 строк × ≤17 колонок не упирается в лимит протокола
// (unsigned short параметров), дубль пакетных проходов — как у drain-циклов
// (subdomainRecheckBatch).
const fixturesImportBatch = 500

// Режимы публикации --publish (двузначный флаг: пусто — не публиковать).
const (
	fixturesPublishAll   = "all"   // все записи дампа (полный прогон §5.1)
	fixturesPublishSmoke = "smoke" // домены smoke-подвыборки (§6.1 п. 6)
)

// ---------------------------------------------------------------- SQL

// fixturesZoneByNameSQL — естественный ключ зон для предпроверки наката:
// зона с тем же name, но другим id (конфликт UNIQUE) ловится до INSERT
// понятной ошибкой. ANY по массиву имён дампа.
const fixturesZoneByNameSQL = "SELECT id, name FROM zones WHERE name = ANY($1::text[])"

// fixturesDomainByLabelSQL — то же для доменов: (zone_id, label) из
// unnest-ключей дампа (стиль пакетных операций internal/db).
const fixturesDomainByLabelSQL = "SELECT d.zone_id, d.label, d.id FROM domains AS d " +
	"JOIN unnest($1::int2[], $2::text[]) AS k(zone_id, label) " +
	"ON d.zone_id = k.zone_id AND d.label = k.label"

// fixturesImportZonesSQL — накат зон по PK: оригинальные id входят мимо
// identity (OVERRIDING SYSTEM VALUE, контракт T-242), конфликт PK —
// обновление до состояния дампа (идемпотентность повторного наката).
const fixturesImportZonesSQL = "INSERT INTO zones " +
	"(id, name, name_unicode, is_active, domains_total, domains_checked, updated_at) " +
	"OVERRIDING SYSTEM VALUE " +
	"SELECT z.id, z.name, z.name_unicode, z.is_active, z.domains_total, " +
	"z.domains_checked, z.updated_at FROM unnest(" +
	"$1::int2[], $2::text[], $3::text[], $4::bool[], $5::int8[], $6::int8[], $7::timestamptz[]) " +
	"AS z(id, name, name_unicode, is_active, domains_total, domains_checked, updated_at) " +
	"ON CONFLICT (id) DO UPDATE SET " +
	"name = EXCLUDED.name, name_unicode = EXCLUDED.name_unicode, " +
	"is_active = EXCLUDED.is_active, domains_total = EXCLUDED.domains_total, " +
	"domains_checked = EXCLUDED.domains_checked, updated_at = EXCLUDED.updated_at"

// fixturesImportDomainsSQL — накат доменов по PK (zone_id, id); метка и
// состояние — из дампа (UNIQUE (zone_id, label) закрыт предпроверкой).
// last_checked_at приводится к date из timestamptz со сдвигом в UTC
// (AT TIME ZONE 'UTC'): прямое приведение брало бы дату в TimeZone
// сессии — при отрицательном смещении (стенд вне UTC) дата уезжала бы
// на день назад.
const fixturesImportDomainsSQL = "INSERT INTO domains " +
	"(zone_id, id, label, status, check_state, flags, dns_mask, queued_at, last_checked_at, temp_attempts) " +
	"SELECT d.zone_id, d.id, d.label, d.status, d.check_state, d.flags, d.dns_mask, " +
	"d.queued_at, (d.last_checked_at AT TIME ZONE 'UTC')::date, d.temp_attempts FROM unnest(" +
	"$1::int2[], $2::int8[], $3::text[], $4::int2[], $5::int2[], $6::int2[], " +
	"$7::int8[], $8::timestamptz[], $9::timestamptz[], $10::int2[]) " +
	"AS d(zone_id, id, label, status, check_state, flags, dns_mask, " +
	"queued_at, last_checked_at, temp_attempts) " +
	"ON CONFLICT (zone_id, id) DO UPDATE SET " +
	"label = EXCLUDED.label, status = EXCLUDED.status, " +
	"check_state = EXCLUDED.check_state, flags = EXCLUDED.flags, " +
	"dns_mask = EXCLUDED.dns_mask, queued_at = EXCLUDED.queued_at, " +
	"last_checked_at = EXCLUDED.last_checked_at, temp_attempts = EXCLUDED.temp_attempts"

// fixturesImportResultsSQL — накат строк subdomain_results по PK
// (zone_id, domain_id, subdomain_id, variant): колонки — контракт формата
// v1 (fixturesResultCols, T-242) + render_failed_at (страта failed);
// колонок БД вне дампа (addrs/cname/tls/redirects) накат не касается —
// они не выгружались (обезличивание §6.1 п. 2) и задаче рендера не нужны.
// content_hash намеренно НЕ накатывается (и сбрасывается в NULL
// повторным накатом): контент-хранилище не выгружалось (T-242, «DOM
// пишет сам рендер стенда»), продовая ссылка на стенде висит в воздухе
// и валит запись результата рендера — SwapRefTx снимает ссылку старого
// контента и падает ErrRefIntegrity на отсутствующей строке contents
// (internal/content/refcount.go); NULL — валидное «контента нет»
// (hasOld=false, воркер пишет свой DOM без свопа).
const fixturesImportResultsSQL = "INSERT INTO subdomain_results " +
	"(zone_id, domain_id, subdomain_id, variant, scheme, " +
	"http_status, final_url, alias_kind, alias_subdomain_id, " +
	"headers, cookies, rendered, error, response_ms, " +
	"checked_at, rules_version, render_failed_at) " +
	"SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, r.scheme, " +
	"r.http_status, r.final_url, r.alias_kind, r.alias_subdomain_id, " +
	"r.headers, r.cookies, r.rendered, r.error, r.response_ms, " +
	"r.checked_at, r.rules_version, r.render_failed_at FROM unnest(" +
	"$1::int2[], $2::int8[], $3::int2[], $4::int2[], $5::int2[], " +
	"$6::int2[], $7::text[], $8::int2[], $9::int2[], " +
	"$10::jsonb[], $11::jsonb[], $12::bool[], $13::int2[], " +
	"$14::int4[], $15::timestamptz[], $16::int4[], $17::timestamptz[]) " +
	"AS r(zone_id, domain_id, subdomain_id, variant, scheme, " +
	"http_status, final_url, alias_kind, alias_subdomain_id, " +
	"headers, cookies, rendered, error, response_ms, " +
	"checked_at, rules_version, render_failed_at) " +
	"ON CONFLICT (zone_id, domain_id, subdomain_id, variant) DO UPDATE SET " +
	"scheme = EXCLUDED.scheme, http_status = EXCLUDED.http_status, " +
	"final_url = EXCLUDED.final_url, alias_kind = EXCLUDED.alias_kind, " +
	"alias_subdomain_id = EXCLUDED.alias_subdomain_id, " +
	"headers = EXCLUDED.headers, cookies = EXCLUDED.cookies, " +
	"rendered = EXCLUDED.rendered, content_hash = NULL, " +
	"error = EXCLUDED.error, response_ms = EXCLUDED.response_ms, " +
	"checked_at = EXCLUDED.checked_at, rules_version = EXCLUDED.rules_version, " +
	"render_failed_at = EXCLUDED.render_failed_at"

// Последовательности наката (заметки контракта T-242): identity зон — по
// pg_get_serial_sequence (информация о связи есть у GENERATED ALWAYS AS
// IDENTITY), domain_id_seq — литералом: DEFAULT nextval без OWNED BY не
// связан с колонкой, pg_get_serial_sequence('domains', 'id') вернул бы
// NULL (миграция 000001). GREATEST с максимумом таблицы закрывает БД,
// где до фикстуры уже есть строки с бо́льшими id; setval(v) выставляет
// last_value=v (is_called) — nextval выдаст v+1.
const (
	fixturesZonesSeqSQL = "SELECT setval(pg_get_serial_sequence('zones', 'id'), " +
		"GREATEST((SELECT coalesce(max(id), 0) FROM zones), $1::int))"

	fixturesDomainsSeqSQL = "SELECT setval('domain_id_seq', " +
		"GREATEST((SELECT coalesce(max(id), 0) FROM domains), $1::int8))"
)

// fixturesSweepContentsSQL — уборка осиротевшего контента после прогона
// (ревью T-243 P2-1): повторный накат сбрасывает content_hash строк, но
// не трогает contents — ссылки DOM прошлого прогона оставались с
// refcount > 0 и нулём держателей: компактификация удаляет только
// refcount = 0, retention выбирает лишь строки с живыми ссылками —
// контент не освобождался никогда и рос с каждым прогоном. На стенде
// единственный держатель ссылок — subdomain_results: выметаем строки
// contents без ссылок (паки на диске подожмёт компактификация
// хранилища — файлы учёта не держат). Выполняется при остановленных
// воркерах стенда (Put до записи результата — транзиентная ссылка).
const fixturesSweepContentsSQL = "DELETE FROM contents WHERE NOT EXISTS " +
	"(SELECT 1 FROM subdomain_results AS r WHERE r.content_hash = contents.hash)"

// ---------------------------------------------------------------- дамп

// fixturesDump — прочитанный дамп формата v1: манифест и строки файлов
// (структуры — общие с export, T-242). Дубли ключей внутри файлов
// (дамп T-242 — множество, защита от собранных вручную) вычитываются при
// чтении: побеждает последняя запись — та же семантика, что у наката
// поверх существующих строк.
type fixturesDump struct {
	Manifest fixturesManifest
	Zones    []fixturesZone
	Domains  []fixturesDomain
	Results  []fixturesResult
	Smoke    []fixturesSmokeDomain

	DupZones   int
	DupDomains int
	DupResults int
}

// fixturesReadNDJSON читает файл дампа построчно (один JSON-объект на
// строку): json.Decoder в потоке — строки дампа (headers/cookies) не
// ограничены по размеру, bufio.Reader держит память в узде.
func fixturesReadNDJSON[T any](path string) ([]T, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	dec := json.NewDecoder(bufio.NewReader(f))
	var out []T
	for n := 1; ; n++ {
		var row T
		err := dec.Decode(&row)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%s: запись %d: %w", path, n, err)
		}
		out = append(out, row)
	}
}

// fixturesDedup схлопывает дубли ключей: побеждает последняя запись
// (состояние дампа), счётчик — в отчёт.
func fixturesDedup[T any, K comparable](rows []T, dups *int, key func(T) K) []T {
	if len(rows) < 2 {
		return rows
	}
	pos := make(map[K]int, len(rows))
	out := make([]T, 0, len(rows))
	for _, r := range rows {
		k := key(r)
		if j, ok := pos[k]; ok {
			out[j] = r
			*dups++
		} else {
			pos[k] = len(out)
			out = append(out, r)
		}
	}
	return out
}

// fixturesReadDump читает и валидирует дамп формата v1. manifest.json
// появляется в выгрузке последним — его отсутствие означает незавершённый
// дамп (контракт T-242), накат не выполняется. Счётчики строк файлов в
// манифесте сверяются с фактически прочитанными (обрезанный при копировании
// дамп — ошибка, а не молчаливый частичный накат); ссылочная целостность
// (зоны доменов, домены строк) и уникальность естественных ключей
// (zones.name, (zone_id, label) доменов — PK-дедуп их не видит, INSERT
// упал бы pgcode 23505) проверяются до БД: обрезанный или пересобранный
// руками дамп падает понятной ошибкой.
func fixturesReadDump(dir string) (*fixturesDump, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("дамп %s: манифест: %w (без manifest.json дамп не завершён — контракт экспорта)", dir, err)
	}
	var manifest fixturesManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, fmt.Errorf("дамп %s: manifest.json: %w", dir, err)
	}
	if manifest.FormatVersion != fixturesFormatVersion {
		return nil, fmt.Errorf("дамп %s: формат v%d не поддерживается (инструмент понимает v%d)",
			dir, manifest.FormatVersion, fixturesFormatVersion)
	}

	dump := &fixturesDump{Manifest: manifest}
	// Чтение до дедупа: счётчик манифеста — про строки файла, дубли
	// вычитаются после сверки.
	files := []struct {
		name string
		read func() error
	}{
		{"zones.ndjson", func() (err error) {
			dump.Zones, err = fixturesReadNDJSON[fixturesZone](filepath.Join(dir, "zones.ndjson"))
			return
		}},
		{"domains.ndjson", func() (err error) {
			dump.Domains, err = fixturesReadNDJSON[fixturesDomain](filepath.Join(dir, "domains.ndjson"))
			return
		}},
		{"subdomain_results.ndjson", func() (err error) {
			dump.Results, err = fixturesReadNDJSON[fixturesResult](filepath.Join(dir, "subdomain_results.ndjson"))
			return
		}},
		{"smoke.ndjson", func() (err error) {
			dump.Smoke, err = fixturesReadNDJSON[fixturesSmokeDomain](filepath.Join(dir, "smoke.ndjson"))
			return
		}},
	}
	for _, f := range files {
		if _, err := os.Stat(filepath.Join(dir, f.name)); err != nil {
			return nil, fmt.Errorf("дамп %s: %w", dir, err)
		}
		if err := f.read(); err != nil {
			return nil, fmt.Errorf("дамп %s: %w", dir, err)
		}
	}
	for _, f := range files {
		got := 0
		switch f.name {
		case "zones.ndjson":
			got = len(dump.Zones)
		case "domains.ndjson":
			got = len(dump.Domains)
		case "subdomain_results.ndjson":
			got = len(dump.Results)
		case "smoke.ndjson":
			got = len(dump.Smoke)
		}
		want, ok := manifest.Files[f.name]
		if !ok {
			return nil, fmt.Errorf("дамп %s: манифест без счётчика строк %s (формат v1 его требует)", dir, f.name)
		}
		if want != got {
			return nil, fmt.Errorf("дамп %s: %s: строк %d, манифест насчитал %d — дамп обрезан или пересобран",
				dir, f.name, got, want)
		}
	}

	// Дубли ключей внутри файлов — множества с последней записью.
	dump.Zones = fixturesDedup(dump.Zones, &dump.DupZones, func(z fixturesZone) int16 { return z.ID })
	dump.Domains = fixturesDedup(dump.Domains, &dump.DupDomains,
		func(d fixturesDomain) fixturesDomainKey { return fixturesDomainKey{d.ZoneID, d.ID} })
	dump.Results = fixturesDedup(dump.Results, &dump.DupResults,
		func(r fixturesResult) fixturesKey { return fixturesKey{r.ZoneID, r.DomainID, r.SubdomainID, r.Variant} })

	// Идентификаторы положительны: нулевые/отрицательные (битые ручные
	// дампы) уронят и PK, и очередь (queue.Result.validate) — честнее
	// упасть до записи с указанием файла.
	seenNames := make(map[string]bool, len(dump.Zones))
	for _, z := range dump.Zones {
		if z.ID <= 0 {
			return nil, fmt.Errorf("дамп %s: zones.ndjson: id %d ≤ 0", dir, z.ID)
		}
		if seenNames[z.Name] {
			return nil, fmt.Errorf("дамп %s: zones.ndjson: имя %s дважды с разными id (UNIQUE zones.name — предпроверка наката здесь бессильна)", dir, z.Name)
		}
		seenNames[z.Name] = true
	}
	zoneIDs := make(map[int16]bool, len(dump.Zones))
	for _, z := range dump.Zones {
		zoneIDs[z.ID] = true
	}
	seenLabels := make(map[string]bool, len(dump.Domains))
	for _, d := range dump.Domains {
		if d.ID <= 0 {
			return nil, fmt.Errorf("дамп %s: domains.ndjson: домен %d/%d: id ≤ 0", dir, d.ZoneID, d.ID)
		}
		if !zoneIDs[d.ZoneID] {
			return nil, fmt.Errorf("дамп %s: domains.ndjson: домен %d/%d ссылается на зону, которой нет в zones.ndjson", dir, d.ZoneID, d.ID)
		}
		label := strconv.Itoa(int(d.ZoneID)) + ":" + d.Label
		if seenLabels[label] {
			return nil, fmt.Errorf("дамп %s: domains.ndjson: домен с меткой %s в зоне %d дважды с разными id (UNIQUE (zone_id, label))", dir, d.Label, d.ZoneID)
		}
		seenLabels[label] = true
	}
	domainKeys := make(map[fixturesDomainKey]bool, len(dump.Domains))
	for _, d := range dump.Domains {
		domainKeys[fixturesDomainKey{d.ZoneID, d.ID}] = true
	}
	for _, r := range dump.Results {
		if !domainKeys[fixturesDomainKey{r.ZoneID, r.DomainID}] {
			return nil, fmt.Errorf("дамп %s: subdomain_results.ndjson: строка %d/%d/%d/%v ссылается на домен, которого нет в domains.ndjson",
				dir, r.ZoneID, r.DomainID, r.SubdomainID, r.Variant)
		}
	}
	return dump, nil
}

// ---------------------------------------------------------------- накат

// fixturesImportStats — итог наката: применённые строки (после дедупа
// дампа) и выставленные последовательности (0 — не выполнялся: пустой
// файл дампа, sequence не трогаем).
type fixturesImportStats struct {
	Zones     int
	Domains   int
	Results   int
	ZoneSeq   int64
	DomainSeq int64
}

// fixturesImportRun — накат дампа в тестовую БД: предпроверки
// естественных ключей → зоны (OVERRIDING SYSTEM VALUE) → домены →
// строки результатов → последовательности. Каждый оператор — автокоммит
// (одноразовый инструмент): прерванный накат повторяется целиком,
// идемпотентность по PK докатывает то же состояние.
func fixturesImportRun(ctx context.Context, pool db.DB, dump *fixturesDump, stdout io.Writer) (*fixturesImportStats, error) {
	if err := fixturesCheckNaturalKeys(ctx, pool, dump); err != nil {
		return nil, err
	}

	stats := &fixturesImportStats{}
	var err error
	if stats.Zones, err = fixturesImportZones(ctx, pool, dump.Zones); err != nil {
		return nil, err
	}
	if stats.Domains, err = fixturesImportDomains(ctx, pool, dump.Domains); err != nil {
		return nil, err
	}
	if stats.Results, err = fixturesImportResults(ctx, pool, dump.Results); err != nil {
		return nil, err
	}
	if len(dump.Zones) > 0 {
		if stats.ZoneSeq, err = fixturesSetval(ctx, pool, fixturesZonesSeqSQL, maxDumpZoneID(dump.Zones)); err != nil {
			return nil, err
		}
	}
	if len(dump.Domains) > 0 {
		if stats.DomainSeq, err = fixturesSetval(ctx, pool, fixturesDomainsSeqSQL, maxDumpDomainID(dump.Domains)); err != nil {
			return nil, err
		}
	}
	// Уборка осиротевшего контента прошлогопрогона (fixturesSweepContentsSQL):
	// сброс content_hash строк оставил бы ссылки contents без держателей.
	// Воркеры стенда в этот момент остановлены (инструкция прогона).
	var swept int64
	if len(dump.Results) > 0 {
		tag, err := pool.Exec(ctx, fixturesSweepContentsSQL)
		if err != nil {
			return nil, fmt.Errorf("накат: уборка осиротевшего контента: %w", err)
		}
		swept = tag.RowsAffected()
		if swept > 0 {
			out(stdout, "осиротевший контент прогона убран: %d строк contents\n", swept)
		}
	}

	out(stdout, "накат: зоны %d, домены %d, строки результатов %d\n", stats.Zones, stats.Domains, stats.Results)
	if dump.DupZones+dump.DupDomains+dump.DupResults > 0 {
		out(stdout, "дубли ключей в дампе (схлопнуты): зоны %d, домены %d, результаты %d\n",
			dump.DupZones, dump.DupDomains, dump.DupResults)
	}
	out(stdout, "последовательности: zones.id → %d, domain_id_seq → %d\n", stats.ZoneSeq, stats.DomainSeq)
	return stats, nil
}

// fixturesCheckNaturalKeys — зоны с тем же именем и домены с той же
// меткой, но другим id: конфликт UNIQUE, невидимый для ON CONFLICT по PK.
// Накат рассчитан на чистую тестовую БД (§6.1 п. 3); конфликт — ошибка с
// перечнем, а не pgcode 23505 из глубины INSERT.
func fixturesCheckNaturalKeys(ctx context.Context, pool db.DB, dump *fixturesDump) error {
	if len(dump.Zones) > 0 {
		names := make([]string, 0, len(dump.Zones))
		dumpID := make(map[string]int16, len(dump.Zones))
		for _, z := range dump.Zones {
			names = append(names, z.Name)
			dumpID[z.Name] = z.ID
		}
		rows, err := pool.Query(ctx, fixturesZoneByNameSQL, names)
		if err != nil {
			return fmt.Errorf("накат: проверка имён зон: %w", err)
		}
		defer rows.Close()
		var conflicts []string
		for rows.Next() {
			var id int16
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				return fmt.Errorf("накат: проверка имён зон: чтение: %w", err)
			}
			if want, ok := dumpID[name]; ok && want != id {
				conflicts = append(conflicts, fmt.Sprintf("%s (id дампа %d, id БД %d)", name, want, id))
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("накат: проверка имён зон: %w", err)
		}
		if len(conflicts) > 0 {
			return fmt.Errorf("накат: зоны с тем же именем уже в БД с другими id: %s — тестовая БД должна быть чистой (миграции без данных, ТЗ §6.1 п. 3)",
				strings.Join(conflicts, ", "))
		}
	}

	if len(dump.Domains) > 0 {
		zones := make([]int16, 0, len(dump.Domains))
		labels := make([]string, 0, len(dump.Domains))
		// Естественный ключ домена — пара (zone_id, label): map по
		// строковому ключу zone:label (число:текст однозначно, ':'
		// в DNS-метке недопустим) → id дампа.
		byLabel := make(map[string]int64, len(dump.Domains))
		for _, d := range dump.Domains {
			zones = append(zones, d.ZoneID)
			labels = append(labels, d.Label)
			byLabel[strconv.Itoa(int(d.ZoneID))+":"+d.Label] = d.ID
		}
		rows, err := pool.Query(ctx, fixturesDomainByLabelSQL, zones, labels)
		if err != nil {
			return fmt.Errorf("накат: проверка меток доменов: %w", err)
		}
		defer rows.Close()
		var conflicts []string
		for rows.Next() {
			var zone int16
			var label string
			var id int64
			if err := rows.Scan(&zone, &label, &id); err != nil {
				return fmt.Errorf("накат: проверка меток доменов: чтение: %w", err)
			}
			if want, ok := byLabel[strconv.Itoa(int(zone))+":"+label]; ok && want != id {
				conflicts = append(conflicts, fmt.Sprintf("зона %d/%s (id дампа %d, id БД %d)", zone, label, want, id))
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("накат: проверка меток доменов: %w", err)
		}
		if len(conflicts) > 0 {
			return fmt.Errorf("накат: домены с той же меткой уже в БД с другими id: %s — тестовая БД должна быть чистой (миграции без данных, ТЗ §6.1 п. 3)",
				strings.Join(conflicts, ", "))
		}
	}
	return nil
}

// fixturesImportZones — накат зон пачками (зон в дампе единицы, пачка —
// тот же механизм). Возвращает число применённых строк.
func fixturesImportZones(ctx context.Context, pool db.DB, zones []fixturesZone) (int, error) {
	applied := 0
	for start := 0; start < len(zones); start += fixturesImportBatch {
		end := min(start+fixturesImportBatch, len(zones))
		part := zones[start:end]
		ids := make([]int16, len(part))
		names := make([]string, len(part))
		unicode := make([]*string, len(part))
		active := make([]bool, len(part))
		total := make([]int64, len(part))
		checked := make([]int64, len(part))
		updated := make([]*time.Time, len(part))
		for i, z := range part {
			ids[i], names[i], active[i] = z.ID, z.Name, z.IsActive
			total[i], checked[i] = z.DomainsTotal, z.DomainsChecked
			if z.NameUnicode != nil {
				s := *z.NameUnicode
				unicode[i] = &s
			}
			if z.UpdatedAt != nil {
				t := *z.UpdatedAt
				updated[i] = &t
			}
		}
		tag, err := pool.Exec(ctx, fixturesImportZonesSQL,
			ids, names, unicode, active, total, checked, updated)
		if err != nil {
			return applied, fmt.Errorf("накат зон (пачка %d…%d): %w", start, end, err)
		}
		if int(tag.RowsAffected()) != len(part) {
			return applied, fmt.Errorf("накат зон (пачка %d…%d): применено %d строк из %d", start, end, tag.RowsAffected(), len(part))
		}
		applied += len(part)
	}
	return applied, nil
}

// fixturesImportDomains — накат доменов пачками по fixturesImportBatch.
func fixturesImportDomains(ctx context.Context, pool db.DB, domains []fixturesDomain) (int, error) {
	applied := 0
	for start := 0; start < len(domains); start += fixturesImportBatch {
		end := min(start+fixturesImportBatch, len(domains))
		part := domains[start:end]
		zones := make([]int16, len(part))
		ids := make([]int64, len(part))
		labels := make([]string, len(part))
		status := make([]int16, len(part))
		checkState := make([]int16, len(part))
		flags := make([]int16, len(part))
		dnsMask := make([]int64, len(part))
		queued := make([]*time.Time, len(part))
		lastChecked := make([]*time.Time, len(part))
		tempAttempts := make([]int16, len(part))
		for i, d := range part {
			zones[i], ids[i], labels[i] = d.ZoneID, d.ID, d.Label
			status[i], checkState[i], flags[i] = d.Status, d.CheckState, d.Flags
			dnsMask[i], tempAttempts[i] = d.DNSMask, d.TempAttempts
			if d.QueuedAt != nil {
				t := *d.QueuedAt
				queued[i] = &t
			}
			if d.LastCheckedAt != nil {
				t := *d.LastCheckedAt
				lastChecked[i] = &t
			}
		}
		tag, err := pool.Exec(ctx, fixturesImportDomainsSQL,
			zones, ids, labels, status, checkState, flags, dnsMask, queued, lastChecked, tempAttempts)
		if err != nil {
			return applied, fmt.Errorf("накат доменов (пачка %d…%d): %w", start, end, err)
		}
		if int(tag.RowsAffected()) != len(part) {
			return applied, fmt.Errorf("накат доменов (пачка %d…%d): применено %d строк из %d", start, end, tag.RowsAffected(), len(part))
		}
		applied += len(part)
	}
	return applied, nil
}

// fixturesImportResults — накат строк subdomain_results пачками. jsonb
// (headers/cookies) идёт текстом в jsonb[] (стиль recordInsertSQL, T-058);
// NULL-колонки — nil-элементы массивов. content_hash не передаётся
// вовсе (см. fixturesImportResultsSQL): колонка получает DEFAULT NULL.
func fixturesImportResults(ctx context.Context, pool db.DB, rowsIn []fixturesResult) (int, error) {
	applied := 0
	for start := 0; start < len(rowsIn); start += fixturesImportBatch {
		end := min(start+fixturesImportBatch, len(rowsIn))
		part := rowsIn[start:end]
		n := len(part)
		zones := make([]int16, n)
		domains := make([]int64, n)
		subs := make([]int16, n)
		variants := make([]int16, n)
		scheme := make([]int16, n)
		httpStatus := make([]*int16, n)
		finalURL := make([]*string, n)
		aliasKind := make([]int16, n)
		aliasSub := make([]*int16, n)
		headers := make([]*string, n)
		cookies := make([]*string, n)
		rendered := make([]bool, n)
		errCode := make([]*int16, n)
		responseMS := make([]*int32, n)
		checkedAt := make([]time.Time, n)
		rulesVersion := make([]*int32, n)
		renderFailedAt := make([]*time.Time, n)
		for i, r := range part {
			zones[i], domains[i], subs[i], variants[i] = r.ZoneID, r.DomainID, r.SubdomainID, r.Variant
			scheme[i], aliasKind[i], rendered[i] = r.Scheme, r.AliasKind, r.Rendered
			checkedAt[i] = r.CheckedAt
			if r.HTTPStatus != nil {
				v := *r.HTTPStatus
				httpStatus[i] = &v
			}
			if r.FinalURL != nil {
				v := *r.FinalURL
				finalURL[i] = &v
			}
			if r.AliasSubdomainID != nil {
				v := *r.AliasSubdomainID
				aliasSub[i] = &v
			}
			headers[i] = fixturesJSONText(r.Headers)
			cookies[i] = fixturesJSONText(r.Cookies)
			if r.Error != nil {
				v := *r.Error
				errCode[i] = &v
			}
			if r.ResponseMs != nil {
				v := *r.ResponseMs
				responseMS[i] = &v
			}
			if r.RulesVersion != nil {
				v := *r.RulesVersion
				rulesVersion[i] = &v
			}
			if r.RenderFailedAt != nil {
				v := *r.RenderFailedAt
				renderFailedAt[i] = &v
			}
		}
		tag, err := pool.Exec(ctx, fixturesImportResultsSQL,
			zones, domains, subs, variants, scheme,
			httpStatus, finalURL, aliasKind, aliasSub,
			headers, cookies, rendered, errCode, responseMS,
			checkedAt, rulesVersion, renderFailedAt)
		if err != nil {
			return applied, fmt.Errorf("накат результатов (пачка %d…%d): %w", start, end, err)
		}
		if int(tag.RowsAffected()) != n {
			return applied, fmt.Errorf("накат результатов (пачка %d…%d): применено %d строк из %d", start, end, tag.RowsAffected(), n)
		}
		applied += n
	}
	return applied, nil
}

// fixturesJSONText — jsonb дампа (json.RawMessage) в элемент text[] для
// cast jsonb[]: nil (NULL в дампе) остаётся nil.
func fixturesJSONText(raw json.RawMessage) *string {
	if len(raw) == 0 {
		return nil
	}
	s := string(raw)
	return &s
}

// fixturesSetval выставляет последовательность и возвращает новое
// значение (для отчёта).
func fixturesSetval(ctx context.Context, pool db.DB, sql string, maxID int64) (int64, error) {
	rows, err := pool.Query(ctx, sql, maxID)
	if err != nil {
		return 0, fmt.Errorf("накат: последовательность: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, fmt.Errorf("накат: последовательность: пустой ответ setval")
	}
	var v int64
	if err := rows.Scan(&v); err != nil {
		return 0, fmt.Errorf("накат: последовательность: чтение: %w", err)
	}
	return v, rows.Err()
}

// maxDumpZoneID — максимальный id зон дампа (для setval).
func maxDumpZoneID(zones []fixturesZone) int64 {
	var max int64
	for _, z := range zones {
		if int64(z.ID) > max {
			max = int64(z.ID)
		}
	}
	return max
}

// maxDumpDomainID — максимальный id доменов дампа.
func maxDumpDomainID(domains []fixturesDomain) int64 {
	var max int64
	for _, d := range domains {
		if d.ID > max {
			max = d.ID
		}
	}
	return max
}

// ---------------------------------------------------------------- публикация

// fixturesPublishStats — итог публикации: опубликованные задачи, пропущенные
// дубли ключей, разбивка по стратам дампа и (в smoke-режиме) домены
// подвыборки.
type fixturesPublishStats struct {
	Published    int
	SkippedDups  int
	Strata       map[string]int
	SmokeDomains int
}

// fixturesPublishRun — публикация задач фикстуры в q:render (§6.1 п. 4):
// по одному queue.Result на строку дампа — тот же контракт, что у
// продюсеров q:render (renderq.go: HTTP-этап и проход доработки); Fresh
// не ставится — рендер-воркер сам помечает Fresh публикацию q:analyze
// (worker.go). Стрим — константа конфигурации стенда; ворота T-174 не
// участвуют: стенд публикует разово в пустую очередь (долг/глубина —
// забота живого конвейера, не изолированного прогона).
func fixturesPublishRun(ctx context.Context, q queue.Queue, dump *fixturesDump, mode string, stdout io.Writer) (*fixturesPublishStats, error) {
	if mode != fixturesPublishAll && mode != fixturesPublishSmoke {
		return nil, fmt.Errorf("публикация: неизвестный режим %q (допустимы: %s, %s)", mode, fixturesPublishAll, fixturesPublishSmoke)
	}

	// Имя домена — из строк дампа (label.zone, punycode): его пишут
	// продюсеры q:render для логов и ручного разбора (ТЗ 4.2). Отсутствие
	// (рассинхрон дампа исключён чтением) — пустое имя, контракт
	// queue.Result имя опциональным делает.
	labels := make(map[fixturesDomainKey]string, len(dump.Domains))
	for _, d := range dump.Domains {
		labels[fixturesDomainKey{d.ZoneID, d.ID}] = d.Label
	}
	zoneNames := make(map[int16]string, len(dump.Zones))
	for _, z := range dump.Zones {
		zoneNames[z.ID] = z.Name
	}
	nameOf := func(r fixturesResult) string {
		label, ok := labels[fixturesDomainKey{r.ZoneID, r.DomainID}]
		if !ok {
			return ""
		}
		zone := zoneNames[r.ZoneID]
		if zone == "" {
			return label
		}
		return label + "." + zone
	}

	// Режим smoke — только строки доменов smoke-подвыборки (§6.1 п. 6).
	selection := dump.Results
	stats := &fixturesPublishStats{Strata: map[string]int{}}
	if mode == fixturesPublishSmoke {
		smokeKeys := make(map[fixturesDomainKey]bool, len(dump.Smoke))
		for _, s := range dump.Smoke {
			smokeKeys[fixturesDomainKey{s.ZoneID, s.DomainID}] = true
		}
		stats.SmokeDomains = len(dump.Smoke)
		selection = nil
		for _, r := range dump.Results {
			if smokeKeys[fixturesDomainKey{r.ZoneID, r.DomainID}] {
				selection = append(selection, r)
			}
		}
		if len(selection) == 0 {
			// Пустая smoke-подвыборка с кодом 0 неотличима оператору от
			// рабочего прогона (деградировавший дамп, не тот каталог) —
			// честный отказ: вызывающий скрипт не должен считать прогон
			// состоявшимся.
			return stats, fmt.Errorf("публикация: smoke-подвыборка пуста (доменов %d, задач 0) — публиковать нечего", stats.SmokeDomains)
		}
	}

	// Дубли ключей (множество задач — одинаковый инвариант дампа T-242)
	// пропускаются с подсчётом: каждая строка — ровно одна задача.
	seen := make(map[fixturesKey]bool, len(selection))
	for _, r := range selection {
		k := fixturesKey{r.ZoneID, r.DomainID, r.SubdomainID, r.Variant}
		if seen[k] {
			stats.SkippedDups++
			continue
		}
		seen[k] = true
		msg := queue.Result{
			ZoneID:      r.ZoneID,
			DomainID:    r.DomainID,
			SubdomainID: r.SubdomainID,
			Variant:     model.Variant(r.Variant),
			Name:        nameOf(r),
		}
		if _, err := q.Publish(ctx, queue.StreamRender, msg); err != nil {
			return stats, fmt.Errorf("публикация q:render: опубликовано %d, прервано (зона %d, домен %d): %w",
				stats.Published, r.ZoneID, r.DomainID, err)
		}
		stats.Published++
		stats.Strata[r.Stratum]++
	}

	switch mode {
	case fixturesPublishSmoke:
		out(stdout, "публикация q:render (smoke, домены %d): опубликовано %d (ok %d, failed %d, heavy %d)\n",
			stats.SmokeDomains, stats.Published, stats.Strata["ok"], stats.Strata["failed"], stats.Strata["heavy"])
	default:
		out(stdout, "публикация q:render: опубликовано %d (ok %d, failed %d, heavy %d)\n",
			stats.Published, stats.Strata["ok"], stats.Strata["failed"], stats.Strata["heavy"])
	}
	if stats.SkippedDups > 0 {
		out(stdout, "дубли ключей задач пропущены: %d\n", stats.SkippedDups)
	}
	return stats, nil
}

// ---------------------------------------------------------------- команда

// fixturesImportCmd — crawlerctl fixtures import: разбор флагов, чтение
// дампа (локальные проверки до инфраструктуры), накат и — с --publish —
// публикация. Подключения — по образцу reanalyze: конфигурация + окружение
// (internal/config), пул 2 (бюджет ТЗ 6.3), Redis — адрес секции redis.
func fixturesImportCmd(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("fixtures import")
	dir := fs.String("dir", "", "каталог дампа формата v1 (обязателен)")
	publish := fs.String("publish", "", "публикация задач фикстуры в q:render без DNS/HTTP: "+fixturesPublishAll+
		" — все записи дампа, "+fixturesPublishSmoke+" — домены smoke-подвыборки (пусто — только накат)")
	if handled, err := parseFlags(fs, fixturesHelp, args, stdout); handled {
		return err
	}
	if strings.TrimSpace(*dir) == "" {
		return usagef("укажите каталог дампа: crawlerctl fixtures import --dir /path/to/fixtures")
	}
	switch *publish {
	case "", fixturesPublishAll, fixturesPublishSmoke:
	default:
		return usagef("fixtures import: --publish: неизвестный режим %q (допустимы: %s, %s)",
			*publish, fixturesPublishAll, fixturesPublishSmoke)
	}
	if fs.NArg() > 0 {
		return usagef("fixtures import: лишние аргументы %v (команда принимает только флаги)", fs.Args())
	}

	ctx, stop := signalCtx()
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("fixtures import: %w", err)
	}
	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2           // бюджет соединений ТЗ 6.3 — у сервисов
	poolOpts.AppName = "crawlerctl" // метка в pg_stat_activity (T-177)
	pool, err := db.Open(ctx, poolOpts)
	if err != nil {
		return fmt.Errorf("fixtures import: %w", err)
	}
	defer pool.Close()

	dump, err := fixturesReadDump(*dir)
	if err != nil {
		return fmt.Errorf("fixtures import: %w", err)
	}
	if _, err := fixturesImportRun(ctx, pool, dump, stdout); err != nil {
		return fmt.Errorf("fixtures import: %w", err)
	}
	if *publish == "" {
		return nil
	}

	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{
		MaxLen: int64(cfg.Redis.MaxLen), // XADD MAXLEN ~ на каждом стриме (T-025)
	})
	if _, err := fixturesPublishRun(ctx, q, dump, *publish, stdout); err != nil {
		return fmt.Errorf("fixtures import: %w", err)
	}
	return nil
}

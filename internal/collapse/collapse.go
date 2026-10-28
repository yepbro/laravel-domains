// Package collapse — перенос данных 2LD-зон в TLD-зоны (ТЗТ §7, T-196):
// зона системы — последняя метка имени (internal/idn, T-194), справочник
// зон сворачивается к TLD переносом строк: `zone_id` → TLD, `label` →
// `label || '.' || префикс`. Перенос — консольная команда обслуживания
// окна простоя (crawlerctl zones collapse, прецедент zones sync: CLI,
// а не миграция — тяжёлый перенос строк идёт батчами с отчётом,
// предусловиями и dry-run, ТЗТ §12.4); логика вынесена в пакет, чтобы
// интеграционные тесты прогоняли её против живой схемы.
//
// Свойства (ТЗТ §7.1):
//   - идемпотентность и возобновляемость: переносятся только строки, ещё
//     лежащие в 2LD-зоне (батчи адресуют zone_id = <2LD>); повторный
//     запуск продолжает с места остановки, отсутствие 2LD-зон — no-op;
//   - батчи §7.3: окна по DefaultBatch id из temp-таблицы moving_ids
//     (сессия одного соединения), каждый батч — отдельная транзакция
//     (домены и их дети переезжают атомарно на батч); нарушение
//     UNIQUE (zone_id, label) в батче невозможно (§12.5), аномальный
//     конфликт откатывает батч и останавливает команду с именем зоны;
//   - allowlist ingest.allowed_zone_paths — зоны-исключения (тестовые
//     зоны testbed живут как зоны, §7.1);
//   - предусловие: в затрагиваемых зонах нет доменов check_state = 1;
//   - перенос необратим (§12.5): откат — восстановление бэкапа.
package collapse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/db"
	"crawler/internal/idn"
	"crawler/internal/model"
	"crawler/internal/monitor"
)

// DefaultBatch — окно батча переноса (§7.3): 50000 id, константа, не
// конфиг; Options.Batch позволяет интеграционному тесту уменьшить окно
// (проверка возобновления после сбоя на середине).
const DefaultBatch = 50000

// ---------------------------------------------------------------- SQL
//
// Константы — контракты: юнит-тесты сверяют тексты и параметры (стиль
// cmd/crawlerctl zonessync.go), имена таблиц/колонок — миграции схемы.

// zonePartsFrom — общая часть выборки партиций зоны из каталога: прямые
// LIST-дети корневых таблиц по границе «FOR VALUES IN (<zone_id>)»
// (приём миграции 000003 create_zone_partitions; два варианта записи —
// pg_get_expr печатает smallint-границу в кавычках, %L даёт именно её,
// %s — страховка от смены формата). Для subdomain_results/laravel_*
// выбранный ребёнок — промежуточный родитель hash-листьев: DROP TABLE
// родителя уносит листья (§7.2 шаг 6).
const zonePartsFrom = "FROM pg_inherits i " +
	"JOIN pg_class c ON c.oid = i.inhrelid " +
	"JOIN pg_class p ON p.oid = i.inhparent " +
	"JOIN pg_namespace n ON n.oid = p.relnamespace " +
	"WHERE n.nspname = current_schema() " +
	"AND p.relname IN ('domains','subdomain_results','laravel_domains','laravel_findings') "

// InventorySQL — инвентаризация 2LD-зон вне allowlist (§7.2): LIKE '%.%'
// — составное имя; name <> ALL($1) — вычитание allowlist (NOT IN без
// NULL-ловушек). Скалярные подзапросы — колонки отчёта §7.4 (domains_now
// prescribed ТЗТ; счётчики зон-потомков и партиции — для плана), все —
// точечные по префиксу PK zone_id. domains_total — кэш monitor (T-178)
// до переноса, сверяется живым domains_now.
const InventorySQL = "SELECT z.id, z.name, z.domains_total, " +
	"(SELECT count(*) FROM domains d WHERE d.zone_id = z.id) AS domains_now, " +
	"(SELECT count(*) FROM subdomain_results r WHERE r.zone_id = z.id), " +
	"(SELECT count(*) FROM laravel_findings f WHERE f.zone_id = z.id), " +
	"(SELECT count(*) FROM laravel_history h WHERE h.zone_id = z.id), " +
	"(SELECT count(*) FROM ingest_files i WHERE i.zone_id = z.id), " +
	"EXISTS (SELECT 1 " + zonePartsFrom +
	"AND pg_get_expr(c.relpartbound, c.oid) IN (" +
	"format('FOR VALUES IN (%s)', z.id), format('FOR VALUES IN (%L)', z.id::text))) " +
	"FROM zones z WHERE z.name LIKE '%.%' AND z.name <> ALL($1) ORDER BY z.name"

// QueuedSQL — предусловие §7.1: домены в очереди (check_state = 1) по
// затрагиваемым зонам; код состояния — параметр (internal/model).
const QueuedSQL = "SELECT zone_id, count(*) FROM domains " +
	"WHERE zone_id = ANY($1::int2[]) AND check_state = $2 GROUP BY zone_id"

// ZoneIDByNameSQL / ZoneInsertSQL — шаг 1: зона TLD находится по имени,
// отсутствующая создаётся включённой (стиль zones sync: unicode-форма
// только для XN-зон, ASCII — NULL через nullif).
const ZoneIDByNameSQL = "SELECT id FROM zones WHERE name = $1"

const ZoneInsertSQL = "INSERT INTO zones (name, name_unicode, is_active) " +
	"VALUES ($1, nullif($2, ''), true) RETURNING id"

// CallPartitionsSQL — шаг 2: выделенные партиции зоны TLD до переноса,
// чтобы строки ложились сразу в них; идемпотентно (миграция 000003
// пропускает существующие с NOTICE).
const CallPartitionsSQL = "CALL create_zone_partitions($1)"

// Шаг 3 — temp-таблица moving_ids на сессию соединения: замороженное
// множество переносимых id (окна батчей §7.3 адресуют его границы).
// Перед созданием — DROP IF EXISTS: повторный запуск зоны (возобновление)
// пересобирает таблицу по остатку строк в 2LD-зоне.
const (
	movingDropSQL   = "DROP TABLE IF EXISTS moving_ids"
	movingCreateSQL = "CREATE TEMP TABLE moving_ids AS SELECT id FROM domains WHERE zone_id = $1"
	movingIndexSQL  = "CREATE INDEX ON moving_ids (id)"
	// movingRangeSQL — границы окон: id ≥ 1 (последовательность
	// domain_id_seq), coalesce 0 — пустое множество (батчей нет).
	movingRangeSQL = "SELECT coalesce(min(id), 0), coalesce(max(id), 0) FROM moving_ids"
	// spotLabelSQL — метка первого домена зоны до переноса: материал
	// спот-чека §7.5 (domain_info после переноса).
	spotLabelSQL = "SELECT label FROM domains WHERE zone_id = $1 ORDER BY id LIMIT 1"
)

// Шаг 4 — UPDATE с row movement (PG16 переносит строки между партициями
// LIST при смене ключа): домены меняют зону и доклеивают префикс к label
// (§7.2), дети адресуются диапазоном domain_id того же окна. Охрана
// zone_id = $zid в WHERE — идемпотентность: уже перенесённые строки под
// предикат не попадают.
const (
	moveDomainsSQL = "UPDATE domains " +
		"SET zone_id = $1, label = label || '.' || $2 " +
		"WHERE zone_id = $3 AND id >= $4 AND id < $5"
	moveResultsSQL  = "UPDATE subdomain_results SET zone_id = $1 WHERE zone_id = $2 AND domain_id >= $3 AND domain_id < $4"
	moveLaravelSQL  = "UPDATE laravel_domains SET zone_id = $1 WHERE zone_id = $2 AND domain_id >= $3 AND domain_id < $4"
	moveFindingsSQL = "UPDATE laravel_findings SET zone_id = $1 WHERE zone_id = $2 AND domain_id >= $3 AND domain_id < $4"
	// laravel_history — без партиций, объём на порядки меньше (миграция
	// 000011): одним оператором на зону (§7.2 шаг 4).
	moveHistorySQL = "UPDATE laravel_history SET zone_id = $1 WHERE zone_id = $2"
)

// Шаг 5 — дедуп-журнал ingest_files: перепривязка к TLD сохраняет дедуп
// точной повторной доставки (T-113); то же содержимое, уже доставлявшееся
// в T — дубль удаляется (остаётся строка-владелец). NULL-семантика ключа
// (ручные строки) — IS NOT DISTINCT FROM.
const (
	filesDedupeSQL = "DELETE FROM ingest_files a USING ingest_files b " +
		"WHERE a.zone_id = $1 AND b.zone_id = $2 " +
		"AND a.action IS NOT DISTINCT FROM b.action AND a.sha256 = b.sha256"
	filesMoveSQL = "UPDATE ingest_files SET zone_id = $1 WHERE zone_id = $2"
)

// ZonePartsSQL — шаг 6: выборка партиций 2LD-зоны (parent — корневая
// таблица, part — партиция зоны). DROP выполняется отдельно по каждой
// строке: имя из каталога экранируется pgx.Identifier.
const ZonePartsSQL = "SELECT p.relname AS parent, c.relname AS part " + zonePartsFrom +
	"AND pg_get_expr(c.relpartbound, c.oid) IN (" +
	"format('FOR VALUES IN (%s)', $1::smallint), format('FOR VALUES IN (%L)', $1::text))"

// ZoneLeftSQL — страховка перед DROP: перенос обязан был опустошить зону
// во всех таблицах с zone_id (батчи покрывают [min, max] moving_ids —
// все дети зоны); остаток — аномалия данных (сирота вне диапазона),
// DROP молча удалил бы строки — команда останавливается с ошибкой.
const ZoneLeftSQL = "SELECT " +
	"(SELECT count(*) FROM domains WHERE zone_id = $1), " +
	"(SELECT count(*) FROM subdomain_results WHERE zone_id = $1), " +
	"(SELECT count(*) FROM laravel_domains WHERE zone_id = $1), " +
	"(SELECT count(*) FROM laravel_findings WHERE zone_id = $1), " +
	"(SELECT count(*) FROM laravel_history WHERE zone_id = $1), " +
	"(SELECT count(*) FROM ingest_files WHERE zone_id = $1)"

// ZoneDeleteSQL — шаг 7: строка 2LD-зоны покидает справочник.
const ZoneDeleteSQL = "DELETE FROM zones WHERE id = $1"

// Пост-проверки §7.5: 2LD-зон вне allowlist не осталось; сиротских
// zone_id в таблицах с зоной нет.
const (
	CheckZonesLeftSQL = "SELECT count(*) FROM zones WHERE name LIKE '%.%' AND name <> ALL($1)"

	CheckOrphansSQL = "SELECT " +
		"(SELECT count(*) FROM domains t WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.id = t.zone_id)), " +
		"(SELECT count(*) FROM subdomain_results t WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.id = t.zone_id)), " +
		"(SELECT count(*) FROM laravel_domains t WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.id = t.zone_id)), " +
		"(SELECT count(*) FROM laravel_findings t WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.id = t.zone_id)), " +
		"(SELECT count(*) FROM laravel_history t WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.id = t.zone_id))"
)

// SpotCheckSQL — спот-чек §7.5 через SQL-функцию домена (миграция
// 000006): зона — самый длинный суффикс из справочника, после сворачивания
// это T. Скалярная функция в FROM даёт ровно одну строку даже при NULL
// (домен/зона не найдены) — WHERE di IS NOT NULL выжимает пустой ответ.
const SpotCheckSQL = "SELECT di->'zone'->>'name', di->'domain'->>'label' " +
	"FROM domain_info($1) di WHERE di IS NOT NULL"

// ---------------------------------------------------------------- типы

// Conn — соединение переноса: temp-таблица moving_ids живёт в сессии
// одного соединения (шаг 3), батчи — транзакции того же соединения
// (§7.3); *pgxpool.Conn и *pgx.Conn удовлетворяют интерфейсу.
type Conn interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Options — параметры Run; нулевые значения — дефолты. Поле Allowlist
// заполняет CLI из ingest.allowed_zone_paths (валидация и нормализация —
// internal/config, T-195).
type Options struct {
	// Allowlist — многоуровневые зоны, не сворачиваемые (ТЗТ §7.1).
	Allowlist []string
	// Apply — выполнить перенос; false — dry-run: инвентаризация и план,
	// ничего не меняет.
	Apply bool
	// Force — игнорировать предусловие check_state = 1 (после разбора
	// оператором, runbook §8.2).
	Force bool
	// SkipReconcile — пропустить реконсиляцию счётчиков после переноса
	// (её выполнит первый проход monitor, §7.2 шаг 8).
	SkipReconcile bool
	// Batch — размер окна id (§7.3); 0 — DefaultBatch. Не конфиг:
	// уменьшение для интеграционного теста возобновления.
	Batch int
}

// ZoneRow — строка инвентаризации: зона и объёмы переноса (отчёт §7.4).
type ZoneRow struct {
	ID       int16
	Name     string // punycode, составное имя (LIKE '%.%')
	Total    int64  // zones.domains_total — кэш monitor до переноса
	Domains  int64  // живой счёт (domains_now по ТЗТ)
	Results  int64  // subdomain_results
	Findings int64  // laravel_findings
	History  int64  // laravel_history
	Files    int64  // ingest_files
	Parts    bool   // есть выделенные партиции зоны
}

// Spec — зона переноса, вычисленная из имени (§7.2): `5g.in` →
// TLD `in`, префикс `5g`; `city.kawasaki.jp` → TLD `jp`, префикс
// `city.kawasaki`.
type Spec struct {
	ID     int16  // id 2LD-зоны
	Name   string // имя 2LD-зоны
	TLD    string // целевая зона — последняя метка имени
	Prefix string // имя без .TLD — доклейка к label
}

// SpecOf — Spec из строки инвентаризации: зона = последняя метка,
// префикс = всё до неё (формула §4.1 ТЗТ).
func SpecOf(z ZoneRow) (Spec, error) {
	i := strings.LastIndexByte(z.Name, '.')
	if i <= 0 || i == len(z.Name)-1 {
		return Spec{}, fmt.Errorf("имя зоны %q — не составное (пустая метка зоны или префикса)", z.Name)
	}
	return Spec{ID: z.ID, Name: z.Name, TLD: z.Name[i+1:], Prefix: z.Name[:i]}, nil
}

// Result — итог переноса одной зоны (отчёт apply; счётчики — строки,
// перенесённые этим запуском: возобновлённый запуск считает остаток).
type Result struct {
	Zone         string // имя 2LD-зоны
	TLD          string // целевая зона
	TLDZoneID    int16
	TLDZoneNew   bool // зона TLD создана переносом
	Domains      int64
	Results      int64
	Laravel      int64 // laravel_domains
	Findings     int64
	History      int64
	Files        int64
	FilesDupes   int64 // дублей ingest_files удалено (шаг 5)
	DroppedParts []string
	Batches      int
	// Спот-чек §7.5: полное имя первого домена зоны и ожидаемый label
	// после переноса; пустое имя — доменов в зоне не было.
	SpotName  string
	SpotLabel string
}

// ---------------------------------------------------------------- шаги

// Inventory — инвентаризация 2LD-зон вне allowlist (InventorySQL):
// список для dry-run, цикла переноса и предусловия.
func Inventory(ctx context.Context, pool db.DB, allowlist []string) ([]ZoneRow, error) {
	if pool == nil {
		return nil, errors.New("collapse: инвентаризация: пул не задан")
	}
	rows, err := pool.Query(ctx, InventorySQL, allowlistArg(allowlist))
	if err != nil {
		return nil, fmt.Errorf("collapse: инвентаризация 2LD-зон: %w", err)
	}
	defer rows.Close()
	var zones []ZoneRow
	for rows.Next() {
		var z ZoneRow
		if err := rows.Scan(&z.ID, &z.Name, &z.Total, &z.Domains, &z.Results,
			&z.Findings, &z.History, &z.Files, &z.Parts); err != nil {
			return nil, fmt.Errorf("collapse: инвентаризация 2LD-зон: чтение: %w", err)
		}
		zones = append(zones, z)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("collapse: инвентаризация 2LD-зон: %w", err)
	}
	return zones, nil
}

// allowlistArg — аргумент ALL($1): nil превращается в пустой массив
// (ALL(NULL) — NULL, вышибающий весь предикат).
func allowlistArg(allowlist []string) []string {
	if allowlist == nil {
		return []string{}
	}
	return allowlist
}

// QueuedCounts — домены в очереди (check_state = 1) по зонам: предусловие
// §7.1. Пустая карта — предусловие выполнено.
func QueuedCounts(ctx context.Context, pool db.DB, ids []int16) (map[int16]int64, error) {
	if len(ids) == 0 {
		return map[int16]int64{}, nil
	}
	rows, err := pool.Query(ctx, QueuedSQL, ids, int16(model.CheckStateQueued))
	if err != nil {
		return nil, fmt.Errorf("collapse: домены в очереди: %w", err)
	}
	defer rows.Close()
	queued := make(map[int16]int64)
	for rows.Next() {
		var id int16
		var n int64
		if err := rows.Scan(&id, &n); err != nil {
			return nil, fmt.Errorf("collapse: домены в очереди: чтение: %w", err)
		}
		queued[id] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("collapse: домены в очереди: %w", err)
	}
	return queued, nil
}

// Zone — перенос одной 2LD-зоны в TLD (шаги 1–7 §7.2) по одному
// соединению сессии (temp-таблица шага 3). Идемпотентно: каждый оператор
// адресует остаток строк в 2LD-зоне; сбой на любом шаге оставляет базу
// в состоянии, с которого повтор команды продолжается.
func Zone(ctx context.Context, conn Conn, spec Spec, batch int) (Result, error) {
	if conn == nil {
		return Result{}, fmt.Errorf("зона %s: соединение не задано", spec.Name)
	}
	if batch <= 0 {
		batch = DefaultBatch
	}
	res := Result{Zone: spec.Name, TLD: spec.TLD}

	// Шаг 1 — зона TLD.
	tid, created, err := ensureZone(ctx, conn, spec.TLD)
	if err != nil {
		return res, err
	}
	res.TLDZoneID, res.TLDZoneNew = tid, created

	// Шаг 2 — партиции TLD до переноса (строки ложатся сразу в них).
	if _, err := conn.Exec(ctx, CallPartitionsSQL, tid); err != nil {
		return res, fmt.Errorf("зона %s: партиции зоны %s (id %d): %w", spec.Name, spec.TLD, tid, err)
	}

	// Шаг 3 — moving_ids: замороженное множество переносимых id.
	if _, err := conn.Exec(ctx, movingDropSQL); err != nil {
		return res, fmt.Errorf("зона %s: пересоздание moving_ids: %w", spec.Name, err)
	}
	if _, err := conn.Exec(ctx, movingCreateSQL, spec.ID); err != nil {
		return res, fmt.Errorf("зона %s: moving_ids: %w", spec.Name, err)
	}
	if _, err := conn.Exec(ctx, movingIndexSQL); err != nil {
		return res, fmt.Errorf("зона %s: индекс moving_ids: %w", spec.Name, err)
	}

	// Спот-чек §7.5: первый домен зоны до переноса (после — пусто,
	// проверка не выполняется).
	if label, ok, err := firstZoneLabel(ctx, conn, spec.ID); err != nil {
		return res, err
	} else if ok {
		res.SpotName = label + "." + spec.Name
		res.SpotLabel = label + "." + spec.Prefix
	}

	// Шаг 4 — перенос батчами (§7.3): окна по возрастанию id.
	lo0, hi0, err := movingRange(ctx, conn)
	if err != nil {
		return res, err
	}
	for lo := lo0; hi0 > 0 && lo <= hi0; lo += int64(batch) {
		hi := lo + int64(batch)
		m, err := moveBatch(ctx, conn, spec, tid, lo, hi)
		if err != nil {
			return res, err
		}
		res.Batches++
		res.Domains += m.domains
		res.Results += m.results
		res.Laravel += m.laravel
		res.Findings += m.findings
	}

	// laravel_history — одним оператором на зону (без партиций).
	if tag, err := conn.Exec(ctx, moveHistorySQL, tid, spec.ID); err != nil {
		return res, fmt.Errorf("зона %s: laravel_history: %w", spec.Name, err)
	} else {
		res.History = tag.RowsAffected()
	}

	// Шаг 5 — дедуп-журнал ingest_files: дубль содержимого в T удаляется,
	// остаток перепривязывается.
	if tag, err := conn.Exec(ctx, filesDedupeSQL, spec.ID, tid); err != nil {
		return res, fmt.Errorf("зона %s: дедуп ingest_files: %w", spec.Name, err)
	} else {
		res.FilesDupes = tag.RowsAffected()
	}
	if tag, err := conn.Exec(ctx, filesMoveSQL, tid, spec.ID); err != nil {
		return res, fmt.Errorf("зона %s: ingest_files: %w", spec.Name, err)
	} else {
		res.Files = tag.RowsAffected()
	}

	// Шаг 6 — партиции 2LD-зоны дропнуть (с страховкой ZoneLeftSQL).
	parts, err := dropZonePartitions(ctx, conn, spec)
	if err != nil {
		return res, err
	}
	res.DroppedParts = parts

	// Шаг 7 — строка зоны.
	if tag, err := conn.Exec(ctx, ZoneDeleteSQL, spec.ID); err != nil {
		return res, fmt.Errorf("зона %s: удаление строки: %w", spec.Name, err)
	} else if tag.RowsAffected() != 1 {
		return res, fmt.Errorf("зона %s: удаление строки: затронуто %d строк, ожидалась 1", spec.Name, tag.RowsAffected())
	}
	return res, nil
}

// ensureZone — шаг 1: id зоны по имени; отсутствующая создаётся
// включённой (is_active = true — как автосоздание ingest'ом и zones
// sync, unicode-форма только для XN-зон).
func ensureZone(ctx context.Context, conn db.DB, name string) (id int16, created bool, err error) {
	rows, err := conn.Query(ctx, ZoneIDByNameSQL, name)
	if err != nil {
		return 0, false, fmt.Errorf("зона %s: чтение справочника: %w", name, err)
	}
	defer rows.Close()
	if rows.Next() {
		if err := rows.Scan(&id); err != nil {
			return 0, false, fmt.Errorf("зона %s: чтение справочника: %w", name, err)
		}
		return id, false, rows.Err()
	}
	if err := rows.Err(); err != nil {
		return 0, false, fmt.Errorf("зона %s: чтение справочника: %w", name, err)
	}
	// unicode-форма (стиль zones sync): только отличающаяся от punycode.
	unicode := ""
	if u := idn.ToUnicode(name); u != name {
		unicode = u
	}
	irows, err := conn.Query(ctx, ZoneInsertSQL, name, unicode)
	if err != nil {
		return 0, false, fmt.Errorf("зона %s: вставка зоны %s: %w", name, name, err)
	}
	defer irows.Close()
	if !irows.Next() {
		return 0, false, fmt.Errorf("зона %s: вставка зоны %s: строка не вставлена", name, name)
	}
	if err := irows.Scan(&id); err != nil {
		return 0, false, fmt.Errorf("зона %s: вставка зоны %s: чтение: %w", name, name, err)
	}
	return id, true, irows.Err()
}

// firstZoneLabel — метка первого (min id) домена зоны; ok = false —
// доменов нет.
func firstZoneLabel(ctx context.Context, conn db.DB, zoneID int16) (string, bool, error) {
	rows, err := conn.Query(ctx, spotLabelSQL, zoneID)
	if err != nil {
		return "", false, fmt.Errorf("зона %d: первый домен: %w", zoneID, err)
	}
	defer rows.Close()
	if rows.Next() {
		var label string
		if err := rows.Scan(&label); err != nil {
			return "", false, fmt.Errorf("зона %d: первый домен: чтение: %w", zoneID, err)
		}
		return label, true, rows.Err()
	}
	if err := rows.Err(); err != nil {
		return "", false, fmt.Errorf("зона %d: первый домен: %w", zoneID, err)
	}
	return "", false, nil
}

// movingRange — границы окон батчей: [min(id), max(id)] moving_ids;
// 0/0 — множество пусто.
func movingRange(ctx context.Context, conn db.DB) (int64, int64, error) {
	rows, err := conn.Query(ctx, movingRangeSQL)
	if err != nil {
		return 0, 0, fmt.Errorf("moving_ids: границы: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, 0, fmt.Errorf("moving_ids: границы: пустой ответ агрегата")
	}
	var lo, hi int64
	if err := rows.Scan(&lo, &hi); err != nil {
		return 0, 0, fmt.Errorf("moving_ids: границы: чтение: %w", err)
	}
	return lo, hi, rows.Err()
}

// batchMoved — строки одного батча по таблицам.
type batchMoved struct {
	domains, results, laravel, findings int64
}

// moveBatch — один батч §7.3: транзакция с четырьмя UPDATE одного
// диапазона (домены и их дети переезжают атомарно); откат по ошибке —
// батч целиком. Нарушение UNIQUE (zone_id, label) невозможно (§12.5);
// аномальный конфликт останавливает команду с именем зоны и диапазоном —
// естественная защита, отдельный pre-check не нужен.
func moveBatch(ctx context.Context, conn Conn, spec Spec, tid int16, lo, hi int64) (batchMoved, error) {
	var m batchMoved
	tx, err := conn.Begin(ctx)
	if err != nil {
		return m, fmt.Errorf("зона %s: батч [%d, %d): транзакция: %w", spec.Name, lo, hi, err)
	}
	// Откат безусловно: после Commit — no-op (стиль monitor, T-044).
	defer func() { _ = tx.Rollback(ctx) }()

	steps := []struct {
		sql  string
		args []any
		dst  *int64
	}{
		{moveDomainsSQL, []any{tid, spec.Prefix, spec.ID, lo, hi}, &m.domains},
		{moveResultsSQL, []any{tid, spec.ID, lo, hi}, &m.results},
		{moveLaravelSQL, []any{tid, spec.ID, lo, hi}, &m.laravel},
		{moveFindingsSQL, []any{tid, spec.ID, lo, hi}, &m.findings},
	}
	for _, st := range steps {
		tag, err := tx.Exec(ctx, st.sql, st.args...)
		if err != nil {
			return m, fmt.Errorf("зона %s: батч [%d, %d): %w", spec.Name, lo, hi, err)
		}
		*st.dst = tag.RowsAffected()
	}
	if err := tx.Commit(ctx); err != nil {
		return m, fmt.Errorf("зона %s: батч [%d, %d): фиксация: %w", spec.Name, lo, hi, err)
	}
	return m, nil
}

// dropZonePartitions — шаг 6: страховка пустоты зоны (ZoneLeftSQL),
// выборка партиций (ZonePartsSQL) и DROP по каждой. Имена — из каталога,
// экранирование идентификатора — pgx.Identifier (ввод извне недопустим).
func dropZonePartitions(ctx context.Context, conn db.DB, spec Spec) ([]string, error) {
	what := fmt.Sprintf("зона %s: партиции: ", spec.Name)

	lrows, err := conn.Query(ctx, ZoneLeftSQL, spec.ID)
	if err != nil {
		return nil, fmt.Errorf("%sостаток: %w", what, err)
	}
	defer lrows.Close()
	if !lrows.Next() {
		return nil, fmt.Errorf("%sостаток: пустой ответ агрегата", what)
	}
	var left [6]int64
	if err := lrows.Scan(&left[0], &left[1], &left[2], &left[3], &left[4], &left[5]); err != nil {
		return nil, fmt.Errorf("%sостаток: чтение: %w", what, err)
	}
	if err := lrows.Err(); err != nil {
		return nil, fmt.Errorf("%sостаток: %w", what, err)
	}
	tables := []string{"domains", "subdomain_results", "laravel_domains", "laravel_findings", "laravel_history", "ingest_files"}
	for i, n := range left {
		if n > 0 {
			return nil, fmt.Errorf("%sперенос неполон: в зоне осталось строк %s — %d (аномалия данных; DROP удалил бы их)",
				what, tables[i], n)
		}
	}

	prows, err := conn.Query(ctx, ZonePartsSQL, spec.ID)
	if err != nil {
		return nil, fmt.Errorf("%sвыборка: %w", what, err)
	}
	defer prows.Close()
	var parts []string
	for prows.Next() {
		var parent, part string
		if err := prows.Scan(&parent, &part); err != nil {
			return nil, fmt.Errorf("%sвыборка: чтение: %w", what, err)
		}
		parts = append(parts, part)
	}
	if err := prows.Err(); err != nil {
		return nil, fmt.Errorf("%sвыборка: %w", what, err)
	}
	for _, part := range parts {
		if _, err := conn.Exec(ctx, "DROP TABLE "+pgx.Identifier{part}.Sanitize()); err != nil {
			return nil, fmt.Errorf("%sDROP %s: %w", what, part, err)
		}
	}
	return parts, nil
}

// Orphans — счётчики сиротских zone_id пост-проверки §7.5 (ссылки на
// отсутствующие зоны в таблицах с zone_id).
type Orphans struct {
	Domains, Results, Laravel, Findings, History int64
}

// Any — есть ли сироты хоть в одной таблице.
func (o Orphans) Any() bool {
	return o.Domains != 0 || o.Results != 0 || o.Laravel != 0 || o.Findings != 0 || o.History != 0
}

// PostChecks — пост-проверки §7.5: 2LD-зон вне allowlist не осталось
// (zonesLeft = 0) и сиротских zone_id нет.
func PostChecks(ctx context.Context, pool db.DB, allowlist []string) (zonesLeft int64, orphans Orphans, err error) {
	zrows, err := pool.Query(ctx, CheckZonesLeftSQL, allowlistArg(allowlist))
	if err != nil {
		return 0, Orphans{}, fmt.Errorf("collapse: пост-проверка 2LD-зон: %w", err)
	}
	defer zrows.Close()
	if !zrows.Next() {
		return 0, Orphans{}, fmt.Errorf("collapse: пост-проверка 2LD-зон: пустой ответ агрегата")
	}
	if err := zrows.Scan(&zonesLeft); err != nil {
		return 0, Orphans{}, fmt.Errorf("collapse: пост-проверка 2LD-зон: чтение: %w", err)
	}
	if err := zrows.Err(); err != nil {
		return 0, Orphans{}, fmt.Errorf("collapse: пост-проверка 2LD-зон: %w", err)
	}

	orows, err := pool.Query(ctx, CheckOrphansSQL)
	if err != nil {
		return 0, Orphans{}, fmt.Errorf("collapse: пост-проверка сирот: %w", err)
	}
	defer orows.Close()
	if !orows.Next() {
		return 0, Orphans{}, fmt.Errorf("collapse: пост-проверка сирот: пустой ответ агрегата")
	}
	if err := orows.Scan(&orphans.Domains, &orphans.Results, &orphans.Laravel, &orphans.Findings, &orphans.History); err != nil {
		return 0, Orphans{}, fmt.Errorf("collapse: пост-проверка сирот: чтение: %w", err)
	}
	if err := orows.Err(); err != nil {
		return 0, Orphans{}, fmt.Errorf("collapse: пост-проверка сирот: %w", err)
	}
	return zonesLeft, orphans, nil
}

// SpotCheck — спот-чек §7.5: domain_info(name) отвечает зону wantZone и
// label wantLabel (доклеенный префикс).
func SpotCheck(ctx context.Context, pool db.DB, name, wantZone, wantLabel string) error {
	rows, err := pool.Query(ctx, SpotCheckSQL, name)
	if err != nil {
		return fmt.Errorf("collapse: спот-чек %s: %w", name, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return fmt.Errorf("collapse: спот-чек %s: domain_info не нашёл домен", name)
	}
	var zone, label string
	if err := rows.Scan(&zone, &label); err != nil {
		return fmt.Errorf("collapse: спот-чек %s: чтение: %w", name, err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("collapse: спот-чек %s: %w", name, err)
	}
	if zone != wantZone || label != wantLabel {
		return fmt.Errorf("collapse: спот-чек %s: зона %q, label %q; хочу зону %q, label %q",
			name, zone, label, wantZone, wantLabel)
	}
	return nil
}

// ---------------------------------------------------------------- проход

// Run — полный проход команды (§7.1–§7.5): инвентаризация и отчёт-план,
// предусловие check_state = 1, по --apply — перенос зон по шагам §7.2,
// реконсиляция счётчиков (общая с monitor/crawlerctl counters, T-178 —
// она же авторитетно пересчитает колонки TLD-зон и глобальные кумулятивы),
// пост-проверки и спот-чеки. Отчёт — в stdout (стиль zones sync).
//
// Перенос идёт по одному соединению пула (temp-таблица moving_ids живёт
// в его сессии), чтения — через пул.
func Run(ctx context.Context, pool *pgxpool.Pool, opts Options, stdout io.Writer) error {
	if pool == nil {
		return errors.New("collapse: пул не задан")
	}
	allow := allowlistArg(opts.Allowlist)
	batch := opts.Batch
	if batch <= 0 {
		batch = DefaultBatch
	}

	zones, err := Inventory(ctx, pool, allow)
	if err != nil {
		return err
	}
	if len(zones) == 0 {
		out(stdout, "2LD-зон вне allowlist нет — сворачивать нечего (no-op)\n")
		return nil
	}

	// Отчёт-план §7.4 (dry-run и apply).
	specs := make([]Spec, len(zones))
	for i, z := range zones {
		spec, err := SpecOf(z)
		if err != nil {
			return err
		}
		specs[i] = spec
	}
	printPlan(stdout, zones)

	// Предусловие §7.1: домены в очереди затрагиваемых зон.
	ids := make([]int16, len(zones))
	for i, z := range zones {
		ids[i] = z.ID
	}
	queued, err := QueuedCounts(ctx, pool, ids)
	if err != nil {
		return err
	}
	if len(queued) > 0 {
		parts := make([]string, 0, len(queued))
		for _, z := range zones { // порядок инвентаризации (по имени)
			if n, ok := queued[z.ID]; ok {
				parts = append(parts, fmt.Sprintf("%s — %d", z.Name, n))
			}
		}
		out(stdout, "домены в очереди (check_state = 1) в затрагиваемых зонах: %s\n", strings.Join(parts, ", "))
		if !opts.Apply {
			out(stdout, "дренируйте очереди перед переносом (runbook ТЗТ §8.2) или повторите с --force\n")
		} else if !opts.Force {
			return fmt.Errorf("в затрагиваемых зонах есть домены в очереди (check_state = 1); дренируйте очереди (runbook ТЗТ §8.2) или повторите с --force")
		} else {
			out(stdout, "продолжение по --force (после разбора очередей оператором, runbook ТЗТ §8.2)\n")
		}
	}

	if !opts.Apply {
		out(stdout, "dry-run: данные не изменены; перенос выполнит crawlerctl zones collapse --apply\n")
		return nil
	}

	// Перенос: одно соединение (temp-таблица §7.3), зоны по порядку
	// инвентаризации; сбой останавливает команду с именем зоны.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("collapse: соединение переноса: %w", err)
	}
	defer func() {
		// Финальная уборка moving_ids (§7.2): temp-таблица пережила бы
		// возврат соединения в пул. Луч-effort: отмена ctx не должна
		// мешать уборке.
		_, _ = conn.Exec(context.Background(), movingDropSQL)
		conn.Release()
	}()

	results := make([]Result, len(zones))
	for i, spec := range specs {
		res, err := Zone(ctx, conn, spec, batch)
		if err != nil {
			return err
		}
		results[i] = res
		printMoved(stdout, res)
	}

	// Шаг 8 — счётчики: колонки зон не мержатся вручную, общая
	// реконсиляция (T-178) авторитетно пересчитывает TLD-зоны и
	// глобальные кумулятивы; --skip-reconcile оставляет это monitor.
	if opts.SkipReconcile {
		out(stdout, "реконсиляция счётчиков пропущена (--skip-reconcile): колонки зон пересчитает первый проход monitor\n")
	} else {
		rec, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
		if err != nil {
			return fmt.Errorf("collapse: реконсиляция счётчиков: %w", err)
		}
		drifted := 0
		for _, p := range rec.Positions {
			if p.Drift() == 0 {
				continue
			}
			drifted++
			out(stdout, "  %-32s %12d → %d (Δ%d)\n", p.Counter, p.Old, p.New, p.Drift())
		}
		out(stdout, "реконсиляция счётчиков: перезаписано зон %d, позиций с дрейфом %d\n", rec.Zones, drifted)
	}

	// Пост-проверки §7.5.
	zonesLeft, orphans, err := PostChecks(ctx, pool, allow)
	if err != nil {
		return err
	}
	out(stdout, "пост-проверки: 2LD-зон вне allowlist: %d; сиротских zone_id: domains %d, subdomain_results %d, laravel_domains %d, laravel_findings %d, laravel_history %d\n",
		zonesLeft, orphans.Domains, orphans.Results, orphans.Laravel, orphans.Findings, orphans.History)
	if zonesLeft != 0 || orphans.Any() {
		return fmt.Errorf("пост-проверки не сошлись: 2LD-зон %d, сирот %+v", zonesLeft, orphans)
	}
	if !opts.SkipReconcile {
		mismatch, err := monitor.VerifyCounters(ctx, pool, monitor.VerifyOptions{})
		if err != nil {
			return fmt.Errorf("collapse: сверка счётчиков: %w", err)
		}
		if len(mismatch) > 0 {
			return fmt.Errorf("collapse: счётчики зон ≠ агрегат: %d позиций с дрейфом (лечит crawlerctl counters recount)", len(mismatch))
		}
		out(stdout, "счётчики зон = агрегат: расхождений нет\n")
	}

	// Спот-чек §7.5: по одному перенесённому имени на зону с доменами.
	for i := range results {
		res := results[i]
		if res.SpotName == "" {
			continue
		}
		if err := SpotCheck(ctx, pool, res.SpotName, specs[i].TLD, res.SpotLabel); err != nil {
			return err
		}
		out(stdout, "спот-чек %s: зона %s, label %s — ок\n", res.SpotName, specs[i].TLD, res.SpotLabel)
	}
	out(stdout, "перенос завершён: 2LD-зон %d\n", len(zones))
	return nil
}

// out — печать отчёта (стиль cmd/crawlerctl: ошибка записи игнорируется,
// поток — stdout процесса).
func out(w io.Writer, format string, a ...any) {
	_, _ = fmt.Fprintf(w, format, a...)
}

// printPlan — таблица плана §7.4 в стиле zones list: числовые колонки
// вправо, текстовые влево; итог — по каждой целевой TLD.
func printPlan(w io.Writer, zones []ZoneRow) {
	head := []string{"зона", "TLD", "доменов", "результатов", "находок", "история", "файлов", "партиции"}
	rows := [][]string{head}
	for _, z := range zones {
		tld := zoneTLD(z.Name)
		parts := "default"
		if z.Parts {
			parts = "есть"
		}
		rows = append(rows, []string{
			z.Name, tld,
			fmt.Sprintf("%d", z.Domains), fmt.Sprintf("%d", z.Results),
			fmt.Sprintf("%d", z.Findings), fmt.Sprintf("%d", z.History),
			fmt.Sprintf("%d", z.Files), parts,
		})
	}
	wmax := make([]int, len(head))
	for _, r := range rows {
		for i, c := range r {
			if l := len([]rune(c)); l > wmax[i] {
				wmax[i] = l
			}
		}
	}
	for _, r := range rows {
		var b strings.Builder
		for i, c := range r {
			if i > 0 {
				b.WriteByte(' ')
			}
			if i >= 2 && i <= 6 { // числовые колонки — вправо
				fmt.Fprintf(&b, "%*s", wmax[i], c)
			} else {
				fmt.Fprintf(&b, "%-*s", wmax[i], c)
			}
		}
		out(w, "%s\n", b.String())
	}
	// Итоги по целевым TLD (§7.4): несколько 2LD одной TLD суммируются.
	byTLD := make(map[string]int64)
	for _, z := range zones {
		byTLD[zoneTLD(z.Name)] += z.Domains
	}
	tlds := make([]string, 0, len(byTLD))
	for tld := range byTLD {
		tlds = append(tlds, tld)
	}
	sort.Strings(tlds)
	for _, tld := range tlds {
		n := 0
		for _, z := range zones {
			if zoneTLD(z.Name) == tld {
				n++
			}
		}
		word := "зона"
		if n > 1 {
			word = "зоны"
		}
		out(w, "итого: 2LD-зон %d, доменов %d → %s %s\n", n, byTLD[tld], word, tld)
	}
}

// zoneTLD — целевая зона имени: последняя метка (формула §4.1 ТЗТ);
// вызывается только для составных имён инвентаризации.
func zoneTLD(name string) string {
	return name[strings.LastIndexByte(name, '.')+1:]
}

// printMoved — строка отчёта apply: фактический перенос зоны.
func printMoved(w io.Writer, r Result) {
	newMark := ""
	if r.TLDZoneNew {
		newMark = " (зона создана)"
	}
	parts := "партиций нет"
	if len(r.DroppedParts) > 0 {
		parts = fmt.Sprintf("партиции др. %d: %s", len(r.DroppedParts), strings.Join(r.DroppedParts, ", "))
	}
	out(w, "%s → %s (id %d%s): батчей %d, доменов %d, результатов %d, laravel-доменов %d, находок %d, история %d, файлов %d (дублей удалено %d), %s\n",
		r.Zone, r.TLD, r.TLDZoneID, newMark, r.Batches, r.Domains, r.Results,
		r.Laravel, r.Findings, r.History, r.Files, r.FilesDupes, parts)
}

// compile-time: соединение пула подходит под интерфейс переноса.
var _ Conn = (*pgxpool.Conn)(nil)

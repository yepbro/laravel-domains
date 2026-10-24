// ---------------------------------------------------------------- recheck
//
// crawlerctl recheck (T-084, ТЗ 4.1): принудительная постановка доменов
// на проверку — «ручные задачи» из ТЗ 4.1. Имена — в любой форме
// (unicode/punycode, регистр, точка; internal/idn.Normalize — тот же
// конвейер, что у domain, T-081), домены ищутся по ключу (zone, label)
// (UNIQUE, миграция 000001).
//
// Механизм (зафиксирован по семантике T-028/T-030, консистентен
// subdomains recheck T-083): перевод check_state → pending,
// queued_at = NULL — публикацию в q:dns делает планировщик, он владеет
// backpressure (watermark) и lease (T-028); CLI, публикующий сам,
// дублировал бы конвейер и обходил бы порог очереди. Домен в pending
// подберёт DispatchBatch (T-027) в круговом порядке между зонами.
//
// Семантика по исходным состояниям (check_state):
//
//   - done → pending: основной случай (оператор проверил руками сайт —
//     не ждём recheck_interval, ТЗ 5.3);
//   - pending → pending: идемпотентный no-op — домен уже ждёт, перевод
//     ничего не меняет;
//   - queued → pending: равносилен ручному возврату lease (T-029):
//     домен взят в работу, повторная проверка гарантируется; задача
//     в q:dns уже могла быть опубликована — домен проверится дважды,
//     доставка at-least-once и идемпотентные записи результатов это
//     допускают (ТЗ 3.4), дубль работы ограничен одним циклом.
//
// deleted — отказ с сообщением (ТЗ 13.1: удалённые не проверяются);
// не найденные (нет зоны/нет метки в зоне) — отказ со списком; код 1
// возвращается, если хоть один домен не поставлен (поставленные всё
// равно переводятся — частичный успех виден в выводе).

package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5"

	"crawler/internal/db"
	"crawler/internal/idn"
	"crawler/internal/model"
)

// recheckHelp — справка crawlerctl recheck --help.
const recheckHelp = `crawlerctl recheck — принудительная постановка домена на проверку

Использование:
  crawlerctl recheck <домен> [домен …]

Домены (одно имя или несколько, в любой форме: unicode/punycode,
регистр, точка на конце) ставятся на проверку, не дожидаясь интервала
перепроверки (ТЗ 5.3): найденные по имени домены переводятся в
check_state = pending (queued_at сбрасывается), публикацию в q:dns
делает планировщик — с учётом backpressure, темп задаёт очередь.
Это «ручные задачи» crawlerctl из ТЗ 4.1.

Домен в работе (queued) переводится как возврат lease: он проверится
заново; уже ожидающий (pending) остаётся в очереди. Удалённый
(deleted) домен не проверяется (ТЗ 13.1) — отказ с сообщением.
Не найденные имена (нет зоны в справочнике или нет метки в зоне)
перечисляются в отказе, код 1; некорректное имя — код 2.

Коды возврата: 0 — все домены поставлены, 1 — часть не поставлена
или ошибка, 2 — использование.
`

// ---------------------------------------------------------------- SQL

// recheckLookupSQL — разрешение всех запрошенных имён одним запросом:
// полные punycode-имена приходят массивом (unnest — стиль пакетных
// операций internal/db), WITH ORDINALITY сохраняет порядок аргументов
// для вывода. Зона — длиннейший суффикс имени, присутствующий
// в справочнике zones (как отделяет зону domain_info, T-082: PSL
// тестовых зон вроде crawl.test не знает, последняя метка неверна;
// co.uk предпочитается uk), LATERAL LIMIT 1 — по убыванию длины.
// Метка — префикс имени без точки и зоны: substr(name, 1,
// length(name) − length(zone) − 1). LEFT JOIN-ы не теряют ни одно
// имя: отсутствующая зона/метка — coalesce-нули (id ≥ 1 у реальных
// строк, миграция 000001). is_active зоны — предупреждение «домен ждёт
// включения зоны» (zones disable, T-084). Зона заведомо не содержит
// «%»/«_» (idna.Lookup, ТЗ 5.2) — LIKE-шаблон безопасен. Выверено
// живым прогоном в изолированной схеме (стиль T-081).
const recheckLookupSQL = "SELECT n.ord, " +
	"coalesce(z.id, 0) AS zone_id, coalesce(z.name, '') AS zone_name, coalesce(z.is_active, false) AS zone_on, " +
	"coalesce(d.id, 0) AS domain_id, coalesce(d.label, '') AS label, " +
	"coalesce(d.check_state, 0) AS check_state, coalesce(d.status, 0) AS status " +
	"FROM unnest($1::text[]) WITH ORDINALITY AS n(name, ord) " +
	"LEFT JOIN LATERAL (" +
	"SELECT id, name, is_active FROM zones AS z " +
	"WHERE n.name LIKE '%.' || z.name " +
	"ORDER BY length(z.name) DESC, z.name LIMIT 1" +
	") AS z ON true " +
	"LEFT JOIN domains AS d ON d.zone_id = z.id AND d.label = substr(n.name, 1, length(n.name) - length(z.name) - 1) " +
	"ORDER BY n.ord"

// recheckUpdateSQL — перевод найденных доменов в pending одним
// оператором по ключам PK (unnest — стиль enqueueUpdate T-028):
// check_state = pending и queued_at = NULL — как у суточной
// перепроверки (T-030): у pending нет lease, его заведёт постановка
// в очередь. Охрана d.status <> deleted перепроверяет статус на момент
// блокировки строки — гонка с delete-файлом (T-019) не переведёт
// удаляемый домен; RETURNING — ключи фактически переведённых строк
// (пропущенные raced в deleted и попадают в отказ).
const recheckUpdateSQL = "UPDATE domains AS d " +
	"SET check_state = $1, queued_at = NULL " +
	"FROM unnest($2::int2[], $3::int8[]) AS k(zone_id, domain_id) " +
	"WHERE d.zone_id = k.zone_id AND d.id = k.domain_id AND d.status <> $4 " +
	"RETURNING d.zone_id, d.id"

// recheckStatesSQL — старые check_state переводимых строк точечным
// предчтением FOR UPDATE (T-178, ТЗИ §6.1.2: оператор перевода не
// пинирует старое состояние — pending/queued/done переводятся alike, а
// PG 16 без RETURNING OLD): блокировка строки сериализует предчтение с
// переводом в одной транзакции (recheckDB) — старое состояние
// актуально на момент UPDATE, дельты счётчиков зон точны (done →
// checked −n/pending +n, queued → queued −n/pending +n, pending → без
// дельт: состояние не менялось).
const recheckStatesSQL = "SELECT d.zone_id, d.id, d.check_state " +
	"FROM domains AS d " +
	"JOIN unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id) " +
	"ON d.zone_id = k.zone_id AND d.id = k.domain_id " +
	"WHERE d.status <> $3 " +
	"FOR UPDATE OF d"

// recheckDB — пул перевода с транзакциями: предчтение состояний,
// перевод и дельты счётчиков зон атомарны (T-178, ТЗИ §6.1.3);
// *pgxpool.Pool (openManagePool) удовлетворяет интерфейсу.
type recheckDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// ---------------------------------------------------------------- данные

// recheckItem — один запрошенный домен после разрешения имени
// (строка recheckLookupSQL). Нулевые id — «нет строки» (coalesce).
type recheckItem struct {
	ord      int64
	zoneID   int16 // 0 — зоны нет в справочнике
	zoneName string
	zoneOn   bool  // is_active зоны
	domainID int64 // 0 — домена нет в зоне
	label    string
	state    model.CheckState
	status   model.Status
}

// fetchRecheckItems разрешает полные punycode-имена (уже нормализованы)
// в строки доменов; результат — по одному элементу на имя, в порядке
// аргументов (ord).
func fetchRecheckItems(ctx context.Context, pool db.DB, names []string) ([]recheckItem, error) {
	rows, err := pool.Query(ctx, recheckLookupSQL, names)
	if err != nil {
		return nil, fmt.Errorf("разрешение имён доменов: %w", err)
	}
	defer rows.Close()
	items := make([]recheckItem, 0, len(names))
	for rows.Next() {
		var it recheckItem
		var state, status int16 // коды схем (smallint), типы модели — ниже
		if err := rows.Scan(&it.ord, &it.zoneID, &it.zoneName, &it.zoneOn,
			&it.domainID, &it.label, &state, &status); err != nil {
			return nil, fmt.Errorf("разрешение имён доменов: чтение: %w", err)
		}
		it.state, it.status = model.CheckState(state), model.Status(status)
		if it.ord < 1 || it.ord > int64(len(names)) {
			return nil, fmt.Errorf("разрешение имён доменов: ord %d вне диапазона 1..%d (контракт WITH ORDINALITY)", it.ord, len(names))
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("разрешение имён доменов: %w", err)
	}
	return items, nil
}

// recheckKey — ключ строки domains (PK, ТЗ 6.1): перевод адресуется
// парой (zone_id, id); дубликаты ключей в unnest не страшны — UPDATE
// применится один раз на строку.
type recheckKey struct {
	zone int16
	id   int64
}

// recheckTranslate переводит найденные домены в pending в одной
// транзакции (T-178): предчтение старых состояний FOR UPDATE →
// перевод (recheckUpdateSQL) → дельты счётчиков зон по старым
// состояниям переведённых строк (db.ZoneShift); возвращает множество
// фактически переведённых ключей (по RETURNING — охрана status могла
// пропустить строку).
func recheckTranslate(ctx context.Context, pool recheckDB, keys []recheckKey) (map[recheckKey]bool, error) {
	zones := make([]int16, len(keys))
	ids := make([]int64, len(keys))
	for i, k := range keys {
		zones[i], ids[i] = k.zone, k.id
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("перевод доменов в pending: транзакция: %w", err)
	}
	// Откат безусловно: после Commit — no-op (стиль T-044).
	defer func() { _ = tx.Rollback(ctx) }()

	// Старые состояния: блокировка строк делает предчтение и перевод
	// согласованными (дубль-доставка record/lease меняет состояние
	// только до блокировки — после неё мимо транзакции не пройти).
	oldStates := make(map[recheckKey]model.CheckState, len(keys))
	rows, err := tx.Query(ctx, recheckStatesSQL, zones, ids, int16(model.StatusDeleted))
	if err != nil {
		return nil, fmt.Errorf("перевод доменов в pending: чтение состояний: %w", err)
	}
	for rows.Next() {
		var k recheckKey
		var state int16
		if err := rows.Scan(&k.zone, &k.id, &state); err != nil {
			rows.Close()
			return nil, fmt.Errorf("перевод доменов в pending: чтение состояний: строка: %w", err)
		}
		oldStates[k] = model.CheckState(state)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("перевод доменов в pending: чтение состояний: %w", err)
	}
	rows.Close()

	rows, err = tx.Query(ctx, recheckUpdateSQL,
		int16(model.CheckStatePending), zones, ids, int16(model.StatusDeleted))
	if err != nil {
		return nil, fmt.Errorf("перевод доменов в pending: %w", err)
	}
	shift := db.NewZoneShift()
	done := make(map[recheckKey]bool, len(keys))
	for rows.Next() {
		var k recheckKey
		if err := rows.Scan(&k.zone, &k.id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("перевод доменов в pending: чтение: %w", err)
		}
		done[k] = true
		switch oldStates[k] {
		case model.CheckStateDone:
			shift.Add(k.zone, 0, 1, 0, -1) // checked −1, pending +1
		case model.CheckStateQueued:
			shift.Add(k.zone, 0, 1, -1, 0) // queued −1, pending +1
		}
		// pending → pending: состояние не менялось — без дельт.
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("перевод доменов в pending: %w", err)
	}
	rows.Close()

	if err := shift.Exec(ctx, tx); err != nil {
		return nil, fmt.Errorf("перевод доменов в pending: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("перевод доменов в pending: фиксация: %w", err)
	}
	return done, nil
}

// ---------------------------------------------------------------- действие

// recheckRun — постановка доменов на проверку: разрешение имён,
// перевод найденных, построчный итог. Возвращает ошибку, если хоть
// один домен не поставлен (переведённые остаются в pending — перевод
// не откатывается, повтор команды идемпотентен). names — полные
// punycode-имена (idn.Normalize), порядок сохраняется в выводе.
func recheckRun(ctx context.Context, pool recheckDB, names []string, stdout io.Writer) error {
	items, err := fetchRecheckItems(ctx, pool, names)
	if err != nil {
		return err
	}

	// Кандидаты на перевод: найденные и не deleted. Отказы собираются
	// построчно (что именно не поставлено и почему).
	var keys []recheckKey
	var failed []string
	for _, it := range items {
		name := names[it.ord-1] // ord — 1-базис WITH ORDINALITY
		switch {
		case it.zoneID == 0:
			// Зона-кандидат для сообщения — длиннейший суффикс имени
			// (стиль domainNotFound, T-081: что именно не найдено).
			zone := zoneSuffixCandidates(name)[0]
			out(stdout, "%s: не найден: зоны %s нет в справочнике (зоны создаёт ingest при первом файле)\n", name, zone)
			failed = append(failed, name)
		case it.domainID == 0:
			out(stdout, "%s: не найден: в зоне %s нет домена с меткой %s\n",
				name, it.zoneName, strings.TrimSuffix(name, "."+it.zoneName))
			failed = append(failed, name)
		case it.status == model.StatusDeleted:
			out(stdout, "%s: удалён (deleted) — повторная проверка не выполняется (ТЗ 13.1)\n", name)
			failed = append(failed, name)
		default:
			keys = append(keys, recheckKey{zone: it.zoneID, id: it.domainID})
		}
	}

	translated := map[recheckKey]bool{}
	if len(keys) > 0 {
		translated, err = recheckTranslate(ctx, pool, keys)
		if err != nil {
			return err
		}
	}

	// Построчный итог перевода — вторым проходом по items (порядок
	// аргументов): перевод мог пропустить строку, raced в deleted.
	for _, it := range items {
		if it.domainID == 0 {
			continue // отказ уже напечатан
		}
		name := names[it.ord-1]
		k := recheckKey{zone: it.zoneID, id: it.domainID}
		if !translated[k] {
			if it.status != model.StatusDeleted {
				out(stdout, "%s: не поставлен: статус изменился на deleted во время команды (ТЗ 13.1)\n", name)
				failed = append(failed, name)
			}
			continue
		}
		note := ""
		if !it.zoneOn {
			note = " — зона выключена, домен ждёт включения (crawlerctl zones enable " + it.zoneName + ")"
		}
		out(stdout, "%s: поставлен на проверку (было check_state: %s)%s\n", name, it.state, note)
	}

	out(stdout, "переведено в проверку (pending): %d из %d\n", len(translated), len(items))
	if len(failed) > 0 {
		return fmt.Errorf("не поставлены: %s", joinNames(failed))
	}
	out(stdout, "домены проверит планировщик: выборка pending → q:dns (backpressure задаёт темп, ТЗ 5.3)\n")
	return nil
}

// joinNames перечисляет имена отказа: первые три полностью, остаток
// счётчиком (+N) — сообщение-строка не растягивается.
func joinNames(names []string) string {
	const show = 3
	if len(names) <= show {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s (+%d)", strings.Join(names[:show], ", "), len(names)-show)
}

// ---------------------------------------------------------------- команда

// cmdRecheck — crawlerctl recheck: нормализация имён (до конфигурации —
// неверное имя это ошибка использования, код 2), затем разовая
// постановка. Ctrl-C/SIGTERM прерывают команду; переведённое остаётся
// (перевод идемпотентен, повтор продолжает).
func cmdRecheck(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("recheck")
	if handled, err := parseFlags(fs, recheckHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() == 0 {
		return usagef("укажите домен: crawlerctl recheck example.crawl.test")
	}
	names := make([]string, 0, fs.NArg())
	for _, a := range fs.Args() {
		res, err := idn.Normalize(a)
		if err != nil {
			return usagef("recheck: некорректное имя домена %q: %v", a, err)
		}
		names = append(names, res.ASCII)
	}

	ctx, stop := signalCtx()
	defer stop()

	pool, err := openManagePool(ctx)
	if err != nil {
		return fmt.Errorf("recheck %s: %w", joinNames(names), err)
	}
	defer pool.Close()

	if err := recheckRun(ctx, pool, names, stdout); err != nil {
		return fmt.Errorf("recheck %s: %w", joinNames(names), err)
	}
	return nil
}

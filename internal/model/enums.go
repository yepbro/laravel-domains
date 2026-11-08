package model

import (
	"fmt"
	"strings"
)

// Status — статус домена, колонка domains.status (smallint, ТЗ 6.2).
// Коды 0..6 зафиксированы схемой и тестом; код 7 — диагностический
// invalid (T-109). ТЗ 6.2 допускает дополнительные статусы (например,
// parked): новая константа расширяет Statuses и таблицу statusNames
// следующим кодом по порядку.
//
// # Автомат status домена: общая таблица решений (T-256, ТЗ-fsm §3 ФТ-2)
//
// Единая точка правды о переходах статуса: таблица «этап × условие →
// статус» по писателям штатного конвейера (административные пути —
// импорт fixtures стенда T-242, cmd/crawlerctl, переносит состояние
// дампа как есть, и ручной SQL — вне таблицы). Формализация не
// переписывает переходы на FSM-фреймворк — они остаются SQL-операторами
// с охранными предикатами (ТЗ-fsm §1); спецификация — ниже, писатели
// ссылаются на неё и не дублируют. Ветки этапов развёрнуты в
// пакетах-исполнителях: DNS — internal/dnsworker (result.go, у
// операторов записи), HTTP — internal/httpworker (record.go). Смежный
// автомат — check_state (internal/scheduler/checkstate.go, T-255):
// статус — факт последнего эпизода, check_state — состояние цикла;
// переходы той таблицы, несущие переходы статуса этой как побочное
// действие, — записи результатов и диагноз (Р2/Р3/Р4/Р13 = О2–О6, О10),
// ретрай (Р8 = О11), удаление/воскрешение/вставка (Р14/Р9/Р10 =
// О12/О13/О1).
//
//	| №  | Этап     | Условие                                        | Статус                | Побочные действия                                | Писатель |
//	|----|----------|------------------------------------------------|-----------------------|--------------------------------------------------|----------|
//	| О1 | ingest   | вставка нового домена add-файлом               | new (DEFAULT схемы)   | —                                                | ingest/apply_add.go insertAddSQL (T-018) |
//	| О2 | DNS      | апекс NXDOMAIN (RFC 8020)                      | not_delegated         | dns_mask = 0; temp_attempts = 0                  | dnsworker/result.go resultUpdateStatusSQL |
//	| О3 | DNS      | апекс SERVFAIL/таймаут после ретраев           | temp_error            | dns_mask = 0; temp_attempts сохраняет            | dnsworker/result.go resultUpdateStatusSQL |
//	| О4 | DNS      | адресов нет, сбоев резолвера нет (вкл. wildcard-only и дефекты зоны: цикл/глубина CNAME) | delegated_no_address | dns_mask эпизода; temp_attempts = 0 | dnsworker/result.go resultUpdateStatusSQL |
//	| О5 | DNS      | адресов нет, был сбой резолвера (транспорт / код вне NOERROR и NXDOMAIN) | temp_error | dns_mask эпизода; temp_attempts сохраняет | dnsworker/result.go resultUpdateStatusSQL |
//	| О6 | DNS      | адреса есть (маска после wildcard-фильтра непуста) | НЕ пишется: статус решает HTTP-этап (О7–О9); до его итога хранится прежний (первая проверка — new) | dns_mask эпизода; temp_attempts = 0; публикация q:http | dnsworker/result.go resultUpdateKeepStatusSQL |
//	| О7 | HTTP     | есть записи (хоть один субдомен×протокол ответил) | alive              | temp_attempts = 0; строки subdomain_results      | httpworker/record.go recordDomainSQL |
//	| О8 | HTTP     | записей нет, среди проверок есть таймаут-класс ErrTimeout (вкл. connect-таймаут) | temp_error | temp_attempts сохраняет | httpworker/record.go recordDomainSQL |
//	| О9 | HTTP     | записей нет, таймаутов нет (вкл. connect-refused — ErrNetwork) | no_http | temp_attempts = 0 | httpworker/record.go recordDomainSQL |
//	| О10 | DNS     | ядовитая строка: проверка невозможна детерминированно (пустое имя) | invalid (T-109) | temp_attempts = 0; перепроверка не возвращает | dnsworker/result.go resultInvalidSQL |
//	| О11 | scheduler | temp_error, окно temp_error_retry_after прошло, temp_attempts ≥ max_retries | delegated_no_address (АСИММЕТРИЯ А1) | temp_attempts = 0; check_state остаётся done | scheduler/retry.go retryFinalizeSQL (T-031) |
//	| О12 | ingest  | delete-файл: домен удалён из зарегистрированных | deleted             | строки результата чистит T-020; флаг render_pending снимает та же транзакция | ingest/apply_delete.go (T-019) |
//	| О13 | ingest  | воскрешение: deleted-домен в новом add-файле   | new                  | dns_mask/flags/last_checked_at сохраняются (факт прошлой проверки не обнуляется) | ingest/apply_add.go reviveAddSQL (T-018) |
//
// temp_attempts (T-031): сбрасывается в 0 на каждом не-temp_error итоге
// записи эпизода (О2, О4, О6, О7, О9, О10) и при финализации ретрая
// (О11); temp_error-итоги (О3, О5, О8) сохраняют накопленное —
// наращивает планировщик при назначении повтора (Р8 спецификации
// check_state); ingest-переходы (О12, О13) счётчик не трогают.
//
// ## Осознанные асимметрии (решения, НЕ дефекты — не «исправляются»)
//
//	А1  О11: temp_error при исчерпании попыток финализируется в
//	    delegated_no_address, а не в спец-статус: последняя проверка
//	    закончилась SERVFAIL/таймаутом — адресов для HTTP-этапа нет,
//	    это терминальная ветка «нет адресов» алгоритма ТЗ 7;
//	    not_delegated отвергнут (кодирует именно NXDOMAIN апекса,
//	    RFC 8020), новых статусов набор не расширяется (семь статусов
//	    подтверждены заказчиком, ТЗ 13.1). Ошибка самокорректируется
//	    регулярной перепроверкой T-030 (см. scheduler/retry.go).
//	А2  О8/О9: connect-refused → no_http против connect-таймаута →
//	    temp_error: RST на SYN (порт закрыт) — постоянное свойство
//	    хоста, повтор в ближнем горизонте бессмыслен (ErrNetwork, не
//	    temp); молчание SYN (connect-таймаут) — транзиент (ErrTimeout).
//	    Классифицирует httpworker/worker.go workerClassifyError по
//	    httpfetch.IsTimeout (net.Error.Timeout).
//	А3  HTTP-этап (О7–О9) пишет итог БЕЗ охраны check_state <> done
//	    (в отличие от DNS-операторов): повторная доставка обязана
//	    переиздать q:analyze, фильтр done отбросил бы публикацию
//	    навсегда (httpworker/record.go, комментарий файла). Плата —
//	    теоретическая гонка со свежей перепроверкой, окно практически
//	    недостижимо (попытки доставки исчерпываются за минуты, recheck
//	    измеряется месяцами; ранний recheck Р15 (T-270) сокращает
//	    «месяцы» до 24 ч — дубль задачи ожившего стоявшего воркера
//	    гасится той же идемпотентностью: перезапись эпизода
//	    DELETE+INSERT, переиздание q:analyze).
//
// ## Наблюдаемость аномалии «статус повис» (ФТ-2.2)
//
// О6 без О7–О9 (задача q:http потеряна после XADD) оставляет домен в
// status = new ∧ check_state = done: нормой состояние НЕ является
// (О6 обязан завершиться HTTP-итогом), наблюдается gauge
// crawler_domains_new_done_stale и алертом domains_new_done_stale
// (проход monitor/zonestats.go по счётчикам реконсиляции, без seq scan
// по domains). Лечит компенсатор T-270 — ранний recheck (Р15
// спецификации check_state, internal/scheduler/stalerequeue.go: часовой
// проход возвращает new∧done старше 24 ч в pending, дешёвая проба по
// строке new_done_stale — domains в стационаре не сканируются); домен
// не ждёт возрастной перепроверки T-030 (recheck_interval, месяцы).
// Gauge остаётся сигналом оператору о НОВЫХ потерях (класс открыт:
// рестарт Redis без персистентности — docs/redis.md §5, OOM-окна —
// docs/load-measurements.md §11.1).
type Status int16

// Статусы домена (ТЗ 6.2). Комментарии — значения из таблицы §6.2.
const (
	// StatusNew — ещё не проверялся.
	StatusNew Status = 0
	// StatusNotDelegated — апекс NXDOMAIN; субдомены не проверяются
	// (RFC 8020, ТЗ 5.4 п. 1).
	StatusNotDelegated Status = 1
	// StatusDelegatedNoAddress — делегирован, но ни у одного субдомена
	// нет A/AAAA (ТЗ 5.4 п. 5).
	StatusDelegatedNoAddress Status = 2
	// StatusNoHTTP — адреса есть, веб не отвечает ни по http, ни по https.
	StatusNoHTTP Status = 3
	// StatusAlive — хотя бы один субдомен ответил по HTTP(S).
	StatusAlive Status = 4
	// StatusTempError — временная ошибка (SERVFAIL, таймауты), требуется
	// повтор (ТЗ 5.3: temp_error_retry_after).
	StatusTempError Status = 5
	// StatusDeleted — удалён из зарегистрированных по файлу удаления
	// (ТЗ 5.1, 13.1).
	StatusDeleted Status = 6
	// StatusInvalid — диагностический терминальный статус ядовитой
	// строки (T-109, остаток DNS-03): проверка невозможна — имя пустое
	// или пробельное (мусорная/порченая строка domains), повтор эпизода
	// исход не изменит. Не факт о домене: ставится DNS-этапом вместо
	// вечного цикла queued→lease→dispatch; перепроверка (recheck) такие
	// строки не возвращает — выход один: ручной разбор (правка имени или
	// удаление строки). Код 7 — расширение таблицы §6.2, допущенное ТЗ:
	// прецедент «семь статусов» (ТЗ 13.1, retry.go T-031) осознанно
	// переигран: temp_error породил бы ложные повторы с финализацией в
	// delegated_no_address — обман о домене, а не диагностика.
	StatusInvalid Status = 7
)

// Statuses — все статусы по возрастанию кодов (итерация, вывод в CLI).
var Statuses = []Status{
	StatusNew,
	StatusNotDelegated,
	StatusDelegatedNoAddress,
	StatusNoHTTP,
	StatusAlive,
	StatusTempError,
	StatusDeleted,
	StatusInvalid,
}

// statusNames — имена статусов: 0..6 — ровно из таблицы ТЗ 6.2,
// 7 — диагностический invalid (T-109, расширение §6.2).
var statusNames = map[Status]string{
	StatusNew:                "new",
	StatusNotDelegated:       "not_delegated",
	StatusDelegatedNoAddress: "delegated_no_address",
	StatusNoHTTP:             "no_http",
	StatusAlive:              "alive",
	StatusTempError:          "temp_error",
	StatusDeleted:            "deleted",
	StatusInvalid:            "invalid",
}

// statusByName — обратное соответствие для ParseStatus.
var statusByName = func() map[string]Status {
	m := make(map[string]Status, len(statusNames))
	for s, name := range statusNames {
		m[name] = s
	}
	return m
}()

// String возвращает имя статуса из ТЗ 6.2 ("new", "not_delegated", …);
// для кода вне таблицы — "Status(<код>)".
func (s Status) String() string {
	if name, ok := statusNames[s]; ok {
		return name
	}
	return fmt.Sprintf("Status(%d)", int16(s))
}

// Valid сообщает, что код входит в таблицу статусов ТЗ 6.2.
func (s Status) Valid() bool {
	_, ok := statusNames[s]
	return ok
}

// ParseStatus разбирает имя статуса (ТЗ 6.2) в код: "new", "alive", …
// Регистр и пробелы по краям не важны; неизвестное имя — ошибка со списком
// допустимых.
func ParseStatus(name string) (Status, error) {
	s, ok := statusByName[strings.ToLower(strings.TrimSpace(name))]
	if !ok {
		return 0, fmt.Errorf("model: неизвестный статус домена %q (ТЗ 6.2: %s)", name, strings.Join(statusNamesList(), ", "))
	}
	return s, nil
}

// statusNamesList — имена по порядку кодов (сообщения об ошибках).
func statusNamesList() []string {
	names := make([]string, len(Statuses))
	for i, s := range Statuses {
		names[i] = statusNames[s]
	}
	return names
}

// CheckState — состояние проверки домена, колонка domains.check_state
// (smallint, ТЗ 6.1): 0 pending, 1 queued, 2 done. Жизненный цикл задачи
// планировщика (ТЗ 5.3); смены состояний выполняет планировщик — здесь
// только значения.
type CheckState int16

const (
	// CheckStatePending — ожидает выборки планировщиком (частичный индекс
	// domains_pending_idx по check_state = 0).
	CheckStatePending CheckState = 0
	// CheckStateQueued — взят в работу, действует lease (queued_at).
	CheckStateQueued CheckState = 1
	// CheckStateDone — DNS/HTTP-этап завершён (ТЗ 7); рендер и анализ
	// дополняют результат асинхронно.
	CheckStateDone CheckState = 2
)

// String возвращает имя состояния ("pending", "queued", "done").
func (cs CheckState) String() string {
	switch cs {
	case CheckStatePending:
		return "pending"
	case CheckStateQueued:
		return "queued"
	case CheckStateDone:
		return "done"
	default:
		return fmt.Sprintf("CheckState(%d)", int16(cs))
	}
}

// Valid сообщает, что код — 0, 1 или 2.
func (cs CheckState) Valid() bool {
	return cs >= CheckStatePending && cs <= CheckStateDone
}

// Variant — вариант записи результата, колонка subdomain_results.variant
// (smallint, ТЗ 6.1): 0 основной, 1 http-вариант (если отличается).
// Логика выбора — ТЗ 5.5 п. 5.
type Variant int16

const (
	// VariantMain — основной вариант: оба протокола ведут на один сайт
	// либо отвечает только один протокол (какой именно — колонка scheme).
	VariantMain Variant = 0
	// VariantHTTP — http-вариант: вторая запись, когда http и https ведут
	// на разные сайты (variant 0 — https-сайт).
	VariantHTTP Variant = 1
)

// String возвращает имя варианта ("main", "http").
func (v Variant) String() string {
	switch v {
	case VariantMain:
		return "main"
	case VariantHTTP:
		return "http"
	default:
		return fmt.Sprintf("Variant(%d)", int16(v))
	}
}

// Valid сообщает, что код — 0 или 1.
func (v Variant) Valid() bool {
	return v == VariantMain || v == VariantHTTP
}

// Scheme — протокол записи результата, колонка subdomain_results.scheme
// (smallint, ТЗ 6.1): 1 http, 2 https.
type Scheme int16

const (
	// SchemeHTTP — запись получена по http.
	SchemeHTTP Scheme = 1
	// SchemeHTTPS — запись получена по https.
	SchemeHTTPS Scheme = 2
)

// String возвращает имя протокола ("http", "https").
func (s Scheme) String() string {
	switch s {
	case SchemeHTTP:
		return "http"
	case SchemeHTTPS:
		return "https"
	default:
		return fmt.Sprintf("Scheme(%d)", int16(s))
	}
}

// Valid сообщает, что код — 1 или 2.
func (s Scheme) Valid() bool {
	return s == SchemeHTTP || s == SchemeHTTPS
}

// ParseScheme разбирает имя протокола ("http", "https"). Регистр и
// пробелы по краям не важны.
func ParseScheme(name string) (Scheme, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "http":
		return SchemeHTTP, nil
	case "https":
		return SchemeHTTPS, nil
	default:
		return 0, fmt.Errorf("model: неизвестный протокол %q (допустимы http, https)", name)
	}
}

// AliasKind — связь субдомена с другим субдоменом того же домена,
// колонка subdomain_results.alias_kind (smallint, ТЗ 6.1): 0 нет,
// 1 идентичен, 2 редирект. Правила определения — ТЗ 5.5 п. 6.
type AliasKind int16

const (
	// AliasNone — независимый результат, алиаса нет.
	AliasNone AliasKind = 0
	// AliasIdentical — идентичен другому субдомену: совпадает итоговый URL
	// или хэш контента; контент хранится один раз.
	AliasIdentical AliasKind = 1
	// AliasRedirect — редирект на другой субдомен этого домена.
	AliasRedirect AliasKind = 2
)

// String возвращает имя связи ("none", "identical", "redirect").
func (a AliasKind) String() string {
	switch a {
	case AliasNone:
		return "none"
	case AliasIdentical:
		return "identical"
	case AliasRedirect:
		return "redirect"
	default:
		return fmt.Sprintf("AliasKind(%d)", int16(a))
	}
}

// Valid сообщает, что код — 0, 1 или 2.
func (a AliasKind) Valid() bool {
	return a >= AliasNone && a <= AliasRedirect
}

// Action — действие входного файла, колонка ingest_files.action
// (smallint, ТЗ 6.1): 1 add, 2 delete (ТЗ 5.1: путь <зона>/add|delete/).
type Action int16

const (
	// ActionAdd — файл добавления доменов.
	ActionAdd Action = 1
	// ActionDelete — файл удаления доменов.
	ActionDelete Action = 2
)

// String возвращает имя действия ("add", "delete").
func (a Action) String() string {
	switch a {
	case ActionAdd:
		return "add"
	case ActionDelete:
		return "delete"
	default:
		return fmt.Sprintf("Action(%d)", int16(a))
	}
}

// Valid сообщает, что код — 1 или 2.
func (a Action) Valid() bool {
	return a == ActionAdd || a == ActionDelete
}

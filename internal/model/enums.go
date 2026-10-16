package model

import (
	"fmt"
	"strings"
)

// Status — статус домена, колонка domains.status (smallint, ТЗ 6.2).
// Коды 0..6 зафиксированы схемой и тестом. ТЗ 6.2 допускает дополнительные
// статусы (например, parked): новая константа расширяет Statuses и таблицу
// statusNames следующим кодом по порядку.
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
}

// statusNames — имена статусов ровно из таблицы ТЗ 6.2.
var statusNames = map[Status]string{
	StatusNew:                "new",
	StatusNotDelegated:       "not_delegated",
	StatusDelegatedNoAddress: "delegated_no_address",
	StatusNoHTTP:             "no_http",
	StatusAlive:              "alive",
	StatusTempError:          "temp_error",
	StatusDeleted:            "deleted",
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

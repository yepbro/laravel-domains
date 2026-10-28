// Package idn — нормализация доменных имён (ТЗ 5.2 основного ТЗ).
//
// Правило ТЗ 5.2: внутри системы везде хранится ASCII-форма (punycode,
// xn--…); unicode-форма — производная для отображения. Конвейер
// Normalize:
//
//   - нижний регистр, без пробелов и завершающей точки (п. 1);
//   - punycode через профиль Lookup (UTS-46) из golang.org/x/net/idna
//     (пп. 2–3): unicode-метки кодируются, метки xn-- проверяются
//     декодированием, невалидные отклоняются;
//   - длины в ASCII-форме: метка ≤ 63, полное имя ≤ 253 (п. 4);
//   - отделение зоны по последней метке имени — TLD (п. 5 в редакции
//     ТЗТ tld-only-zones): составные регистрации (co.uk, 5g.in) зоной
//     не выделяются — всё до TLD становится меткой имени;
//   - числовая зона отвергается: TLD из одних цифр не существует (IANA),
//     иначе через вывод зоны прошли бы даты выгрузок и IPv4-литералы;
//   - признак is_idn — метка имени содержит xn-- (п. 9): бит 0
//     domains.flags (model.FlagIDN, ТЗ 5.2 п. 9).
//
// Ошибки типизированы (*Error: класс, имя, причина): rejected-лог ingest
// (ТЗ 5.1, задача T-016) использует класс как причину отклонения строки.
//
// Пакет не занимается ни приёмом файлов (этап ingest, T-013…T-021), ни
// извлечением хостов из Location/meta refresh (T-047/T-048): сюда
// приходят уже выделенные имена и хосты.
package idn

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	"golang.org/x/net/idna"
)

// Лимиты DNS-имён (ТЗ 5.2 п. 4) и префикс A-метки (RFC 5890).
const (
	maxLabelLen = 63  // метка ≤ 63 октет в ASCII-форме
	maxNameLen  = 253 // полное имя ≤ 253 октет в ASCII-форме
	xnPrefix    = "xn--"
)

// ErrorKind — класс ошибки нормализации: устойчивая причина отклонения
// для rejected-лога (ТЗ 5.1) и вывода crawlerctl.
type ErrorKind string

const (
	// ErrEmpty — имя пусто (после уборки пробелов и завершающих точек).
	ErrEmpty ErrorKind = "empty"
	// ErrInvalid — имя не проходит UTS-46/IDNA или содержит пустую метку
	// (в том числе невалидный xn--: ТЗ 5.2 п. 3).
	ErrInvalid ErrorKind = "invalid"
	// ErrLabelLen — метка длиннее 63 октет (ASCII-форма).
	ErrLabelLen ErrorKind = "label_len"
	// ErrNameLen — имя длиннее 253 октет (ASCII-форма).
	ErrNameLen ErrorKind = "name_len"
	// ErrNoLabel — нет метки перед зоной: имя само — TLD (com, москва),
	// домена из него не получается.
	ErrNoLabel ErrorKind = "no_label"
	// ErrNumericZone — последняя метка имени (зона) состоит только из
	// цифр: числовых TLD не существует (IANA), а без этой проверки
	// через вывод зоны проходят даты выгрузок («18.06.1999» → зона
	// 1999) и IPv4-литералы («1.2.3.4» → зона 4).
	ErrNumericZone ErrorKind = "numeric_zone"
)

// kindDescriptions — тексты классов для Error() и String().
var kindDescriptions = map[ErrorKind]string{
	ErrEmpty:       "пустое имя",
	ErrInvalid:     "невалидное имя домена",
	ErrLabelLen:    "метка длиннее 63 символов",
	ErrNameLen:     "имя длиннее 253 символов",
	ErrNoLabel:     "нет метки перед зоной",
	ErrNumericZone: "числовая зона — таких TLD не существует",
}

// String возвращает текст класса (стиль model.Status.String).
func (k ErrorKind) String() string {
	if d, ok := kindDescriptions[k]; ok {
		return d
	}
	return string(k)
}

// Error — ошибка нормализации: класс, исходное имя и причина. Имя
// обрезается до maxErrNameLen рун: вход недоверенный и может быть
// сколь угодно длинным, а rejected-логу длинный мусор не нужен.
type Error struct {
	Kind ErrorKind // класс — причина отклонения
	Name string    // исходное имя (обрезанное)
	Err  error     // причина (например, ошибка idna); может быть nil
}

// maxErrNameLen — сколько рун исходного имени хранить в ошибке.
const maxErrNameLen = 255

func newErr(kind ErrorKind, name string, cause error) *Error {
	if runes := []rune(name); len(runes) > maxErrNameLen {
		name = string(runes[:maxErrNameLen]) + "…"
	}
	return &Error{Kind: kind, Name: name, Err: cause}
}

func (e *Error) Error() string {
	desc := e.Kind.String()
	if e.Err != nil {
		return fmt.Sprintf("idn: %s: %q: %v", desc, e.Name, e.Err)
	}
	return fmt.Sprintf("idn: %s: %q", desc, e.Name)
}

// Unwrap даёт доступ к причине (errors.Is/As по цепочке).
func (e *Error) Unwrap() error { return e.Err }

// KindOf возвращает класс ошибки нормализации; ok = false, если err —
// не *Error (чужая ошибка).
func KindOf(err error) (ErrorKind, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind, true
	}
	return "", false
}

// Result — нормализованное доменное имя.
type Result struct {
	// ASCII — полное имя в ASCII-форме (punycode), нижний регистр, без
	// завершающей точки. Так имя хранится в системе (ТЗ 5.2).
	ASCII string
	// Unicode — unicode-форма для отображения (ТЗ 5.2 п. 7): ToUnicode
	// на лету; для чисто ASCII-имени совпадает с ASCII.
	Unicode string
	// Zone — зона имени: TLD, последняя метка ASCII-формы (п. 5 в
	// редакции ТЗТ tld-only-zones). Составные суффиксы (co.uk,
	// github.io, 5g.in) зоной не выделяются: у example.co.uk зона uk,
	// у example.5g.in — in. Зона incoming-каталога при проверке строк
	// задаётся явно (T-014/T-016) и может быть глубже TLD (тестовые
	// зоны); Zone — способ отделить зону, когда она не известна
	// заранее (зона-less файлы, вывод).
	Zone string
	// Label — имя без зоны (колонка domains.label): левая часть до Zone.
	// Составной label — норма для зон с регистрацией на втором уровне:
	// example.5g.in → label example.5g (зона in), example.co.uk →
	// example.co (зона uk).
	Label string
	// IsIDN — метка имени содержит xn-- (ТЗ 5.2 п. 9): бит 0
	// domains.flags (model.FlagIDN). Проверяется метка домена, а не
	// полное имя: example.рф (зона xn--p1ai) бита не получает — у самой
	// метки нет punycode; всей зоне бит не принадлежит.
	IsIDN bool
}

// Normalize приводит полное доменное имя к ASCII-форме и раскладывает
// его на зону и метку (ТЗ 5.2 пп. 1–5, 9). Так нормализуются строки
// файлов incoming (ТЗ 5.1) и ввод оператора в crawlerctl (T-081).
func Normalize(name string) (Result, error) {
	ascii, err := normalizeASCII(name)
	if err != nil {
		return Result{}, err
	}
	// п. 5 (в редакции ТЗТ tld-only-zones): зона — последняя метка
	// имени (TLD): у example.5g.in зона in, метка example.5g; точек
	// нет — имя само TLD, метки перед зоной нет. Составные суффиксы
	// (co.uk, github.io) зоной не выделяются — всё до TLD метка.
	i := strings.LastIndexByte(ascii, '.')
	zone := ascii[i+1:] // i < 0 → зона — всё имя (голый TLD), случай ниже
	// Числовая зона — не домен: даты выгрузок («18.06.1999») и
	// IPv4-литералы («1.2.3.4») проходят UTS-46, а числовых TLD не
	// существует (IANA) — последняя метка из цифр (1999, 4) зоной
	// быть не может; проверка идёт до ErrNoLabel, чтобы голая числовая
	// метка («1999») классифицировалась числовой зоной, как раньше.
	// Числовая метка в буквенной зоне (123.com) — обычное имя.
	if isNumericZone(zone) {
		return Result{}, newErr(ErrNumericZone, name, nil)
	}
	if i < 0 {
		return Result{}, newErr(ErrNoLabel, name, nil)
	}
	label := ascii[:i]
	// zone == "" и label == "" недостижимы: normalizeASCII гарантирует
	// непустой ASCII без пустых меток и завершающих точек — точка не
	// в начале и не в конце имени, метки с двух сторон непусты. Ветка —
	// защита от будущих изменений конвейера, оба мутанта условия
	// эквивалентны, аннотация глушит строку целиком: trade-off
	// гранулярности, docs/mutation-testing.md §4.
	// mutator-disable-next-line expression/remove
	if zone == "" || label == "" {
		return Result{}, newErr(ErrNoLabel, name, nil)
	}
	return Result{
		ASCII:   ascii,
		Unicode: ToUnicode(ascii),
		Zone:    zone,
		Label:   label,
		IsIDN:   HasXNLabel(label),
	}, nil
}

// isNumericZone — все метки зоны состоят только из цифр ASCII: числовых
// TLD не существует (IANA), поэтому последняя метка из цифр — не зона
// имени: даты выгрузок («18.06.1999» — последняя метка 1999) и
// IPv4-литералы («1.2.3.4» — 4) обязаны отвергаться выводом зоны. Зона —
// одна метка (TLD), цикл по точкам оставлен для прямых вызовов и
// страховки. Числовая метка в буквенной зоне (123.com) зоной не
// является и имени не мешает.
func isNumericZone(zone string) bool {
	if zone == "" {
		return false
	}
	for _, label := range strings.Split(zone, ".") {
		if strings.Trim(label, "0123456789") != "" {
			return false
		}
	}
	return true
}

// NormalizeZone нормализует имя зоны — каталог incoming/<зона> (ТЗ 5.1)
// тем же конвейером, что и домен (ТЗ 5.2 п. 6: «зона тоже нормализуется
// тем же способом»), но без отделения метки. Зона каталога не обязана
// быть TLD (тестовые зоны вроде crawl.test, составные пути) —
// принадлежность зоны никакому списку здесь не проверяется, только
// синтаксис имени. Unicode-форма для zones.name_unicode — ToUnicode
// зоны на лету.
func NormalizeZone(zone string) (string, error) {
	return normalizeASCII(zone)
}

// HostToASCII приводит хост к ASCII-форме (ТЗ 5.2 п. 8): хосты из
// Location и <meta http-equiv="refresh">, содержащие unicode,
// кодируются в punycode перед DNS-запросом, Host и SNI (для DNS и SNI
// всегда используется ASCII-форма). ASCII-хосты валидируются и
// возвращаются в нижнем регистре; IPv4-литерал проходит как есть
// (метки из цифр синтаксически валидны), IPv6-литералы не поддержаны —
// это не имена, вызывающая сторона (T-047/T-048) обрабатывает их
// отдельно через net.ParseIP. Хост передаётся без схемы, порта и пути
// (url.URL.Hostname); сам перехват Location/meta refresh — задачи
// T-047/T-048.
func HostToASCII(host string) (string, error) {
	return normalizeASCII(host)
}

// normalizeASCII — общий конвейер Normalize/NormalizeZone/HostToASCII:
// очистка (п. 1), punycode и проверка xn-- (пп. 2–3), длины (п. 4).
func normalizeASCII(name string) (string, error) {
	s := cleanup(name)
	if s == "" {
		return "", newErr(ErrEmpty, name, nil)
	}
	// Профиль Lookup (UTS-46): маппинг регистра, кодирование unicode-меток
	// в punycode, проверка A-меток xn-- декодированием, STD3, Bidi и
	// контекстные правила (ТЗ 5.2 пп. 2–3).
	ascii, err := idna.Lookup.ToASCII(s)
	if err != nil {
		return "", newErr(ErrInvalid, name, err)
	}
	// idna.Lookup не отвергает пустые метки («a..b» проходит, «xn--»
	// декодируется в пустоту — поведение golang.org/x/net v0.61) и не
	// проверяет длины — контролируем сами (ТЗ 5.2 пп. 3–4); поведение
	// закреплено тестами (TestNormalizeInvalid), здесь — страховка.
	// Ветка достижима входом «xn--» (Lookup декодирует его в пустоту,
	// ToASCII("xn--") = "", nil) и входом из полностью игнорируемых
	// маппингом символов (BOM/U+00AD/U+200B/U+2060): в обоих случаях
	// ascii == "" и Split("", ".") ниже даёт [""] — следующая проверка
	// отвечает той же ошибкой «пустая метка» с теми же Kind/Name/
	// причиной, мутант вырезания return эквивалентен.
	// mutator-disable-next-line branch/if
	if ascii == "" {
		return "", newErr(ErrInvalid, name, errors.New("пустая метка"))
	}
	for _, label := range strings.Split(ascii, ".") {
		if label == "" {
			return "", newErr(ErrInvalid, name, errors.New("пустая метка"))
		}
		if len(label) > maxLabelLen {
			return "", newErr(ErrLabelLen, name, nil)
		}
	}
	if len(ascii) > maxNameLen {
		return "", newErr(ErrNameLen, name, nil)
	}
	return ascii, nil
}

// cleanup выполняет ТЗ 5.2 п. 1: нижний регистр, без пробельных
// символов и без завершающих точек. Пробелы убираются все, не только
// по краям («убрать пробелы» без уточнения в ТЗ): помарки ввода
// («при мер.рф») не должны отправлять имя в rejected-лог. Скрытые
// символы — не пробелы и остаются на усмотрение UTS-46: BOM (U+FEFF),
// U+00AD, U+200B и U+2060 убираются маппингом idna.Lookup (ignored),
// ZWNJ/ZWJ (U+200C/U+200D) проходят только по контекстным правилам
// (CONTEXTJ), U+180E — disallowed; корпус — corpus_test.go (T-010).
func cleanup(name string) string {
	// Быстрый путь — чистая оптимизация: IndexFunc для имени без пробелов
	// отвечает -1, «< 0 → < -1» переводит безпробельное имя на медленный
	// путь, который фильтрует только пробелы (их нет) и применяет тот же
	// TrimRight(ToLower) — результат неотличим; вырезание return — то же
	// самое в обратную сторону (branch/if ключуется строкой if). Grow —
	// подсказка ёмкости, на результате не сказывается. Три эквивалентных
	// мутанта.
	// mutator-disable-next-line numbers/decrementer, branch/if
	if strings.IndexFunc(name, unicode.IsSpace) < 0 {
		return strings.TrimRight(strings.ToLower(name), ".")
	}
	var b strings.Builder
	// mutator-disable-next-line statement/remove
	b.Grow(len(name))
	for _, r := range name {
		if !unicode.IsSpace(r) {
			b.WriteRune(r)
		}
	}
	return strings.TrimRight(strings.ToLower(b.String()), ".")
}

// ToUnicode возвращает unicode-форму ASCII-имени для отображения
// (ТЗ 5.2 п. 7: поиск и вывод unicode-имён домена — idna.ToUnicode на
// лету, без отдельного столбца в domains). Ошибок не возвращает
// намеренно: если декодировать не удалось (имя не в punycode),
// возвращается вход без изменений.
func ToUnicode(ascii string) string {
	// Ошибка декодирования и пустой результат — один исход: вернуть вход
	// (idna.ToUnicode("") даёт пустую строку без ошибки).
	if u, err := idna.ToUnicode(ascii); err == nil && u != "" {
		return u
	}
	return ascii
}

// HasXNLabel сообщает, содержит ли имя хотя бы одну метку xn--
// (A-метку punycode, ТЗ 5.2 п. 9). Имя — ASCII-форма с точками-
// разделителями; регистр не важен. Normalize использует функцию для
// метки домена (Result.IsIDN), но она применима к любому имени —
// например к зоне (zones.name).
func HasXNLabel(name string) bool {
	for _, label := range strings.Split(name, ".") {
		if len(label) >= len(xnPrefix) && strings.EqualFold(label[:len(xnPrefix)], xnPrefix) {
			return true
		}
	}
	return false
}

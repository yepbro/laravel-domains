// Package ingest — приём файлов доменов (ТЗ 5.1). T-016: валидация строк
// файла относительно зоны, указанной в пути (<зона>/add|delete/*.txt).
//
// Строка обязана быть полным именем домена из зоны пути: имя нормализуется
// конвейером internal/idn (ТЗ 5.2: нижний регистр, punycode, длины), затем
// зона результата сверяется с зоной пути (ТЗ 5.2 п. 6). Невалидные имена
// и слишком длинные отклоняются с типизированной причиной (класс idn.Error)
// и пишутся в отдельный лог rejected (ТЗ 5.1: файл, номер строки, обрезанная
// строка, причина); единичные плохие строки не делают файл ошибочным — его
// роняет только доля ошибок выше ingest.max_error_ratio.
//
// Парсинг файла (пустые строки, комментарии, CRLF/BOM) — T-015; разбор пути
// и нормализация каталога зоны — T-014; загрузка валидных строк в БД — T-017;
// перемещение в processed/failed и отчёт ingest_files — T-021.
package ingest

import (
	"fmt"
	"io"
	"log/slog"
	"strings"

	"crawler/internal/idn"
)

// DefaultMaxErrorRatio — порог доли отклонённых строк файла по умолчанию
// (ingest.max_error_ratio). Дублирует config.Default, чтобы пакет работал
// и без конфигурации. 10%: реальные файлы зон содержат единичный мусор
// (опечатки, обрезанные записи), который по ТЗ 5.1 файл ронять не должен,
// а файл не в том каталоге зоны или битый дамп дают долю ошибок, близкую
// к единице, — их отсекает и десять процентов.
const DefaultMaxErrorRatio = 0.1

// Причины отклонения строки. Тип Reason общий для пакета (объявлен в
// parser.go): лог rejected и отчёт файла (ТЗ 5.1). Классы ошибок
// нормализации совпадают с idn.ErrorKind (T-016 только отображает их
// наружу); wrong_zone — собственная причина ingest: домен принадлежит
// другой зоне, чем каталог файла (ТЗ 5.2 п. 6).
const (
	// ReasonEmpty — имя пусто после уборки пробелов и завершающих точек.
	ReasonEmpty Reason = "empty"
	// ReasonInvalid — имя не проходит UTS-46/IDNA (в том числе битый xn--).
	ReasonInvalid Reason = "invalid"
	// ReasonLabelLen — метка длиннее 63 октет в ASCII-форме.
	ReasonLabelLen Reason = "label_len"
	// ReasonNameLen — имя длиннее 253 октет в ASCII-форме.
	ReasonNameLen Reason = "name_len"
	// ReasonNoLabel — метки перед зоной нет: имя само — зона.
	ReasonNoLabel Reason = "no_label"
	// ReasonWrongZone — домен не из зоны пути (ТЗ 5.2 п. 6).
	ReasonWrongZone Reason = "wrong_zone"
)

// reasonDescriptions — тексты причин для String (стиль idn.ErrorKind).
var reasonDescriptions = map[Reason]string{
	ReasonEmpty:     "пустое имя",
	ReasonInvalid:   "невалидное имя домена",
	ReasonLabelLen:  "метка длиннее 63 символов",
	ReasonNameLen:   "имя длиннее 253 символов",
	ReasonNoLabel:   "нет метки перед зоной",
	ReasonWrongZone: "домен не из зоны пути",
}

// String возвращает текст причины (стиль model.Status.String).
func (r Reason) String() string {
	if d, ok := reasonDescriptions[r]; ok {
		return d
	}
	return string(r)
}

// Entry — валидная строка файла: домен зоны пути в форме для загрузки
// в БД (T-017): zone_id ищется по Zone, Label — колонка domains.label,
// IsIDN — бит is_idn в domains.flags (model.FlagIDN).
type Entry struct {
	// Zone — зона пути в ASCII-форме (общая для всех строк файла).
	Zone string
	// Label — метка домена: имя без зоны; может быть составной
	// («магазин.рф.co.uk» в зоне co.uk — контракт internal/idn,
	// корпус T-010).
	Label string
	// IsIDN — метка содержит xn-- (ТЗ 5.2 п. 9; HasXNLabel по метке).
	IsIDN bool
}

// ValidatorOptions — параметры валидации одного файла.
type ValidatorOptions struct {
	// File — путь файла: атрибут каждой записи лога rejected.
	File string
	// Zone — зона из пути файла (incoming/<зона>/add|delete, T-014).
	// Уже нормализована в ASCII (T-014); unicode-форма или верхний
	// регистр не ошибочны — нормализуется ещё раз (идемпотентно).
	Zone string
	// MaxErrorRatio — порог доли отклонённых строк: при превышении файл
	// целиком считается ошибочным (ingest.max_error_ratio). 0 — дефолт
	// DefaultMaxErrorRatio; допустимо (0, 1], как и в config.Validate.
	MaxErrorRatio float64
	// RejectLog — логгер rejected (NewRejectedLogger); nil — не писать
	// записи (счётчики ведутся всё равно).
	RejectLog *slog.Logger
}

// Validator — валидация строк одного файла зоны: проверяет строки,
// пишет отклонённые в лог rejected и ведёт счётчики для решения
// «филет целиком ошибка или нет» (порог доли ошибок, ТЗ 5.1).
type Validator struct {
	file     string
	zone     string
	ratio    float64
	log      *slog.Logger
	total    int
	accepted int
	reasons  map[Reason]int
}

// NewValidator подготавливает валидацию файла: зона нормализуется тем же
// конвейером, что и строки (ТЗ 5.2 п. 6: «зона тоже нормализуется тем же
// способом»), поэтому каталог зоны в unicode и punycode даёт одну зону.
func NewValidator(opts ValidatorOptions) (*Validator, error) {
	zone, err := idn.NormalizeZone(opts.Zone)
	if err != nil {
		return nil, fmt.Errorf("ingest: зона файла %s: %w", opts.File, err)
	}
	ratio := opts.MaxErrorRatio
	if ratio == 0 {
		ratio = DefaultMaxErrorRatio
	}
	if ratio < 0 || ratio > 1 {
		return nil, fmt.Errorf("ingest: порог доли ошибок %v вне диапазона (0, 1]", ratio)
	}
	log := opts.RejectLog
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Validator{
		file:    opts.File,
		zone:    zone,
		ratio:   ratio,
		log:     log,
		reasons: make(map[Reason]int),
	}, nil
}

// Line проверяет очередную содержательную строку файла: n — номер строки
// в файле для лога rejected (пустые строки и комментарии # исключает
// парсер T-015, они сюда не попадают). Валидная строка — домен зоны пути
// (ok = true, причина пуста); невалидная — причина отклонения: строка
// пишется в лог rejected и учитывается в счётчиках.
func (v *Validator) Line(n int, line string) (Entry, Reason, bool) {
	v.total++
	res, err := idn.Normalize(line)
	if err != nil {
		reason := ReasonInvalid
		if kind, ok := idn.KindOf(err); ok {
			reason = Reason(kind)
		}
		v.reject(n, line, reason)
		return Entry{}, reason, false
	}
	label, reason := domainLabel(res, v.zone)
	if reason != "" {
		v.reject(n, line, reason)
		return Entry{}, reason, false
	}
	v.accepted++
	return Entry{Zone: v.zone, Label: label, IsIDN: idn.HasXNLabel(label)}, "", true
}

// domainLabel выделяет метку домена зоны пути из нормализованного имени
// (ТЗ 5.2 пп. 5–6) и возвращает пустую причину, если имя — полное имя
// домена из зоны zone, либо причину отклонения.
//
// Зона пути задаётся каталогом файла явно, поэтому сравнивается не только
// с публичным суффиксом имени (res.Zone — способ отделения зоны по списку
// publicsuffix, ТЗ 5.2 п. 5: составные зоны и частные суффиксы не режутся
// по последней точке), но и с более глубокой зоной, которой в списке нет
// (тестовые зоны вроде crawl.test, T-014: у example.crawl.test суффикс
// по списку — test, а зона пути — crawl.test). Обратный случай — суффикс
// глубже зоны пути (example.co.uk при зоне uk): домен зарегистрирован во
// вложенной публичной зоне, простое разбиение по точке запрещено п. 5 —
// отклонение.
func domainLabel(res idn.Result, zone string) (string, Reason) {
	if res.Zone == zone {
		// res.Label уже отделён по publicsuffix; составные метки
		// допустимы — контракт internal/idn (корпус T-010).
		return res.Label, ""
	}
	// Само имя зоны: метки перед зоной пути нет (для PSL-зон это
	// раньше ловит idn.Normalize как ErrNoLabel — здесь не-PSL зоны).
	if res.ASCII == zone {
		return "", ReasonNoLabel
	}
	// Имя обязано оканчиваться на зону пути, а зона пути — быть не
	// мельче публичного суффикса имени (см. комментарий функции).
	suffix := "." + zone
	if !strings.HasSuffix(res.ASCII, suffix) || !strings.HasSuffix(zone, "."+res.Zone) {
		return "", ReasonWrongZone
	}
	return strings.TrimSuffix(res.ASCII, suffix), ""
}

// reject учитывает отклонённую строку и пишет её в лог rejected (ТЗ 5.1):
// файл, номер строки, обрезанная строка и причина.
func (v *Validator) reject(n int, line string, reason Reason) {
	v.reasons[reason]++
	v.log.Warn("строка отклонена",
		"file", v.file,
		"line", n,
		"text", truncate(line),
		"reason", string(reason),
	)
}

// ValidatorStats — счётчики строк файла (отчёт ingest_files, ТЗ 5.1, T-021).
type ValidatorStats struct {
	// Total — содержательных строк файла (без пустых и комментариев —
	// их исключает парсер T-015).
	Total int
	// Accepted — валидных имён доменов зоны.
	Accepted int
	// Rejected — отклонённых строк (Total − Accepted).
	Rejected int
	// Reasons — отклонения в разрезе причин.
	Reasons map[Reason]int
}

// FileResult — итог валидации файла.
type FileResult struct {
	ValidatorStats
	// Failed — доля отклонённых строк превысила ingest.max_error_ratio:
	// файл целиком считается ошибочным (ТЗ 5.1), T-021 перемещает его
	// в failed/ с лог-отчётом; валидные строки такого файла не грузятся.
	Failed bool
}

// Finish завершает валидацию файла и возвращает итог: счётчики и вердикт
// по порогу доли ошибок. Порог сравнивается строго: доля равная порогу
// файл не роняет («единичные плохие строки», ТЗ 5.1); файл без
// содержательных строк ошибочным не считается.
func (v *Validator) Finish() FileResult {
	rejected := v.total - v.accepted
	return FileResult{
		ValidatorStats: ValidatorStats{
			Total:    v.total,
			Accepted: v.accepted,
			Rejected: rejected,
			Reasons:  v.reasons,
		},
		Failed: v.total > 0 && float64(rejected)/float64(v.total) > v.ratio,
	}
}

// maxLoggedRunes — сколько рун строки оставлять в записях лога rejected:
// вход недоверенный, строка может быть сколь угодно длинной (стиль
// обрезки idn.Error).
const maxLoggedRunes = 255

// truncate обрезает строку до maxLoggedRunes рун с многоточием.
func truncate(s string) string {
	if runes := []rune(s); len(runes) > maxLoggedRunes {
		return string(runes[:maxLoggedRunes]) + "…"
	}
	return s
}

// NewRejectedLogger — логгер rejected: отдельный поток записей об
// отклонённых строках (ТЗ 5.1: «отклонённые строки пишутся в лог rejected
// с причиной»). Формат — стиль internal/service (T-012): текстовый
// обработчик slog на w, атрибут service в каждой записи (значение — имя
// лога «rejected»). Куда писать (в тот же stderr, что и лог сервиса, или
// отдельный файл) и уровень — решает вызывающая сторона (cmd/ingest,
// T-017+); сами записи формирует Validator.reject.
func NewRejectedLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: level})).
		With("service", "rejected")
}

// Package ingest — приём файлов доменов из наблюдаемого каталога incoming
// (ТЗ 5.1 основного ТЗ).
//
// Конвейер приёма (разбор пути → построчный парсинг → валидация строк →
// загрузка и применение) собирается задачами T-014…T-021; T-014 вводит
// первые ступени:
//
//   - ParsePath (path.go): разбор пути готового файла относительно incoming
//     по схеме <зона>/add|delete/*.txt — нормализованная зона и действие
//     файла; имя каталога зоны в unicode или punycode нормализуется в одну
//     зону — дублей зон в двух формах не возникает (ТЗ 5.1, 11.4 п. 1);
//   - EnsureZone (zones.go): зона ищется по punycode-имени, отсутствующая
//     создаётся автоматически при первом файле с is_active = true (ТЗ 5.1).
//
// Готовые файлы выдаёт наблюдатель internal/watch (T-013) абсолютными
// путями; приём работает с путём относительно incoming (разделитель «/»,
// как в filepath.Rel) — его вычислит сборка конвейера ingest, и он же
// пишется в журнал ingest_files (T-021).
package ingest

import (
	"errors"
	"fmt"
	"path"
	"strings"

	"crawler/internal/idn"
	"crawler/internal/model"
)

// PathErrorKind — класс ошибки разбора пути входного файла: устойчивая
// причина для отчёта об ошибке файла (failed/, ТЗ 5.1 — T-021).
type PathErrorKind string

const (
	// ErrPathStructure — путь не вида <зона>/add|delete/*.txt: нет каталога
	// add|delete (или он не в нижнем регистре), лишняя вложенность, абсолютный
	// путь либо выход за пределы incoming.
	ErrPathStructure PathErrorKind = "structure"
	// ErrPathNotTXT — имя файла не *.txt: по ТЗ 5.1 готовность файла —
	// появление имени *.txt (временные *.tmp/*.part наблюдатель не выдаёт).
	ErrPathNotTXT PathErrorKind = "not_txt"
	// ErrPathZone — имя каталога зоны не нормализуется (ТЗ 5.2 п. 6);
	// причина — *idn.Error, доступна через errors.As/Unwrap.
	ErrPathZone PathErrorKind = "zone"
)

// pathKindDescriptions — тексты классов для Error() и String().
var pathKindDescriptions = map[PathErrorKind]string{
	ErrPathStructure: "структура пути не <зона>/add|delete/*.txt",
	ErrPathNotTXT:    "имя файла не *.txt",
	ErrPathZone:      "невалидное имя каталога зоны",
}

// String возвращает текст класса (стиль idn.ErrorKind.String).
func (k PathErrorKind) String() string {
	if d, ok := pathKindDescriptions[k]; ok {
		return d
	}
	return string(k)
}

// PathError — типизированная ошибка разбора пути: класс, исходный путь
// и причина (для ErrPathZone — *idn.Error).
type PathError struct {
	Kind PathErrorKind // класс — причина отказа файла
	Path string        // исходный относительный путь
	Err  error         // причина; nil для ошибок структуры
}

func (e *PathError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("ingest: %s: %q: %v", e.Kind, e.Path, e.Err)
	}
	return fmt.Sprintf("ingest: %s: %q", e.Kind, e.Path)
}

// Unwrap даёт доступ к причине (errors.Is/As по цепочке — до *idn.Error).
func (e *PathError) Unwrap() error { return e.Err }

// PathKindOf возвращает класс ошибки разбора пути; ok = false, если err —
// не *PathError (чужая ошибка).
func PathKindOf(err error) (PathErrorKind, bool) {
	var e *PathError
	if errors.As(err, &e) {
		return e.Kind, true
	}
	return "", false
}

// File — путь входного файла, разобранный по схеме ТЗ 5.1
// <зона>/add|delete/*.txt.
type File struct {
	// Zone — нормализованная зона в ASCII-форме (punycode, ТЗ 5.2 п. 6):
	// ключ поиска zones.name; unicode- и punycode-имя каталога дают одно
	// значение (ТЗ 11.4 п. 1).
	Zone string
	// ZoneUnicode — unicode-форма зоны для отображения (ТЗ 5.2 п. 7):
	// значение zones.name_unicode при создании зоны; для ASCII-зоны
	// совпадает с Zone.
	ZoneUnicode string
	// Action — действие файла: add — добавить или обновить домены,
	// delete — удалить (ТЗ 5.1; model.Action, колонка ingest_files.action).
	Action model.Action
	// Path — путь относительно incoming после очистки (path.Clean: без
	// «.»/«..» и лишних разделителей) — пишется в журнал ingest_files
	// (T-021).
	Path string
}

// ParsePath разбирает путь готового файла относительно incoming:
// <зона>/add|delete/*.txt → нормализованная зона и действие (ТЗ 5.1).
//
// Правила:
//
//   - ровно три компонента: каталог зоны, каталог операции, имя файла;
//     лишняя вложенность и отсутствие add/delete — ErrPathStructure;
//   - имя каталога операции — строго «add» или «delete» в нижнем регистре:
//     структура каталогов ТЗ 5.1 фиксирована, а регистр файловой системы
//     Linux точен — иное имя это другая структура, а не вариант записи;
//   - имя файла — *.txt без учёта регистра, как фильтр готовности
//     наблюдателя (internal/watch, ТЗ 5.1): всё, что он выдаёт, приём
//     разбирает одинаково; иначе — ErrPathNotTXT;
//   - имя каталога зоны нормализуется idn.NormalizeZone (составные зоны
//     co.uk допустимы, принадлежность PSL не требуется — ТЗ 5.2 п. 6);
//     ошибка нормализации — ErrPathZone с причиной *idn.Error;
//   - абсолютные пути и «..» за пределами incoming — ErrPathStructure.
func ParsePath(rel string) (File, error) {
	if rel == "" || path.IsAbs(rel) {
		return File{}, &PathError{Kind: ErrPathStructure, Path: rel}
	}
	p := path.Clean(rel)
	if p == "." || p == ".." || strings.HasPrefix(p, "../") {
		return File{}, &PathError{Kind: ErrPathStructure, Path: rel}
	}

	parts := strings.Split(p, "/")
	if len(parts) != 3 {
		return File{}, &PathError{Kind: ErrPathStructure, Path: rel}
	}
	zoneDir, opDir, name := parts[0], parts[1], parts[2]

	var action model.Action
	switch opDir {
	case "add":
		action = model.ActionAdd
	case "delete":
		action = model.ActionDelete
	default:
		return File{}, &PathError{Kind: ErrPathStructure, Path: rel}
	}

	// path.Ext даёт хвост от последней точки; готовность по ТЗ 5.1 —
	// имя *.txt, регистр не важен (равно фильтру наблюдателя).
	if !strings.EqualFold(path.Ext(name), ".txt") {
		return File{}, &PathError{Kind: ErrPathNotTXT, Path: rel}
	}

	// Зона тоже нормализуется тем же способом, что и домен (ТЗ 5.2 п. 6):
	// unicode-каталог «москва» и punycode «xn--80adxhks» — одна зона.
	zone, err := idn.NormalizeZone(zoneDir)
	if err != nil {
		return File{}, &PathError{Kind: ErrPathZone, Path: rel, Err: err}
	}
	return File{
		Zone:        zone,
		ZoneUnicode: idn.ToUnicode(zone),
		Action:      action,
		Path:        p,
	}, nil
}

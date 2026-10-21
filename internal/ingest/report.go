// Учёт обработанных файлов в ingest_files (T-021; ТЗ 5.1, 6.1): запись
// отчёта (sha256, действие, счётчики строк и применения) и защита от
// повторной обработки того же файла по уникальной тройке
// (zone_id, action, sha256) — точной повторной доставки (T-113).
// Доступ к БД — через маленький интерфейс FileStore (юнит-тесты — на
// фейке, стиль ZoneStore из T-014), SQL-реализация SQLFileStore —
// поверх internal/db.
//
// Здесь же — текстовый лог-отчёт об обработке файла (renderReport):
// рядом с файлом в failed/ (ТЗ 5.1: «файлы с ошибкой (с лог-отчётом
// рядом)») и основа записей лога сервиса. Формат фиксирован тестом
// TestRenderReport; rejected-лог построчных отклонений (T-016) остаётся
// отдельным потоком записей.
package ingest

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"crawler/internal/db"
	"crawler/internal/model"
)

// FileStore — журнал обработанных файлов ingest_files (ТЗ 5.1, 6.1),
// необходимый конвейеру приёма: защита от повторной обработки того же
// файла и запись отчёта. Интерфейс маленький намеренно (как ZoneStore,
// T-014): юнит-тесты конвейера обходятся фейком, SQL-реализация
// подменяема.
type FileStore interface {
	// FilesBySHA256 возвращает все строки журнала с данным sha256
	// (любые зона и действие, от ранней к поздней): конвейер ищет
	// среди них точную повторную доставку — совпадение
	// (zone_id, action, sha256), уникальный ключ T-113, — и
	// подсвечивает то же содержимое, доставлявшееся иначе (другая
	// зона, другое действие). Пустой срез — содержимое не
	// обрабатывалось, прочее — ошибка хранилища.
	FilesBySHA256(ctx context.Context, sha []byte) ([]model.IngestFile, error)

	// RecordFile пишет отчёт обработанного файла (счётчики применения).
	// Тройка (zone_id, action, sha256) уникальна (миграция 000009):
	// конкурентный отчёт того же файла — ошибка хранилища.
	RecordFile(ctx context.Context, rec FileRecord) error
}

// FileRecord — строка ingest_files для обработанного файла (ТЗ 6.1):
// значения колонок отчёта. Отображение счётчиков конвейера —
// FileReport.record.
type FileRecord struct {
	ZoneID      int16        // зона файла (zones.id)
	Action      model.Action // 1 add, 2 delete (ТЗ 6.1)
	Path        string       // путь относительно incoming (ParsePath)
	SHA256      []byte       // хэш содержимого, 32 октета
	LinesTotal  int64        // всего физических строк файла
	Added       int64        // add: добавлено (включая возвращённых из deleted)
	Existed     int64        // add: уже было
	Deleted     int64        // delete: переведено в deleted
	Rejected    int64        // отклонено строк (парсер + валидатор)
	ProcessedAt time.Time    // момент завершения обработки
}

// ingestFileColumns — колонки ingest_files в порядке полей model.IngestFile
// (теги db модели, T-008); pgx.RowToStructByName.
const ingestFileColumns = "id, zone_id, action, path, sha256, lines_total, added, existed, deleted, rejected, processed_at"

const (
	// От ранней к поздней (ORDER BY id): классификация доставок в
	// конвейере детерминирована, ссылки «первая доставка» — ранняя.
	// Обслуживается ingest_files_sha256_idx (миграция 000009).
	sqlFilesBySHA = "SELECT " + ingestFileColumns + " FROM ingest_files WHERE sha256 = $1 ORDER BY id"

	// Простой INSERT, без ON CONFLICT: конкурентный отчёт того же файла
	// (второй экземпляр сервиса) отвергается уникальной тройкой
	// (zone_id, action, sha256) — схеме виднее, обрабатывался ли файл;
	// подавлять конфликт молча значило бы потерять счётчики одной из
	// попыток.
	sqlFileRecord = "INSERT INTO ingest_files " +
		"(zone_id, action, path, sha256, lines_total, added, existed, deleted, rejected, processed_at) " +
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
)

// SQLFileStore — FileStore поверх pgx-пула (internal/db). Реальное
// поведение против живой БД — интеграционные тесты ingest (T-023);
// юнит-тесты проверяют текст SQL и параметры на фейковом пуле.
type SQLFileStore struct {
	pool db.DB
}

// NewFileStore — FileStore на pgx-пуле (*pgxpool.Pool и любые обёртки,
// удовлетворяющие db.DB).
func NewFileStore(pool db.DB) *SQLFileStore {
	return &SQLFileStore{pool: pool}
}

// FilesBySHA256 — все строки журнала с данным содержимым, от ранней к
// поздней; пустой срез — содержимое не обрабатывалось.
func (s *SQLFileStore) FilesBySHA256(ctx context.Context, sha []byte) ([]model.IngestFile, error) {
	rows, err := s.pool.Query(ctx, sqlFilesBySHA, sha)
	if err != nil {
		return nil, fmt.Errorf("db: ingest_files: поиск по sha256: %w", err)
	}
	fs, err := pgx.CollectRows(rows, pgx.RowToStructByName[model.IngestFile])
	if err != nil {
		return nil, fmt.Errorf("db: ingest_files: поиск по sha256: %w", err)
	}
	return fs, nil
}

// RecordFile пишет отчёт обработанного файла. Зона-less файл пишет
// zone_id = NULL (зон у файла нет — они у строк); NULL в уникальной
// тройке (zone_id, action, sha256) не конфликтует (миграция 000009),
// защиту от точной повторной доставки выполняет конвейер.
func (s *SQLFileStore) RecordFile(ctx context.Context, rec FileRecord) error {
	if len(rec.SHA256) != sha256.Size {
		return fmt.Errorf("ingest: отчёт файла %s: sha256 — %d октетов, ожидается %d",
			rec.Path, len(rec.SHA256), sha256.Size)
	}
	var zoneID any // nil → NULL: зона-less файл (add|delete/<файл>)
	if rec.ZoneID > 0 {
		zoneID = rec.ZoneID
	}
	if _, err := s.pool.Exec(ctx, sqlFileRecord,
		zoneID, int16(rec.Action), rec.Path, rec.SHA256,
		rec.LinesTotal, rec.Added, rec.Existed, rec.Deleted, rec.Rejected,
		rec.ProcessedAt,
	); err != nil {
		return fmt.Errorf("db: ingest_files: отчёт %s: %w", rec.Path, err)
	}
	return nil
}

// FileReport — итог обработки одного файла: счётчики строк (парсер
// T-015 + валидатор T-016) и применения (T-018/T-019). Значения пишутся
// в ingest_files (record) и в лог-отчёт (renderReport); поля без колонок
// в схеме (Skipped, Accepted, Revived, NotFound, Reasons) живут только
// в лог-отчёте и записях лога сервиса.
type FileReport struct {
	// Zone — punycode-зона пути файла (в БД — ZoneID; для лог-отчёта).
	// Пуста у зона-less файла (add|delete/<файл>: зоны строк — из имён)
	// и если путь не разобран (ошибочные файлы тоже получают лог-отчёт).
	Zone string
	// ZoneID — идентификатор zones.id; 0 — зона-less файл или путь
	// не разобран (в журнале — NULL).
	ZoneID int16
	// Action — действие файла (1 add, 2 delete); 0 при неразобранном пути.
	Action model.Action
	// Path — путь относительно incoming (ParsePath) — пишется в
	// ingest_files.path (T-014).
	Path string
	// SHA256 — хэш содержимого файла; nil, если чтение не завершилось.
	SHA256 []byte

	// Счётчики строк.
	LinesTotal int64 // физических строк файла (ТЗ 5.1: «кол-во строк»)
	Skipped    int64 // пустые строки и комментарии — в БД не попадают
	Rejected   int64 // отклонено: парсер (UTF-8/длина) + валидатор (имя/зона)
	Accepted   int64 // валидных строк (дошло до staging)

	// Reasons — отклонения в разрезе причин (Reason), для лог-отчёта.
	Reasons map[Reason]int

	// Members — членов архива обработано (T-133); у txt-файлов 0.
	// Колонки в схеме нет — только лог-отчёт и записи лога сервиса.
	Members int

	// Применение (после ApplyAdd/ApplyDelete; нули, если не дошло).
	Added    int64 // add: Inserted + Revived
	Revived  int64 // add: возвращено из deleted — часть Added
	Existed  int64 // add: уже было
	Deleted  int64 // delete: переведено в deleted
	NotFound int64 // delete: строк без домена в БД
}

// record — значения колонок ingest_files для этого итога (ТЗ 6.1).
// Отображение счётчиков: Revived входит в Added (возвращённый из
// deleted домен — тоже результат файла «добавлен», отдельных сущностей
// схема не различает); NotFound колонки не имеет (ТЗ 5.1 перечисляет
// добавлено/уже было/удалено/отклонено) — он виден в лог-отчёте:
// NotFound = Accepted − Deleted по строкам delete-файла.
func (r FileReport) record(at time.Time) FileRecord {
	return FileRecord{
		ZoneID:      r.ZoneID,
		Action:      r.Action,
		Path:        r.Path,
		SHA256:      r.SHA256,
		LinesTotal:  r.LinesTotal,
		Added:       r.Added,
		Existed:     r.Existed,
		Deleted:     r.Deleted,
		Rejected:    r.Rejected,
		ProcessedAt: at,
	}
}

// renderReport — текстовый лог-отчёт об обработке файла (ТЗ 5.1: failed/
// — «файлы с ошибкой (с лог-отчётом рядом)»; тот же текст — основа
// записей лога сервиса). verdict — итог одной строкой: «применён»,
// «дубль по sha256 — уже был обработан», «ошибочен — <причина>».
func renderReport(rep FileReport, verdict string) string {
	var b strings.Builder
	b.WriteString("# лог-отчёт ingest (ТЗ 5.1)\n")
	fmt.Fprintf(&b, "файл: %s\n", rep.Path)
	if rep.Zone != "" {
		fmt.Fprintf(&b, "зона: %s (id %d)\n", rep.Zone, rep.ZoneID)
	} else if rep.Action.Valid() {
		b.WriteString("зона: по именам строк (файл без каталога зоны)\n")
	} else {
		b.WriteString("зона: — (путь не разобран)\n")
	}
	fmt.Fprintf(&b, "действие: %s\n", actionName(rep.Action))
	if len(rep.SHA256) > 0 {
		fmt.Fprintf(&b, "sha256: %x\n", rep.SHA256)
	}
	fmt.Fprintf(&b, "итог: %s\n", verdict)

	fmt.Fprintf(&b, "строк: %d\n", rep.LinesTotal)
	if rep.Members > 0 {
		fmt.Fprintf(&b, "членов архива: %d\n", rep.Members)
	}
	fmt.Fprintf(&b, "  пропущено (пустые и комментарии): %d\n", rep.Skipped)
	fmt.Fprintf(&b, "  отклонено: %d\n", rep.Rejected)
	for _, r := range sortedReasons(rep.Reasons) {
		// string(r), не %s: в отчёте — устойчивое имя причины (как в
		// rejected-логе, T-016), а не его текст.
		fmt.Fprintf(&b, "    %s: %d\n", string(r), rep.Reasons[r])
	}
	fmt.Fprintf(&b, "  валидных: %d\n", rep.Accepted)

	switch rep.Action {
	case model.ActionAdd:
		fmt.Fprintf(&b, "добавлено: %d (в т.ч. возвращено из deleted: %d)\n", rep.Added, rep.Revived)
		fmt.Fprintf(&b, "уже было: %d\n", rep.Existed)
	case model.ActionDelete:
		fmt.Fprintf(&b, "удалено: %d\n", rep.Deleted)
		fmt.Fprintf(&b, "не найдено в БД: %d\n", rep.NotFound)
	}
	return b.String()
}

// actionName — имя действия файла; нулевое (путь не разобран) — прочерк.
func actionName(a model.Action) string {
	if a.Valid() {
		return a.String()
	}
	return "—"
}

// sortedReasons — причины отклонений в детерминированном порядке
// (алфавит): лог-отчёт воспроизводим от строки к строке.
func sortedReasons(m map[Reason]int) []Reason {
	out := make([]Reason, 0, len(m))
	for r := range m {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// mergeReasons — отклонения парсера и валидатора в одной карте
// (FileReport.Reasons): причины парсера (invalid_utf8, line_len) и
// валидатора (wrong_zone, классы idn.ErrorKind) не пересекаются по именам.
func mergeReasons(parser, validator map[Reason]int) map[Reason]int {
	out := make(map[Reason]int, len(parser)+len(validator))
	for k, n := range parser {
		out[k] += n
	}
	for k, n := range validator {
		out[k] += n
	}
	return out
}

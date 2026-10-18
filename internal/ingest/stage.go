// Package ingest — загрузка валидированных строк входных файлов в БД
// (этап 1, ТЗ 5.1/3.5).
//
// Staging-таблица ingest_stage (миграция 000004) принимает поток строк
// одного файла пачками по ingest.batch_size (по умолчанию 50 000, ТЗ 5.1)
// через COPY: каждая пачка — отдельная транзакция, память ограничена
// размером пачки, отмена контекста между пачками не теряет законченные
// пачки (ТЗ 3.4/3.5). Применение add/delete к domains — задачи T-018/T-019;
// после применения партия очищается (ClearBatch).
//
// Идемпотентность (ТЗ 3.4, 5.1): партия идентифицируется batch_id —
// время создания плюс случайность (T-098). Повторная загрузка той же
// партии (перезапуск прерванной) сначала удаляет свои прежние строки
// и копирует поток заново — данные не портятся и не дублируются, мусор
// не накапливается. Параллельные загрузки (разные batch_id) не мешают
// друг другу: очистка адресована только своей партии.
package ingest

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"crawler/internal/db"
	"crawler/internal/model"
)

// BatchIDLen — размер идентификатора партии загрузки: 16 байт (128 бит).
const BatchIDLen = 16

// batchTimeLen — размер временно́го префикса идентификатора: старшие
// 8 байт, микросекунды Unix (T-098).
const batchTimeLen = 8

// compile-time: префикс времени помещается в идентификатор — срезы
// id[:batchTimeLen] ниже иначе вышли бы за границы массива.
var _ [BatchIDLen - batchTimeLen]struct{}

// BatchID — идентификатор партии загрузки staging: BatchIDLen байт —
// старшие batchTimeLen: микросекунды Unix времени создания партии
// (big-endian), остальные: случайные байты (crypto/rand, NewBatchID).
// Выдаётся один на загрузку файла, объединяет все COPY-пачки этой
// загрузки и адресует очистку партии. Время в префиксе делает побайтовый
// порядок (memcmp bytea в PostgreSQL) хронологическим: фоновая зачистка
// осиротевших партий (CleanupStaleBatches, T-098) режет диапазон
// batch_id по возрасту без колонки времени — и всё ещё сканом по
// префиксу первичного ключа.
type BatchID [BatchIDLen]byte

// NewBatchID генерирует идентификатор партии с текущим временем создания.
func NewBatchID() (BatchID, error) {
	return newBatchIDAt(time.Now())
}

// newBatchIDAt — генерация идентификатора партии с заданным временем
// создания (заглушка времени в тестах): временной префикс плюс
// случайный хвост.
func newBatchIDAt(t time.Time) (BatchID, error) {
	var id BatchID
	binary.BigEndian.PutUint64(id[:batchTimeLen], uint64(t.UnixMicro()))
	if _, err := rand.Read(id[batchTimeLen:]); err != nil {
		return BatchID{}, fmt.Errorf("ingest: генерация идентификатора партии: %w", err)
	}
	return id, nil
}

// batchIDBefore — граница «создана строго раньше момента t» в терминах
// batch_id: идентификаторы партий, созданных до t, побайтово меньше
// возвращаемого. Нулевой случайный хвост — минимальное значение своей
// микросекунды, поэтому партии микросекунды t и моложе не задеваются.
func batchIDBefore(t time.Time) BatchID {
	var id BatchID
	binary.BigEndian.PutUint64(id[:batchTimeLen], uint64(t.UnixMicro()))
	return id
}

// Batch — партия: загрузка строк одного входного файла в staging.
// Все строки файла принадлежат одной зоне и несут одну операцию (ТЗ 5.1:
// путь <зона>/add|delete/*.txt), поэтому зона и операция задаются на партии
// и пишутся в каждую строку: применение (T-018/T-019) читает всё нужное из
// самой строки staging без побочных справочников.
type Batch struct {
	ID     BatchID      // идентификатор партии (NewBatchID)
	ZoneID int16        // зона файла (zones.id)
	Action model.Action // 1 add, 2 delete (ТЗ 5.1)
}

// Row — валидированная строка входного файла (проверка имён — T-016):
// метка домена без зоны в punycode (ТЗ 5.2; может быть составной —
// «магазин.рф.co.uk» в зоне co.uk даёт «xn--80aairftm.xn--p1ai»,
// контракт internal/idn) и её позиция в файле.
type Row struct {
	Label  string
	LineNo int64 // номер строки в исходном файле, с единицы
}

// RowIter — источник строк потока: следующая строка либо io.EOF в конце.
// Как и db.RowIter, позволяет читать данные лениво (построчно из файла),
// не материализуя их целиком — память определяется размером пачки (ТЗ 3.5).
type RowIter func() (Row, error)

// StageTable / StageColumns — staging-таблица и колонки COPY в порядке
// объявления миграции 000004; соответствие SQL — тестом контракта
// TestStageTableContract (интеграционная проверка схемы — T-023).
var (
	StageTable   = pgx.Identifier{"ingest_stage"}
	StageColumns = []string{"batch_id", "zone_id", "label", "action", "line_no"}
)

// MaxLabelLen — предел длины метки домена: 63 октета в ASCII-форме
// (ТЗ 5.2 п. 4). Здесь это дешёвый инвариант строки: слишком длинную метку
// обязан отклонить валидатор потока (T-016).
const MaxLabelLen = 63

// clearStageSQL — очистка партии: ведущий batch_id первичного ключа
// обслуживает скан по префиксу. Имя таблицы — из StageTable (экранирование
// идентификатора, как в internal/db), единый источник для SQL и COPY.
var clearStageSQL = "DELETE FROM " + StageTable.Sanitize() + " WHERE batch_id = $1"

// Stage загружает поток валидированных строк в staging-таблицу пачками по
// batchSize строк (ingest.batch_size, по умолчанию 50 000 — ТЗ 5.1) через
// db.CopyFromStream: пачка — отдельная транзакция COPY, память ограничена
// batchSize (ТЗ 3.5); отмена ctx между пачками прерывает загрузку, не
// затрагивая законченные пачки (ТЗ 3.4). Возвращает число строк, попавших
// в staging (без строк пачки, упавшей или отменённой).
//
// Перед копированием удаляются прежние строки этой партии: повторный вызов
// с тем же Batch.ID перезаписывает партию с чистого листа — данные не
// дублируются и не портятся (идемпотентность повтора, ТЗ 3.4). Чужие
// партии (другие batch_id) не затрагиваются.
func Stage(ctx context.Context, pool db.DB, b Batch, batchSize int, next RowIter) (int64, error) {
	if b.ID == (BatchID{}) {
		return 0, errors.New("ingest: партия без идентификатора (ingest.NewBatchID)")
	}
	if b.ZoneID < 1 {
		return 0, fmt.Errorf("ingest: зона партии %d: ожидается идентификатор zones.id (>= 1)", b.ZoneID)
	}
	if !b.Action.Valid() {
		return 0, fmt.Errorf("ingest: операция партии %d: ожидается 1 add / 2 delete (ТЗ 5.1)", int16(b.Action))
	}
	if batchSize < 1 {
		return 0, fmt.Errorf("ingest: размер пачки %d, минимум 1 (ingest.batch_size)", batchSize)
	}

	// Идемпотентность повтора (ТЗ 3.4): источник истины — входной файл,
	// поэтому прерванная загрузка просто перезаливается целиком, а не
	// дополняется: затираем свою прошлую партию (недогруженную либо уже
	// применённую) и копируем поток заново.
	if _, err := ClearBatch(ctx, pool, b.ID); err != nil {
		return 0, err
	}

	staged, err := db.CopyFromStream(ctx, pool, StageTable, StageColumns, batchSize, b.rowIter(next))
	if err != nil {
		return staged, fmt.Errorf("ingest: загрузка партии в %s: %w", StageTable, err)
	}
	return staged, nil
}

// ClearBatch удаляет из staging все строки партии и возвращает их число.
// Вызывается Stage перед загрузкой (перезапись своей партии) и будущим
// применением после завершения (T-018/T-019) — staging не растёт от файла
// к файлу.
func ClearBatch(ctx context.Context, pool db.DB, id BatchID) (int64, error) {
	if id == (BatchID{}) {
		return 0, errors.New("ingest: партия без идентификатора (ingest.NewBatchID)")
	}
	tag, err := pool.Exec(ctx, clearStageSQL, id[:])
	if err != nil {
		return 0, fmt.Errorf("ingest: очистка партии в %s: %w", StageTable, err)
	}
	return tag.RowsAffected(), nil
}

// rowIter адаптирует типизированный поток строк под db.RowIter: собирает
// значения колонок COPY (в порядке StageColumns) и проверяет дешёвые
// инварианты каждой строки — полная валидация имён делается до загрузки
// (T-016). Проверки защищают staging от программных ошибок источника:
// метка непустая и не-ASCII (это метка без зоны в punycode, ТЗ 5.2),
// составной бывает (контракт internal/idn — точки допустимы), но каждая
// её часть непустая и не длиннее MaxLabelLen (ТЗ 5.2 п. 4); номер строки
// с единицы и строго растёт — уникальность (batch_id, line_no) внутри
// партии до БД.
func (b Batch) rowIter(next RowIter) db.RowIter {
	var lastLine int64
	return func() ([]any, error) {
		row, err := next()
		if err != nil {
			return nil, err
		}
		switch {
		case row.Label == "":
			return nil, fmt.Errorf("ingest: строка %d: пустая метка домена", row.LineNo)
		case !isASCII(row.Label):
			return nil, fmt.Errorf("ingest: строка %d: %q — не-ASCII в метке: ожидается punycode (ТЗ 5.2)", row.LineNo, row.Label)
		case !validLabelParts(row.Label):
			return nil, fmt.Errorf("ingest: строка %d: %q — каждая метка без зоны непустая и не длиннее %d октетов (ТЗ 5.2 п. 4)", row.LineNo, row.Label, MaxLabelLen)
		case row.LineNo < 1:
			return nil, fmt.Errorf("ingest: номер строки %d: нумерация с единицы", row.LineNo)
		case row.LineNo <= lastLine:
			return nil, fmt.Errorf("ingest: номер строки %d не растёт (предыдущий %d): строки партии уникальны по line_no", row.LineNo, lastLine)
		}
		lastLine = row.LineNo
		return []any{b.ID[:], b.ZoneID, row.Label, int16(b.Action), row.LineNo}, nil
	}
}

// validLabelParts — составная метка допустима (контракт internal/idn,
// ТЗ 5.2): каждая часть между точками — непустая метка DNS не длиннее
// MaxLabelLen октет.
func validLabelParts(label string) bool {
	start := 0
	for i := 0; i <= len(label); i++ {
		if i == len(label) || label[i] == '.' {
			if n := i - start; n < 1 || n > MaxLabelLen {
				return false
			}
			start = i + 1
		}
	}
	return true
}

// isASCII — все октеты метки меньше 0x80 (punycode-форма, ТЗ 5.2).
func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > 0x7f {
			return false
		}
	}
	return true
}

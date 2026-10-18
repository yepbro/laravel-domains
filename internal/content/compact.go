// Фоновая компактификация пак-файлов (T-045; ТЗ 5.7, 13.2).
//
// Пак — append-only: Delete снимает ссылки (refcount → 0, «мёртвая»
// строка T-042), перезаписи переводят размещения на свежие блоки, краш
// между дописыванием блока и фиксацией метаданных оставляет мусорный
// блок без строки. Всё это копит в паках мёртвые байты; компактификация
// переписывает живые блоки в новые паки и удаляет старый файл, когда
// доля живых данных падает ниже порога (ТЗ 5.7: «фоновая компактификация
// паков при доле живых данных < 50%»; порог и интервал — на усмотрение
// разработки, ТЗ 13.2).
//
// Порядок правки одного пака (ТЗ 3.4, «дописать → зафиксировать →
// удалить»):
//
//   - компактифицируются только паки, закрытые для записи, — все, кроме
//     новейшего: писатели дописывают исключительно в новейший пак и
//     фиксируют строку метаданных, не отпуская flock (T-042, уточнено
//     здесь), поэтому множество строк не-новейшего пака заморожено;
//   - живые блоки переписываются в новейший пак обычным путём записи
//     (appendBlock: мьютекс + flock, O_APPEND) — старый пак не трогается;
//   - транзакция (пачками, короткими — стиль T-020) переводит размещения
//     живых строк на новые блоки и удаляет мёртвые строки пака. До
//     коммита старый пак цел — краш оставляет скопированные блоки
//     мусором, чтение идёт по старым размещениям;
//   - после коммита файл старого пака удаляется (под flock, после
//     контрольных проверок). Краш между коммитом и удалением оставляет
//     файл из одного мусора — следующий проход видит долю живых 0 и
//     удаляет его.
//
// Повторный проход безопасен: перемещённое не перемещается дважды
// (охрана pack_id в UPDATE), мёртвые строки удаляются с охраной
// refcount = 0. Занятое хранилищем место — сумма размеров пак-файлов
// каталога, отдельного счётчика нет: после удаления старого пака оно
// честно уменьшается (CompactResult.FreedBytes; проактивный порог
// заполнения — T-046).
package content

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/db"
)

const (
	// DefaultCompactThreshold — порог доли живых данных: пак
	// компактифицируется, когда она строго ниже (ТЗ 5.7 — 50%).
	DefaultCompactThreshold = 0.5

	// DefaultCompactInterval — интервал фоновых проходов CompactLoop.
	// ТЗ темп не задаёт (13.2): компактификация не спешит, место
	// возвращается фоново; час не гоняет дисковый I/O после каждой
	// пачки удалений и успевает подобрать хвосты за ночь.
	DefaultCompactInterval = time.Hour

	// compactChunk — строк метаданных на одну транзакцию правки: короткие
	// транзакции и ограниченная память (ТЗ 3.5), крах переигрывается
	// со следующей пачки.
	compactChunk = 1000
)

// SQL компактификации. Точные тексты закреплены тестами
// (compact_test.go, стиль pack_test/ingest-тестов).
const (
	// Строки пака — живые и мёртвые вместе; порядок по смещению —
	// линейное чтение исходного пака при копировании.
	sqlCompactRows = "SELECT hash, off, len, refcount FROM contents " +
		"WHERE pack_id = $1 ORDER BY off"

	// Перенос размещений живых строк на новые блоки (len/raw_len не
	// меняются — блоки копируются дословно). Охрана pack_id и
	// refcount > 0: строка, умершая или переехавшая с момента чтения,
	// не переносится — умершая останется «мёртвой» (невидимой), её блок
	// заберёт следующий проход.
	sqlCompactMove = "UPDATE contents SET pack_id = v.pack_id, off = v.off " +
		"FROM unnest($1::bytea[], $2::int4[], $3::int8[]) AS v(hash, pack_id, off) " +
		"WHERE contents.hash = v.hash AND contents.pack_id = $4 " +
		"AND contents.refcount > 0"

	// Удаление мёртвых строк, указывающих в компактифицируемый пак:
	// место под них освобождается только здесь (docs/content.md).
	// Охрана refcount = 0 — воскрешённая в момент прохода строка остаётся
	// жить со своим свежим блоком.
	sqlCompactDead = "DELETE FROM contents " +
		"USING unnest($1::bytea[]) AS h(hash) " +
		"WHERE contents.hash = h.hash AND contents.pack_id = $2 " +
		"AND contents.refcount = 0"

	// Контроль перед удалением файла: живых строк для пака не осталось.
	// Инвариант T-042/T-045 (строки появляются только под flock вместе с
	// блоком, а блоки — только в новейший пак) делает ответ неизменным
	// для не-новейшего пака; запрос — страховка от нарушений инварианта.
	sqlCompactLive = "SELECT count(*) FROM contents " +
		"WHERE pack_id = $1 AND refcount > 0"

	// Число всех строк пака (живых и мёртвых) — различение «пустого»
	// пака без заголовка (строк нет — файл-мусор) от повреждения
	// (строки, указывающие в файл без заголовка).
	sqlCompactAll = "SELECT count(*) FROM contents WHERE pack_id = $1"

	// Пометка мёртвой строки с нечитаемым блоком (потеря/порча байтов):
	// повторный Put её не чинит (дедуп видит живую строку), компактификация
	// без пометки не смогла бы ни перенести блок, ни удалить пак — «ядовитый»
	// пак вечно блокировал бы освобождение места. Охраны pack_id (строка
	// не переехала) и refcount > 0 (уже мёртвая не нужна).
	sqlCompactCorrupt = "UPDATE contents SET refcount = 0 " +
		"WHERE hash = $1 AND pack_id = $2 AND refcount > 0"
)

// errCompactNewest — попытка компактифицировать новейший пак: он открыт
// для записи (T-042), компактифицируются только закрытые.
var errCompactNewest = errors.New("content: новейший пак не компактифицируется")

// CompactDB — пул для компактификации: db.DB, из которого можно начать
// транзакцию (перенос размещений и удаление мёртвых строк атомарны).
// *pgxpool.Pool удовлетворяет; стиль ingest.CleanupDB (T-020).
type CompactDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// compile-time: реальный пул подходит под интерфейс.
var _ CompactDB = (*pgxpool.Pool)(nil)

// CompactOptions — параметры компактификации; нулевые значения
// заменяются дефолтами (withDefaults, стиль T-020).
type CompactOptions struct {
	// Interval — период проходов CompactLoop (DefaultCompactInterval);
	// на разовый CompactOnce не влияет.
	Interval time.Duration
	// Threshold — порог доли живых данных: пак компактифицируется,
	// когда она строго ниже (DefaultCompactThreshold, ТЗ 5.7).
	Threshold float64
	// Log — логгер цикла CompactLoop; nil — записи подавлены
	// (slog.DiscardHandler).
	Log *slog.Logger
}

// withDefaults заменяет некорректные нули значениями по умолчанию;
// Threshold вне (0, 1] не имеет смысла доли — тоже дефолт.
func (o CompactOptions) withDefaults() CompactOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultCompactInterval
	}
	if o.Threshold <= 0 || o.Threshold > 1 {
		o.Threshold = DefaultCompactThreshold
	}
	return o
}

// CompactInfo — итог анализа пака CompactScan.
type CompactInfo struct {
	PackID int32
	// Size — размер файла пака; DataBytes — место записей без заголовка.
	Size      int64
	DataBytes int64
	// LiveBytes — место живых блоков (строки с refcount > 0, префиксы
	// длины включены): то, что перепишет компактификация.
	LiveBytes int64
	// DeadBytes — место блоков мёртвых строк (refcount = 0).
	DeadBytes int64
	// GarbageBytes — прочее место: блоки без строки метаданных
	// (краш-окно T-042, проигравшие конкурентные Put) и недописанный
	// хвост.
	GarbageBytes int64
	LiveBlocks   int64
	DeadBlocks   int64
	// GarbageBlocks — целые записи без строки; байты недописанного
	// хвоста входят в GarbageBytes, но блоком не считаются.
	GarbageBlocks int64
	// Truncated — обход остановился на недописанной записи в хвосте
	// (краш записи, docs/content.md).
	Truncated bool
	// Ratio — доля живых данных: LiveBytes / DataBytes (0 при пустом).
	Ratio float64
}

// CompactResult — итог компактификации одного пака CompactPack.
type CompactResult struct {
	PackID       int32
	CopiedBlocks int   // переписанные живые блоки
	CopiedBytes  int64 // их место в новых паках (с префиксами длины)
	Tombstones   int64 // удалённые мёртвые строки contents
	Removed      bool  // файл старого пака удалён
	FreedBytes   int64 // освобождено места (размер удалённого файла)
	// Corrupted — живые строки с нечитаемыми блоками: помечены мёртвыми,
	// компактификация пака продолжена; расхождение целостности — в ошибке
	// CompactPack (Warn прохода), детали — в её тексте.
	Corrupted int64
}

// CompactStats — итог одного прохода CompactOnce.
type CompactStats struct {
	Packs      int   // просмотрено паков (кроме новейшего)
	Compacted  int   // переписано и удалено
	FreedBytes int64 // освобождено места суммарно
}

// CompactScan анализирует пак: живые/мёртвые/мусорные байты по строкам
// contents (pack_id, refcount) и обходу записей файла — формат T-042
// позволяет пройти пак по префиксам длины. Строки читаются из БД до
// обхода файла: байты блока всегда старше своей строки («спул →
// фиксация», T-042), поэтому противоречий не бывает, а блоки без строки
// — мусор. Скан не меняет ничего и не держит блокировок: паки
// append-only, а строки не-новейшего пака заморожены (T-042).
func (s *PackStore) CompactScan(ctx context.Context, packID int32) (CompactInfo, error) {
	info := CompactInfo{PackID: packID}
	path := filepath.Join(s.opts.Dir, packFileName(packID))
	st, err := os.Stat(path)
	if err != nil {
		return info, fmt.Errorf("content: скан пака %d: %w", packID, err)
	}
	if st.Size() < packHeaderLen {
		// Файл короче заголовка (обрыв записи заголовка при сбое, C-7):
		// ни одной целой записи в нём быть не может. Строк contents на
		// Строк contents на такой пак не должно быть — «пустой» пак (доля живых 0) проход
		// удалит вместо вечной ошибки скана; строки у пака — повреждение.
		n, err := s.allContents(ctx, packID)
		if err != nil {
			return info, fmt.Errorf("content: скан пака %d: %w", packID, err)
		}
		if n > 0 {
			return info, fmt.Errorf("content: скан пака %d: файл %d байт короче заголовка при %d строках contents",
				packID, st.Size(), n)
		}
		info.Size = st.Size()
		return info, nil
	}
	info.Size = st.Size()
	info.DataBytes = st.Size() - packHeaderLen

	f, err := os.Open(path)
	if err != nil {
		return info, fmt.Errorf("content: скан пака %d: %w", packID, err)
	}
	defer func() { _ = f.Close() }() // только чтение

	rows, err := s.pool.Query(ctx, sqlCompactRows, packID)
	if err != nil {
		return info, fmt.Errorf("content: скан пака %d: метаданные: %w", packID, err)
	}
	defer rows.Close()
	var (
		liveBytes, deadBytes   int64
		liveBlocks, deadBlocks int64
		prefix                 [4]byte
	)
	for rows.Next() {
		var (
			hash       []byte
			off        int64
			clen, refc int32
		)
		if err := rows.Scan(&hash, &off, &clen, &refc); err != nil {
			return info, fmt.Errorf("content: скан пака %d: чтение метаданных: %w", packID, err)
		}
		if len(hash) != HashLen {
			return info, fmt.Errorf("content: скан пака %d: хэш %x: %d октетов, ожидается %d",
				packID, hash, len(hash), HashLen)
		}
		// Строка обязана указывать на целую запись файла: после
		// заголовка, в пределах файла, с совпадающим префиксом длины.
		if off < packHeaderLen+4 || off+int64(clen) > st.Size() {
			return info, fmt.Errorf("content: скан пака %d: %x: блок [%d, %d) вне файла (%d байт)",
				packID, hash, off, off+int64(clen), st.Size())
		}
		if _, err := f.ReadAt(prefix[:], off-4); err != nil {
			return info, fmt.Errorf("content: скан пака %d: %x: префикс длины: %w", packID, hash, err)
		}
		if got := int32(binary.LittleEndian.Uint32(prefix[:])); got != clen {
			return info, fmt.Errorf("content: скан пака %d: %x: префикс длины %d не сходится с len %d",
				packID, hash, got, clen)
		}
		if refc > 0 {
			liveBlocks++
			liveBytes += 4 + int64(clen)
		} else {
			deadBlocks++
			deadBytes += 4 + int64(clen)
		}
	}
	if err := rows.Err(); err != nil {
		return info, fmt.Errorf("content: скан пака %d: метаданные: %w", packID, err)
	}

	// Обход записей: заголовок, затем [u32 len][блок] до конца. Хвост,
	// не собирающийся в целую запись, — байты мусора (краш записи),
	// блоком не считается.
	var (
		walked int64
		pos    = int64(packHeaderLen)
	)
	for pos < st.Size() {
		if _, err := f.ReadAt(prefix[:], pos); err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return info, fmt.Errorf("content: скан пака %d: обход записей: %w", packID, err)
			}
			info.Truncated = true
			break
		}
		recLen := int64(binary.LittleEndian.Uint32(prefix[:]))
		if pos+4+recLen > st.Size() {
			info.Truncated = true
			break
		}
		pos += 4 + recLen
		walked++
	}

	info.LiveBytes = liveBytes
	info.DeadBytes = deadBytes
	info.LiveBlocks = liveBlocks
	info.DeadBlocks = deadBlocks
	info.GarbageBytes = info.DataBytes - liveBytes - deadBytes
	info.GarbageBlocks = walked - liveBlocks - deadBlocks
	// Отрицательный мусор — строки перекрывают друг друга: метаданные
	// повреждены, компактифицировать такой пак нельзя.
	if info.GarbageBytes < 0 || info.GarbageBlocks < 0 {
		return CompactInfo{PackID: packID}, fmt.Errorf(
			"content: скан пака %d: строки перекрывают друг друга (живые %d, мёртвые %d байт при данных %d)",
			packID, liveBytes, deadBytes, info.DataBytes)
	}
	if info.DataBytes > 0 {
		info.Ratio = float64(liveBytes) / float64(info.DataBytes)
	}
	return info, nil
}

// compactLiveRow — живая строка пака для переписывания.
type compactLiveRow struct {
	hash Hash
	off  int64
	clen int32
}

// CompactPack компактифицирует один пак: живые блоки переписываются
// в новейший пак (старый файл не трогается), пачками по compactChunk
// транзакции переносят размещения живых строк и удаляют мёртвые строки,
// после чего файл пака удаляется (контроль и unlink под flock).
// Подробности и краш-безопасность — в комментарии файла.
func (s *PackStore) CompactPack(ctx context.Context, packID int32) (CompactResult, error) {
	cdb, ok := s.pool.(CompactDB)
	if !ok {
		return CompactResult{}, errors.New("content: компактификация: пул БД не поддерживает транзакции")
	}
	if err := s.checkNotNewest(packID); err != nil {
		return CompactResult{}, err
	}
	path := filepath.Join(s.opts.Dir, packFileName(packID))
	st, err := os.Stat(path)
	if err != nil {
		return CompactResult{}, fmt.Errorf("content: компактификация пака %d: %w", packID, err)
	}
	size := st.Size()

	rows, err := s.pool.Query(ctx, sqlCompactRows, packID)
	if err != nil {
		return CompactResult{}, fmt.Errorf("content: компактификация пака %d: метаданные: %w", packID, err)
	}
	defer rows.Close()

	res := CompactResult{PackID: packID}
	var (
		live    []compactLiveRow
		dead    [][]byte
		corrupt []error
	)
	// flush переписывает накопленную пачку живых блоков в новейший пак и
	// фиксирует транзакцию. Сбой до Commit оставляет скопированные блоки
	// мусором, строки не тронуты — повтор прохода переиграет пачку.
	flush := func() error {
		if len(live) == 0 && len(dead) == 0 {
			return nil
		}
		hashes := make([][]byte, 0, len(live))
		packs := make([]int32, 0, len(live))
		offs := make([]int64, 0, len(live))
		var corruptHashes [][]byte
		for _, r := range live {
			block, err := readPackBlock(s.opts.Dir, packID, r.off, r.clen)
			if err != nil {
				// Блок недоступен: чтение и повторный Put этот контент
				// не восстановят (дедуп видит живую строку). Строка
				// помечается мёртвой в той же транзакции — пак остаётся
				// компактифицируемым, место освобождается; расхождение
				// целостности уходит в итог прохода (Warn цикла).
				corrupt = append(corrupt, fmt.Errorf(
					"content: компактификация пака %d: %x: блок нечитаем, строка помечена мёртвой: %w",
					packID, r.hash, err))
				corruptHashes = append(corruptHashes, r.hash[:])
				res.Corrupted++
				continue
			}
			newPack, newOff, err := s.appendBlock(block)
			if err != nil {
				return err
			}
			hashes = append(hashes, r.hash[:])
			packs = append(packs, newPack)
			offs = append(offs, newOff)
			res.CopiedBlocks++
			res.CopiedBytes += 4 + int64(r.clen)
		}
		tx, err := cdb.Begin(ctx)
		if err != nil {
			return fmt.Errorf("content: компактификация пака %d: транзакция: %w", packID, err)
		}
		// Откат безусловно: после Commit — no-op (стиль T-020).
		defer func() { _ = tx.Rollback(ctx) }()
		if len(corruptHashes) > 0 {
			// До sqlCompactDead: помеченная строка удаляется той же
			// транзакцией (охрана refcount = 0 после пометки сойдётся).
			for _, hb := range corruptHashes {
				if _, err := tx.Exec(ctx, sqlCompactCorrupt, hb, packID); err != nil {
					return fmt.Errorf("content: компактификация пака %d: пометка нечитаемого блока: %w", packID, err)
				}
			}
			dead = append(dead, corruptHashes...)
		}
		if len(hashes) > 0 {
			if _, err := tx.Exec(ctx, sqlCompactMove, hashes, packs, offs, packID); err != nil {
				return fmt.Errorf("content: компактификация пака %d: перенос размещений: %w", packID, err)
			}
		}
		if len(dead) > 0 {
			tag, err := tx.Exec(ctx, sqlCompactDead, dead, packID)
			if err != nil {
				return fmt.Errorf("content: компактификация пака %d: удаление мёртвых строк: %w", packID, err)
			}
			res.Tombstones += tag.RowsAffected()
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("content: компактификация пака %d: фиксация: %w", packID, err)
		}
		live, dead = live[:0], dead[:0]
		return nil
	}

	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		var (
			hash       []byte
			off        int64
			clen, refc int32
		)
		if err := rows.Scan(&hash, &off, &clen, &refc); err != nil {
			return res, fmt.Errorf("content: компактификация пака %d: чтение метаданных: %w", packID, err)
		}
		if len(hash) != HashLen {
			return res, fmt.Errorf("content: компактификация пака %d: хэш %x: %d октетов, ожидается %d",
				packID, hash, len(hash), HashLen)
		}
		var h Hash
		copy(h[:], hash)
		if refc > 0 {
			live = append(live, compactLiveRow{hash: h, off: off, clen: clen})
		} else {
			dead = append(dead, h[:])
		}
		if len(live)+len(dead) >= compactChunk {
			if err := flush(); err != nil {
				return res, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return res, fmt.Errorf("content: компактификация пака %d: метаданные: %w", packID, err)
	}
	if err := flush(); err != nil {
		return res, err
	}

	// Контроль перед удалением: живых строк для пака не осталось.
	// Не-новейший пак новых строк не получает (T-042/T-045), запрос —
	// страховка; выполняется до захвата блокировок, чтобы не держать
	// flock на время запроса к БД.
	n, err := s.liveContents(ctx, packID)
	if err != nil {
		return res, err
	}
	if n > 0 {
		return res, fmt.Errorf("content: компактификация пака %d: живых строк %d — файл не удаляется",
			packID, n)
	}

	// Удаление файла: размер не должен был измениться (писать в пак
	// некому), unlink — под мьютексом и flock, как и вся работа с файлами.
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := flockDir(s.lock); err != nil {
		return res, err
	}
	defer func() { unlockDir(s.lock) }()
	st, err = os.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// Другой проход успел удалить файл (двойная компактификация).
			res.Removed = true
			res.FreedBytes = size
			return res, nil
		}
		return res, fmt.Errorf("content: компактификация пака %d: %w", packID, err)
	}
	if st.Size() != size {
		return res, fmt.Errorf(
			"content: компактификация пака %d: файл изменился с момента скана (%d → %d байт) — удаление пропущено",
			packID, size, st.Size())
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return res, fmt.Errorf("content: компактификация пака %d: удаление: %w", packID, err)
	}
	res.Removed = true
	res.FreedBytes = size
	// Пак скомпактифицирован и удалён; расхождения целостности
	// (нечитаемые блоки) — в итоговой ошибке, проход их не теряет.
	return res, errors.Join(corrupt...)
}

// checkNotNewest — предусловие компактификации: пак закрыт для записи.
// Новейший определяется по каталогу под мьютексом и flock — в тот же
// захват, в котором писатели выделяют id новых паков.
func (s *PackStore) checkNotNewest(packID int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errPackClosed
	}
	if err := flockDir(s.lock); err != nil {
		return err
	}
	defer func() { unlockDir(s.lock) }()
	maxID, err := s.scanPackIDs()
	if err != nil {
		return err
	}
	switch {
	case packID == maxID:
		return errCompactNewest
	case packID > maxID:
		return fmt.Errorf("content: компактификация: пак %d не существует (новейший %d)", packID, maxID)
	}
	return nil
}

// liveContents — число живых строк, указывающих в пак (контроль перед
// удалением файла).
func (s *PackStore) liveContents(ctx context.Context, packID int32) (int64, error) {
	rows, err := s.pool.Query(ctx, sqlCompactLive, packID)
	if err != nil {
		return 0, fmt.Errorf("content: контроль живых строк пака %d: %w", packID, err)
	}
	defer rows.Close()
	var n int64
	switch {
	case rows.Next():
		if err := rows.Scan(&n); err != nil {
			return 0, fmt.Errorf("content: контроль живых строк пака %d: %w", packID, err)
		}
	case rows.Err() != nil:
		return 0, fmt.Errorf("content: контроль живых строк пака %d: %w", packID, rows.Err())
	}
	return n, nil
}

// allContents — число всех строк (живых и мёртвых), указывающих в пак:
// скан «пустого» пака без заголовка (C-7).
func (s *PackStore) allContents(ctx context.Context, packID int32) (int64, error) {
	rows, err := s.pool.Query(ctx, sqlCompactAll, packID)
	if err != nil {
		return 0, fmt.Errorf("content: контроль строк пака %d: %w", packID, err)
	}
	defer rows.Close()
	var n int64
	switch {
	case rows.Next():
		if err := rows.Scan(&n); err != nil {
			return 0, fmt.Errorf("content: контроль строк пака %d: %w", packID, err)
		}
	case rows.Err() != nil:
		return 0, fmt.Errorf("content: контроль строк пака %d: %w", packID, rows.Err())
	}
	return n, nil
}

// compactPackList — id паков каталога по возрастанию и новейший
// (0 — паков нет). Реестр паков — множество файлов (T-042).
func compactPackList(dir string) ([]int32, int32, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, fmt.Errorf("content: каталог паков %s: %w", dir, err)
	}
	var ids []int32
	for _, e := range ents {
		if id, ok := packIDFromName(e.Name()); ok {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	var newest int32
	if len(ids) > 0 {
		newest = ids[len(ids)-1]
	}
	return ids, newest, nil
}

// CompactOnce — один проход: скан всех паков, кроме новейшего, правка
// тех, чья доля живых данных ниже порога (строго, ТЗ 5.7). Ошибка
// одного пака не останавливает проход — собирается в итоговый error
// (вызывающий цикл логирует Warn), остальные паки обрабатываются.
func CompactOnce(ctx context.Context, st *PackStore, opts CompactOptions) (CompactStats, error) {
	if st == nil {
		return CompactStats{}, errors.New("content: компактификация: хранилище не задано")
	}
	o := opts.withDefaults()
	ids, newest, err := compactPackList(st.opts.Dir)
	if err != nil {
		return CompactStats{}, err
	}
	var (
		stats CompactStats
		errs  []error
	)
	for _, id := range ids {
		if id == newest {
			break // новейший закрыт для компактификации
		}
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		stats.Packs++
		info, err := st.CompactScan(ctx, id)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if info.Ratio >= o.Threshold {
			continue
		}
		res, err := st.CompactPack(ctx, id)
		// Пак может быть скомпактифицирован и удалён даже при ошибке
		// (расхождения целостности нечитаемых блоков собираются в неё) —
		// освобождённое место и правка учитываются в любом случае.
		if res.Removed {
			stats.Compacted++
			stats.FreedBytes += res.FreedBytes
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
	}
	return stats, errors.Join(errs...)
}

// CompactLoop — фоновая компактификация (ТЗ 5.7): проходы CompactOnce
// по ticker (первый — сразу: дренаж накопленного за простой) до отмены
// ctx, возврат nil — контракт OnRun каркаса сервисов (T-012; стиль
// RecheckDaily и ingest.CleanupDeleted). Ошибка прохода не останавливает
// цикл: Warn, очередной проход повторит — проход идемпотентен (ТЗ 3.4).
func CompactLoop(ctx context.Context, st *PackStore, opts CompactOptions) error {
	if st == nil {
		return errors.New("content: компактификация: хранилище не задано")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("фоновая компактификация паков",
		"interval", o.Interval.String(),
		"threshold", o.Threshold)

	pass := func() {
		stats, err := CompactOnce(ctx, st, o)
		switch {
		case err != nil && ctx.Err() != nil:
			// Отмена во время прохода — часть остановки, не тревога.
		case err != nil:
			log.Warn("проход компактификации паков", "err", err)
		case stats.Compacted > 0:
			log.Info("проход компактификации паков",
				"packs", stats.Compacted, "freed_bytes", stats.FreedBytes)
		}
	}

	pass()
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pass()
		}
	}
}

// Пак-файлы — физический слой хранилища контента (ТЗ 5.7, задача T-042).
//
// Каталог content.dir хранит пак-файлы pack-NNNNNN.pack: append-only
// контейнеры сжатых zstd-блоков (словарь — T-043, CompressWithDict).
// Метаданные блоков — строки PostgreSQL contents(hash, pack_id, off, len,
// raw_len, refcount) (ТЗ 6.1). Возвращаемая реализация удовлетворяет
// ContentStore (T-041); формат и контракты зафиксированы в docs/content.md.
//
// pack_id — порядковый номер файла, реестр паков — само множество файлов
// (заголовок каждого пака дублирует его id): таблицы packs в ТЗ 6.1 нет,
// миграция не вводится. Новейший пак — максимальный id в каталоге.
//
// Транзакционность записи (ТЗ 3.4): блок дописывается в пак и
// синхронизируется на диск (fsync) ДО фиксации строки метаданных. Крах
// между шагами оставляет в паке мусорный блок без строки — он невидим
// (Get идёт только по метаданным) и вычищается компактификацией
// (T-045); обратное окно «строка durable, байтов нет» закрыто: при
// потере питания записанная строка всегда имеет байты на диске
// (PostgreSQL в боевом конфиге коммитит асинхронно — synchronous_commit
// = off, dirty-страницы пака жили бы в кэше минуты).
package content

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/db"
)

// Параметры пак-файлов. Ротация — по content.pack_size (рекомендация
// ТЗ 5.7: 512 МБ–1 ГБ; границы для ops-слоя валидирует конфиг, T-006).
const (
	// DefaultPackSize — порог ротации при нулевом PackOptions.PackSize:
	// 512 МиБ, как content.pack_size по умолчанию.
	DefaultPackSize = 512 << 20

	// packMagic/packVersion — заголовок пак-файла (формат v1).
	packMagic   = "CDPK"
	packVersion = 1

	// packHeaderLen — длина заголовка v1: magic[4] version[1] hdrLen[2]
	// packID[4] maxSize[8] dictID[4], все целые — little-endian.
	packHeaderLen = 4 + 1 + 2 + 4 + 8 + 4

	// packLockFile — файл межпроцессной блокировки каталога паков:
	// http-worker и render-worker дописывают контент одновременно (ТЗ 4.1).
	packLockFile = ".packs.lock"
)

// errPackClosed — операции записи после Close: программная ошибка,
// в контракт ContentStore не входит.
var errPackClosed = errors.New("content: хранилище паков закрыто")

// SQL метаданных contents (ТЗ 6.1). Точные тексты закреплены тестами
// (pack_test.go, стиль ingest-тестов на фейковом pgx-пуле).
const (
	// Дедупликация Put: живой контент уже в хранилище — только ссылка,
	// байты не пишутся. Мёртвая строка (refcount = 0) не воскрешается
	// этим путём: её блок мог быть скомпактифицирован, новый Put пишет
	// свежие байты (sqlMetaPut).
	sqlMetaAddRef = "UPDATE contents SET refcount = refcount + 1 " +
		"WHERE hash = $1 AND refcount > 0"

	// Метаданные нового блока. Конфликт хэша — гонка конкурентов Put
	// одного контента или мёртвая строка (refcount = 0): опоздавший
	// добавляет ссылку, размещение переводится на только что дописанный
	// блок (EXCLUDED — его байты гарантированно на месте, блок прежнего
	// размещенерия мог быть скомпактифицирован), блок проигравшего —
	// мусор для T-045.
	sqlMetaPut = "INSERT INTO contents (hash, pack_id, off, len, raw_len, refcount) " +
		"VALUES ($1, $2, $3, $4, $5, 1) " +
		"ON CONFLICT (hash) DO UPDATE SET refcount = contents.refcount + 1, " +
		"pack_id = EXCLUDED.pack_id, off = EXCLUDED.off, " +
		"len = EXCLUDED.len, raw_len = EXCLUDED.raw_len"

	// Метаданные для чтения: мёртвые строки (refcount = 0) невидимы —
	// контракт Delete (T-041): после последней ссылки контент недоступен.
	sqlMetaGet = "SELECT pack_id, off, len, raw_len FROM contents " +
		"WHERE hash = $1 AND refcount > 0"

	// Снятие ссылки Delete: refcount = 0 строка остаётся «мёртвой» —
	// физическое освобождение места за компактификацией (T-045).
	sqlMetaDelRef = "UPDATE contents SET refcount = refcount - 1 " +
		"WHERE hash = $1 AND refcount > 0"
)

// PackOptions — параметры PackStore из секции content конфигурации
// (T-006). Нулевой PackSize — DefaultPackSize; нулевой MaxStoreBytes —
// без лимита (конфиг задаёт его всегда, T-046).
type PackOptions struct {
	Dir           string // каталог пак-файлов (content.dir)
	PackSize      int64  // порог ротации, байты (content.pack_size)
	MaxStoreBytes int64  // лимит одного контента, байты (content.max_store_bytes)
	// DiskFreePercent — порог свободного места каталога контента, %
	// (content.disk_free_percent, ТЗ 8): ниже порога Put возвращает
	// ErrStoreFull — пауза записи контента; 0 — контроль выключен
	// (конфиг задаёт его всегда, T-046).
	DiskFreePercent float64
	// OnStoreFull — хук алерта T-080: типизированные события «место
	// ниже порога» от Put, ENOSPC и DiskWatchLoop. nil — не задан.
	OnStoreFull OnStoreFullFunc
}

// PackStore — ContentStore на пак-файлах: блоки в pack-NNNNNN.pack,
// метаданные и счётчики ссылок в PostgreSQL (contents). Put сериализован
// (мьютекс позиции записи + flock каталога между процессами), Get читает
// зафиксированные смещения параллельно без блокировок: паки append-only.
type PackStore struct {
	pool db.DB
	opts PackOptions

	mu       sync.Mutex   // позиция записи и ротация (внутри процесса)
	active   *packFile    // пак для дописывания — всегда новейший (appendBlockLocked)
	lock     *os.File     // flock каталога паков (между процессами)
	dirCache packDirCache // новейший пак с кэшем по mtime каталога
	stat     SpaceChecker // источник statfs для контроля места (nil — OSStatfs; тесты подменяют)
	closed   bool
}

// compile-time: PackStore реализует ContentStore (T-041).
var _ ContentStore = (*PackStore)(nil)

// packFile — открытый на дописывание пак.
type packFile struct {
	id   int32
	f    *os.File
	size int64 // актуализируется stat'ом под блокировкой
}

// packHeader — метаданные пак-файла (v1).
type packHeader struct {
	id      int32  // номер пака = порядковый номер имени файла
	maxSize int64  // порог ротации на момент создания (content.pack_size)
	dictID  uint32 // ID словаря zstd на момент создания (0 — без словаря)
}

// OpenPackStore открывает хранилище в opts.Dir: каталог создаётся,
// файл блокировки — тоже (fail-fast на неустройчивом к записи каталоге);
// сами паки сканируются лениво при первой записи. pool — pgx-пул
// (internal/db), где живут строки contents.
func OpenPackStore(pool db.DB, opts PackOptions) (*PackStore, error) {
	if pool == nil {
		return nil, errors.New("content: пул БД не задан")
	}
	if opts.Dir == "" {
		return nil, errors.New("content: каталог паков не задан (content.dir)")
	}
	if opts.PackSize < 0 {
		return nil, fmt.Errorf("content: pack_size %d отрицателен", opts.PackSize)
	}
	if opts.PackSize == 0 {
		opts.PackSize = DefaultPackSize
	}
	if opts.MaxStoreBytes < 0 {
		return nil, fmt.Errorf("content: max_store_bytes %d отрицателен", opts.MaxStoreBytes)
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("content: каталог паков %s: %w", opts.Dir, err)
	}
	lock, err := os.OpenFile(filepath.Join(opts.Dir, packLockFile), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("content: файл блокировки %s: %w", filepath.Join(opts.Dir, packLockFile), err)
	}
	return &PackStore{pool: pool, opts: opts, lock: lock, stat: OSStatfs{}}, nil
}

// Dir — каталог пак-файлов хранилища (диагностика, тесты, сервисы).
func (s *PackStore) Dir() string { return s.opts.Dir }

// Close закрывает файлы хранилища; Get после Close продолжает работать
// (паки открываются на чтение по запросу), запись — ошибка.
func (s *PackStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var err error
	if s.active != nil {
		if cerr := s.active.f.Close(); cerr != nil && err == nil {
			err = cerr
		}
		s.active = nil
	}
	if cerr := s.lock.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

// Put сохраняет data (контракт — store.go). Дедупликация: хэш уже есть
// среди живых строк contents — добавляется ссылка, байты не пишутся.
// Новый контент: блок дописывается в пак (спул), затем фиксируются
// метаданные — крах между шагами оставляет невидимый мусорный блок.
func (s *PackStore) Put(ctx context.Context, data []byte) (PutResult, error) {
	if err := ctx.Err(); err != nil {
		return PutResult{}, err
	}
	if s.opts.MaxStoreBytes > 0 && int64(len(data)) > s.opts.MaxStoreBytes {
		return PutResult{}, fmt.Errorf("%w: %d байт при лимите %d",
			ErrTooLarge, len(data), s.opts.MaxStoreBytes)
	}
	h := HashOf(data)

	// 1) дедуп: живой контент — только ссылка.
	tag, err := s.pool.Exec(ctx, sqlMetaAddRef, h[:])
	if err != nil {
		return PutResult{}, fmt.Errorf("content: метаданные %s: %w", h, err)
	}
	if tag.RowsAffected() == 1 {
		return PutResult{Hash: h}, nil
	}

	// 1.5) пауза записи контента (ТЗ 8, T-046): свободное место каталога
	// ниже порога — новый блок не пишется, ErrStoreFull + событие алерта.
	// Дедуп выше уже вернулся: ссылки на живой контент и чтение продолжают
	// работать — байты не расходуются.
	if err := s.pauseIfDiskFull(); err != nil {
		return PutResult{}, err
	}

	// 2) спул блока в пак и 3) фиксация метаданных — под одним захватом
	// мьютекса и flock (appendAndMeta): блок и его строка появляются в
	// хранилище атомарно относительно компактификации (T-045), которая
	// удаляет старые паки под той же блокировкой. Комментарий о конфликтах
	// INSERT — в appendAndMeta.
	block := CompressWithDict(data)
	tag, err = s.appendAndMeta(ctx, h, block, len(data))
	if err != nil {
		// ENOSPC при физической записи — последняя линия обороны после
		// порога (wrapPackWriteErr): тоже сигнал алерта (ТЗ 8).
		if errors.Is(err, ErrStoreFull) {
			s.fireStoreFull(s.enospcEvent())
		}
		return PutResult{}, err
	}
	return PutResult{Hash: h, Created: tag.Insert()}, nil
}

// appendAndMeta дописывает блок в новейший пак и фиксирует строку
// метаданных под одним захватом мьютекса и flock каталога (T-045):
// строка появляется в БД не позже снятия блокировки, удерживавшейся при
// дописывании блока, — компактификация, взявшая flock следующим, не может
// счесть блок мусорным и удалить пак до фиксации строки. Ошибка на любом
// шаге оставляет блок мусорным без строки — невидим, вычистит
// компактификация (семантика краш-окна T-042 сохранена); блокировка в
// критической секции удерживается и на время запроса к БД — цена
// корректности межпроцессного протокола, запись контента не является
// горячим путём (страницы приходят с сети медленнее коммита).
//
// INSERT-ветка sqlMetaPut — новый контент (Created), UPDATE-ветка
// конфликта — конкурент успел раньше (блок этого вызова — мусор).
// Конфликт с мёртвой строкой (refcount = 0) tag-ом неотличим: Created
// остаётся false при фактически дописанном свежем блоке — редкий случай,
// влияющий только на метрику доли дедупликации (ТЗ 11.4 п. 3);
// размещение переводится на свежий блок (EXCLUDED), чтение корректно.
func (s *PackStore) appendAndMeta(ctx context.Context, h Hash, block []byte, rawLen int) (pgconn.CommandTag, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return pgconn.CommandTag{}, errPackClosed
	}
	if err := flockDir(s.lock); err != nil {
		return pgconn.CommandTag{}, err
	}
	defer func() { unlockDir(s.lock) }()

	packID, off, err := s.appendBlockLocked(block)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	tag, err := s.pool.Exec(ctx, sqlMetaPut, h[:], packID, off, int32(len(block)), int32(rawLen))
	if err != nil {
		return pgconn.CommandTag{}, fmt.Errorf("content: метаданные %s: %w", h, err)
	}
	return tag, nil
}

// Get возвращает контент по метаданным contents: блок (pack_id, off, len)
// читается из пака и распаковывается. Мёртвые строки (refcount = 0) и
// отсутствующие — ErrNotFound.
func (s *PackStore) Get(ctx context.Context, hash Hash) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, sqlMetaGet, hash[:])
	if err != nil {
		return nil, fmt.Errorf("content: метаданные %s: %w", hash, err)
	}
	defer rows.Close()
	var packID int32
	var off int64
	var clen, rawLen int32
	switch {
	case rows.Next():
		if err := rows.Scan(&packID, &off, &clen, &rawLen); err != nil {
			return nil, fmt.Errorf("content: метаданные %s: %w", hash, err)
		}
	case rows.Err() != nil:
		return nil, fmt.Errorf("content: метаданные %s: %w", hash, rows.Err())
	default:
		return nil, ErrNotFound
	}

	block, err := readPackBlock(s.opts.Dir, packID, off, clen)
	if err != nil {
		return nil, err
	}
	raw, err := DecompressWithDict(block)
	if err != nil {
		return nil, fmt.Errorf("content: %s: распаковка блока пака %d: %w", hash, packID, err)
	}
	if int32(len(raw)) != rawLen {
		return nil, fmt.Errorf("content: %s: блок пака %d повреждён: распаковано %d байт, в метаданных %d",
			hash, packID, len(raw), rawLen)
	}
	return raw, nil
}

// Delete снимает одну ссылку: refcount − 1, строка остаётся (при нуле —
// «мёртвая», невидима для Get). Снимать нечего — ErrNotFound.
func (s *PackStore) Delete(ctx context.Context, hash Hash) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, sqlMetaDelRef, hash[:])
	if err != nil {
		return fmt.Errorf("content: метаданные %s: %w", hash, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// appendBlock дописывает сжатый блок в новейший пак и возвращает
// (pack_id, смещение блока). Один писатель в процессе — мьютекс,
// между процессами — flock каталога: размер пака и номер новейшего
// пересчитываются под блокировкой, поэтому смещения не конфликтуют.
// Используется компактификацией (T-045); Put проходит через
// appendAndMeta, удерживающий ту же блокировку до фиксации метаданных.
func (s *PackStore) appendBlock(block []byte) (int32, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, 0, errPackClosed
	}
	if err := flockDir(s.lock); err != nil {
		return 0, 0, err
	}
	defer func() { unlockDir(s.lock) }()
	return s.appendBlockLocked(block)
}

// appendBlockLocked — ядро дописывания, вызывается под мьютексом и
// flock. Активный пак синхронизируется с новейшим в каталоге: писатели
// дописывают ТОЛЬКО в новейший пак, все остальные закрыты для записи —
// предусловие компактификации (T-045), удаляющей старые паки: множество
// строк не-новейшего пака замораживается в момент появления следующего
// пака, опоздавших дописываний не бывает (в прежней схеме активный пак
// мог отставать от новейшего и старый пак оставался открытым для записи).
func (s *PackStore) appendBlockLocked(block []byte) (int32, int64, error) {
	maxID, err := s.newestPackIDLocked()
	if err != nil {
		return 0, 0, err
	}
	if maxID == 0 {
		maxID = 1 // пустой каталог — создаётся пак номер 1
	}
	if s.active == nil || s.active.id != maxID {
		pf, err := s.openPackForWrite(maxID)
		if err != nil {
			return 0, 0, err
		}
		if s.active != nil {
			_ = s.active.f.Close()
		}
		s.active = pf
	} else {
		// Другой процесс мог дописать пак с момента прошлого вызова.
		st, err := s.active.f.Stat()
		if err != nil {
			return 0, 0, fmt.Errorf("content: пак %d: %w", s.active.id, err)
		}
		s.active.size = st.Size()
	}

	recLen := int64(4 + len(block))
	if s.active.size > packHeaderLen && s.active.size+recLen > s.opts.PackSize {
		if err := s.rotateLocked(recLen); err != nil {
			return 0, 0, err
		}
	}

	off := s.active.size + 4 // запись: [len u32 LE][блок]; off — начало блока
	buf := make([]byte, 4+len(block))
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(block)))
	copy(buf[4:], block)
	if _, err := s.active.f.Write(buf); err != nil {
		return 0, 0, wrapPackWriteErr(err, s.active.id)
	}
	// Блок обязан дожить до диска раньше строки метаданных (ТЗ 3.4,
	// «спул → фиксация»): строку фиксирует appendAndMeta, не отпуская
	// flock, — fsync внутри того же захвата закрывает окно «строка
	// durable, байтов нет» при потере питания (повторный Put такой
	// записи не чинит — дедуп видит живую строку). Запись контента —
	// не горячий путь (docs/content.md): цена — один fsync на блок.
	if err := s.active.f.Sync(); err != nil {
		return 0, 0, wrapPackWriteErr(err, s.active.id)
	}
	s.active.size += recLen
	return s.active.id, off, nil
}

// packDirCache — кэш реестра паков: mtime каталога меняется при создании
// и удалении pak-файлов, но не при дописывании в них, поэтому свежесть
// кэша проверяется одним os.Stat вместо ReadDir на каждую запись.
type packDirCache struct {
	valid   bool
	modTime time.Time
	maxID   int32
}

// newestPackIDLocked — максимальный id пака в каталоге (0 — паков нет)
// с кэшем по mtime каталога; вызывается под мьютексом. Каталог контента
// локальный (ТЗ 5.7: NVMe), mtime наносекундный — событие создания или
// удаления пака не проскочит незамеченным между двумя os.Stat.
func (s *PackStore) newestPackIDLocked() (int32, error) {
	// stat до ReadDir: события между ними лишь лишний раз инвалидируют кэш.
	st, err := os.Stat(s.opts.Dir)
	if err != nil {
		return 0, fmt.Errorf("content: каталог паков %s: %w", s.opts.Dir, err)
	}
	if s.dirCache.valid && !s.dirCache.modTime.Equal(st.ModTime()) {
		s.dirCache.valid = false // реестр паков менялся — пересобрать
	}
	if s.dirCache.valid {
		return s.dirCache.maxID, nil
	}
	maxID, err := s.scanPackIDs()
	if err != nil {
		return 0, err
	}
	s.dirCache = packDirCache{valid: true, modTime: st.ModTime(), maxID: maxID}
	return maxID, nil
}

// rotateLocked вызывается, когда текущий пак полон: переключиться на
// новейший пак каталога (другой процесс мог ротировать раньше); если и
// он полон — создать следующий: pack_id монотонно растёт, имя выделяется
// по каталогу под блокировкой. Первый блок пака пишется всегда, даже
// сверх порога, — ротация обязана продвигаться (контент ≤ 16 МиБ
// по валидации max_store_bytes, порог ≥ 1 МиБ).
func (s *PackStore) rotateLocked(recLen int64) error {
	maxID, err := s.newestPackIDLocked()
	if err != nil {
		return err
	}
	if s.active.id != maxID {
		pf, err := s.openPackForWrite(maxID)
		if err != nil {
			return err
		}
		_ = s.active.f.Close()
		s.active = pf
	}
	if s.active.size <= packHeaderLen || s.active.size+recLen <= s.opts.PackSize {
		return nil // новейший пак вместил запись
	}
	pf, err := s.createPackLocked(maxID + 1)
	if err != nil {
		return err
	}
	s.dirCache.valid = false // создан новый пак — реестр изменился
	_ = s.active.f.Close()
	s.active = pf
	return nil
}

// createPackLocked создаёт пак с заголовком. Файл может существовать
// пустым (крах после create до записи заголовка) — заголовок дописывается;
// несовместимое содержимое — ошибка.
func (s *PackStore) createPackLocked(id int32) (*packFile, error) {
	pf, err := s.openPackForWrite(id)
	if err != nil {
		return nil, err
	}
	if pf.size != packHeaderLen {
		return nil, fmt.Errorf("content: пак %d: файл уже содержит %d байт, ожидался только заголовок",
			id, pf.size)
	}
	return pf, nil
}

// openPackForWrite открывает (или создаёт) пак на дописывание и
// приводит заголовок в порядок: пустой файл получает заголовок v1,
// непустой валидируется (версия и совпадение id с именем файла).
func (s *PackStore) openPackForWrite(id int32) (*packFile, error) {
	path := filepath.Join(s.opts.Dir, packFileName(id))
	// O_APPEND: дописывание всегда в конец файла независимо от позиции
	// fd — размер другого процесса не искажает запись.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("content: пак %d: %w", id, err)
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("content: пак %d: %w", id, err)
	}
	hdr := packHeader{id: id, maxSize: s.opts.PackSize, dictID: activeDictID()}
	if st.Size() == 0 {
		// Пустой файл (только что создан либо крах до заголовка): конец
		// файла — смещение 0, обычный Write и есть запись заголовка
		// (WriteAt на O_APPEND-дескрипторе запрещён Go).
		if _, err := f.Write(hdr.bytes()); err != nil {
			_ = f.Close()
			return nil, wrapPackWriteErr(err, id)
		}
		// Заголовок и запись каталога о файле — на диск до любых строк
		// метаданных этого пака: без синка каталога созданный файл может
		// не пережить потерю питания вместе с первыми строками (C-1).
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, wrapPackWriteErr(err, id)
		}
		if err := syncDir(s.opts.Dir); err != nil {
			_ = f.Close()
			return nil, wrapPackWriteErr(err, id)
		}
		return &packFile{id: id, f: f, size: packHeaderLen}, nil
	}
	got, err := readPackHeader(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("content: пак %d: %w", id, err)
	}
	if got.id != id {
		_ = f.Close()
		return nil, fmt.Errorf("content: пак %d: заголовок называет пак %d", id, got.id)
	}
	return &packFile{id: id, f: f, size: st.Size()}, nil
}

// scanPackIDs возвращает максимальный id пака в каталоге (0 — паков нет).
func (s *PackStore) scanPackIDs() (int32, error) {
	ents, err := os.ReadDir(s.opts.Dir)
	if err != nil {
		return 0, fmt.Errorf("content: каталог паков %s: %w", s.opts.Dir, err)
	}
	var max int32
	for _, e := range ents {
		if id, ok := packIDFromName(e.Name()); ok && id > max {
			max = id
		}
	}
	return max, nil
}

// packFileName — имя пак-файла: pack-NNNNNN.pack (порядковый номер —
// pack_id, реестр паков — множество имён плюс заголовок файла).
func packFileName(id int32) string {
	return fmt.Sprintf("pack-%06d.pack", id)
}

// packIDFromName разбирает имя пак-файла; чужие файлы (словарь, замок,
// временные) отвергаются.
func packIDFromName(name string) (int32, bool) {
	const prefix, suffix = "pack-", ".pack"
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return 0, false
	}
	n, err := strconv.Atoi(name[len(prefix) : len(name)-len(suffix)])
	if err != nil || n < 1 || int64(n) > 1<<31-1 {
		return 0, false
	}
	return int32(n), true
}

// bytes кодирует заголовок v1 (little-endian).
func (h packHeader) bytes() []byte {
	b := make([]byte, 0, packHeaderLen)
	b = append(b, packMagic...)
	b = append(b, packVersion)
	b = binary.LittleEndian.AppendUint16(b, packHeaderLen)
	b = binary.LittleEndian.AppendUint32(b, uint32(h.id))
	b = binary.LittleEndian.AppendUint64(b, uint64(h.maxSize))
	b = binary.LittleEndian.AppendUint32(b, h.dictID)
	return b
}

// readPackHeader читает и разбирает заголовок пака.
func readPackHeader(f *os.File) (packHeader, error) {
	buf := make([]byte, packHeaderLen)
	if _, err := f.ReadAt(buf, 0); err != nil {
		return packHeader{}, err
	}
	return parsePackHeader(buf)
}

// parsePackHeader разбирает заголовок v1; hdrLen допускается больше
// packHeaderLen (будущие версии дописывают поля в конец), но не больше
// прочитанного буфера.
func parsePackHeader(b []byte) (packHeader, error) {
	bad := errors.New("заголовок пака нечитаем")
	if len(b) < packHeaderLen || string(b[:len(packMagic)]) != packMagic {
		return packHeader{}, bad
	}
	if b[4] != packVersion {
		return packHeader{}, fmt.Errorf("версия пака %d не поддерживается (поддерживается %d)", b[4], packVersion)
	}
	hdrLen := binary.LittleEndian.Uint16(b[5:7])
	if int(hdrLen) < packHeaderLen || int(hdrLen) > len(b) {
		return packHeader{}, bad
	}
	return packHeader{
		id:      int32(binary.LittleEndian.Uint32(b[7:11])),
		maxSize: int64(binary.LittleEndian.Uint64(b[11:19])),
		dictID:  binary.LittleEndian.Uint32(b[19:23]),
	}, nil
}

// readPackBlock читает clen байт блока из пака packID со смещения off.
func readPackBlock(dir string, packID int32, off int64, clen int32) ([]byte, error) {
	f, err := os.Open(filepath.Join(dir, packFileName(packID)))
	if err != nil {
		return nil, fmt.Errorf("content: пак %d: %w", packID, err)
	}
	defer func() { _ = f.Close() }() // только чтение
	buf := make([]byte, clen)
	if _, err := f.ReadAt(buf, off); err != nil {
		return nil, fmt.Errorf("content: пак %d, смещение %d: %w", packID, off, err)
	}
	return buf, nil
}

// syncDir синхронизирует каталог: запись о создании/удалении файла
// (заголовок нового пака, unlink компактификации) обязана дожить до
// диска вместе с содержимым файла.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}

// wrapPackWriteErr классифицирует ошибки записи пака: кончившееся место
// на диске — ErrStoreFull (ТЗ 8; проактивный порог и пауза записи — T-046),
// дубль сохраняет исходную ошибку для errors.Is.
func wrapPackWriteErr(err error, packID int32) error {
	if errors.Is(err, syscall.ENOSPC) {
		return fmt.Errorf("%w: пак %d: %w", ErrStoreFull, packID, err)
	}
	return fmt.Errorf("content: запись пака %d: %w", packID, err)
}

// flockDir/unlockDir — захват/освобождение межпроцессной блокировки
// каталога паков: писатели (T-042) и компактификация (T-045).
func flockDir(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("content: блокировка каталога паков: %w", err)
	}
	return nil
}

func unlockDir(f *os.File) {
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}

// activeDictID — ID активного словаря пакета (0 — без словаря) для
// заголовка создаваемого пака.
func activeDictID() uint32 {
	dictMu.RLock()
	defer dictMu.RUnlock()
	if activeD == nil {
		return 0
	}
	return activeD.ID()
}

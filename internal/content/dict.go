// Обучаемый словарь zstd (ТЗ 5.7, задача T-043).
//
// Сжатие маленьких HTML-страниц (парковки, заглушки хостингов — основной
// контент краулера) гораздо выгоднее с обученным словарём: типовой шаблон
// попадает в словарь и не кодируется в каждом кадре. Обучение — чистая
// реализация пакета klauspost/compress (zstd.BuildDict, есть с v1.18):
// внешний бинарник `zstd --train` и новые зависимости не нужны. Словарь
// переобучается по требованию командой crawlerctl dict train и кладётся
// в файл по пути content.zstd_dictionary.

package content

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Параметры обучения. Лимиты защищают разовую CLI-операцию переобучения
// от неограниченной памяти: словарь строится по подвыборке корпуса, которой
// для устойчивой статистики шаблонов достаточно с запасом.
const (
	// DefaultDictSize — целевой размер содержимого словаря: 110 KiB, как
	// maxdict по умолчанию у `zstd --train`.
	DefaultDictSize = 110 << 10
	// MinDictSize/MaxDictSize — границы TrainOptions.DictSize.
	MinDictSize = 1 << 10
	MaxDictSize = 1 << 20

	// trainSegmentSize — длина сегмента-кандидата: короткие повторяющиеся
	// фрагменты HTML (теги, атрибуты, типовые скрипты) укладываются в 16 байт,
	// более длинные собираются из соседних сегментов без пропусков.
	trainSegmentSize = 16
	// trainMinSegmentFreq — сегмент-кандидат обязан встретиться минимум дважды:
	// одиночные вхождения словарю не помогают.
	trainMinSegmentFreq = 2
	// trainScanLimit — объём корпуса, по которому считаются частоты сегментов.
	trainScanLimit = 4 << 20
	// trainMaxEntries — запоминаемых вхождений сегмента: берётся первое,
	// не пересекающееся с уже выбранными.
	trainMaxEntries = 4
	// DefaultTrainCorpusLimit — суммарный объём образцов, отдаваемых
	// zstd.BuildDict (статистика литералов и последовательностей).
	DefaultTrainCorpusLimit = 64 << 20
)

// trainEntry — вхождение сегмента обучения (образец, смещение).
type trainEntry struct{ sample, off int }

// ---------------------------------------------------------------- словарь

// Dict — обученный словарь zstd и кодеки, настроенные на него. Удовлетворяет
// формату zstd-словаря (заголовок + таблицы + содержимое), создаётся
// обучением (TrainDict) или загрузкой из файла (LoadDict). Методы
// безопасны для одновременного использования из нескольких горутин:
// EncodeAll/DecodeAll у klauspost/compress не меняют состояние кодеков.
type Dict struct {
	raw []byte
	id  uint32
	enc *zstd.Encoder
	dec *zstd.Decoder
}

// NewDict создаёт кодеки поверх словаря в формате zstd (файл
// `zstd --train`, crawlerctl dict train или LoadDict).
func NewDict(raw []byte) (*Dict, error) {
	insp, err := zstd.InspectDictionary(raw)
	if err != nil {
		return nil, fmt.Errorf("словарь zstd некорректен: %w", err)
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderDict(raw))
	if err != nil {
		return nil, fmt.Errorf("энкодер со словарём: %w", err)
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(raw))
	if err != nil {
		_ = enc.Close() // очистка best effort: основная ошибка — декодер
		return nil, fmt.Errorf("декодер со словарём: %w", err)
	}
	return &Dict{raw: raw, id: insp.ID(), enc: enc, dec: dec}, nil
}

// ID возвращает идентификатор словаря (dictID в заголовке сжатых кадров).
func (d *Dict) ID() uint32 { return d.id }

// Raw возвращает словарь в формате zstd — для записи в файл
// (content.zstd_dictionary) и передачи другим процессам.
func (d *Dict) Raw() []byte { return d.raw }

// Compress сжимает src с обученным словарём. Маленькие страницы того же
// шаблона, по которому обучался словарь, сжимаются заметно лучше Compress.
func (d *Dict) Compress(src []byte) []byte {
	return d.enc.EncodeAll(src, nil)
}

// Decompress распаковывает кадры, сжатые Compress данного словаря; кадры
// без словаря (dictID 0) также читаются — декодер выбирает словарь по
// заголовку кадра.
func (d *Dict) Decompress(src []byte) ([]byte, error) {
	return d.dec.DecodeAll(src, nil)
}

// Save записывает словарь в path атомарно (временный файл в том же
// каталоге + rename): сервис, читающий файл при старте, не увидит
// наполовину записанный словарь переобучения. Данные синхронизируются
// до rename (Close записи на носитель не гарантирует), rename — запись
// в каталоге, поэтому синхронизируется и каталог: иначе сбой питания
// сразу после Save оставляет пустой/частичный словарь, а сервисы
// отказываются стартовать на битом файле (T-043).
func (d *Dict) Save(path string) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*")
	if err != nil {
		return fmt.Errorf("словарь: временный файл: %w", err)
	}
	name := tmp.Name()
	if _, err := tmp.Write(d.raw); err != nil {
		_ = tmp.Close() // очистка best effort: основная ошибка — запись
		_ = os.Remove(name)
		return fmt.Errorf("словарь: запись: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return fmt.Errorf("словарь: синхронизация: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return fmt.Errorf("словарь: запись: %w", err)
	}
	if err := os.Chmod(name, 0o644); err != nil {
		_ = os.Remove(name)
		return fmt.Errorf("словарь: права: %w", err)
	}
	if err := os.Rename(name, path); err != nil {
		_ = os.Remove(name)
		return fmt.Errorf("словарь: переименование: %w", err)
	}
	// Связь имени с файлом живёт в каталоге — best effort: сбой здесь
	// редок, а словарь воспроизводим повторным обучением (train).
	if dh, err := os.Open(dir); err == nil {
		_ = dh.Sync()
		_ = dh.Close()
	}
	return nil
}

// LoadDict читает словарь из файла (формат — как у `zstd --train`).
// Отсутствие файла не оборачивается: errors.Is(err, fs.ErrNotExist).
func LoadDict(path string) (*Dict, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	d, err := NewDict(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return d, nil
}

// ---------------------------------------------------------------- обучение

// TrainOptions — параметры обучения TrainDict. Нулевое DictSize —
// DefaultDictSize.
type TrainOptions struct {
	// DictSize — целевой размер содержимого словаря, байты
	// (MinDictSize..MaxDictSize). Фактический размер может быть меньше,
	// если повторяющихся фрагментов в корпусе мало.
	DictSize int
}

// TrainDict обучает словарь zstd по образцам (HTML-страницы) и возвращает
// готовый к использованию Dict.
//
// Содержимое словаря (History в терминах zstd.BuildDict) собирается из
// наиболее часто повторяющихся сегментов корпуса: жадный отбор по убыванию
// частоты, внутри одного образца выбранные вхождения не пересекаются —
// длинный повтор целиком набирается соседними сегментами без дублирования.
// Статистические таблицы словаря (литералы, последовательности) строят
// самой библиотекой zstd.BuildDict по всему корпусу — формат словаря
// стандартный, кадры читает и реальный zstd.
func TrainDict(samples [][]byte, opts TrainOptions) (*Dict, error) {
	dictSize := opts.DictSize
	if dictSize == 0 {
		dictSize = DefaultDictSize
	}
	if dictSize < MinDictSize || dictSize > MaxDictSize {
		return nil, fmt.Errorf("dict train: размер словаря %d вне диапазона %d..%d байт",
			dictSize, MinDictSize, MaxDictSize)
	}
	contents := make([][]byte, 0, len(samples))
	var total int
	for _, s := range samples {
		if total += len(s); total > DefaultTrainCorpusLimit {
			break
		}
		contents = append(contents, s)
	}
	if len(contents) == 0 {
		return nil, errors.New("dict train: корпус образцов пуст")
	}

	history := trainHistory(contents, dictSize)
	if len(history) < 8 {
		return nil, errors.New("dict train: в корпусе нет повторяющихся фрагментов — словарь не обучается")
	}

	// ID словаря — детерминированная функция содержимого: один и тот же
	// корпус даёт один и тот же ID, разные версии словаря различаются
	// (декодер выбирает словарь из зарегистрированных по dictID кадра).
	// ID 0 форматом запрещён.
	id := fnv.New32a()
	_, _ = id.Write(history)
	dictID := id.Sum32()
	if dictID == 0 {
		dictID = 1
	}

	raw, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       dictID,
		Contents: contents,
		History:  history,
		Level:    zstd.SpeedDefault,
	})
	if err != nil {
		return nil, fmt.Errorf("dict train: построение словаря: %w", err)
	}
	return NewDict(raw)
}

// trainHistory отбирает содержимое словаря: сегменты trainSegmentSize байт
// с частотой ≥ trainMinSegmentFreq, по убыванию частоты; вхождение сегмента
// в образец берётся, только если оно не пересекается с уже выбранными
// вхождениями того же образца. Детерминировано: порядок кандидатов полный
// (частота, затем лексикографический), обход корпуса — в порядке образцов.
func trainHistory(samples [][]byte, dictSize int) []byte {
	type info struct {
		freq    int
		entries [trainMaxEntries]trainEntry
		n       int
	}

	// Фаза 1: частоты сегментов по префиксу корпуса (до trainScanLimit).
	counts := make(map[string]*info)
	var scanned int
	for si, s := range samples {
		if scanned >= trainScanLimit {
			break
		}
		limit := len(s) - trainSegmentSize + 1
		if limit <= 0 {
			continue
		}
		if remain := trainScanLimit - scanned; len(s) > remain {
			limit = remain - trainSegmentSize + 1
		}
		for off := 0; off < limit; off++ {
			seg := s[off : off+trainSegmentSize]
			inf := counts[string(seg)] // lookup без аллокации
			if inf == nil {
				inf = &info{}
				counts[string(seg)] = inf
			}
			inf.freq++
			if inf.n < trainMaxEntries {
				inf.entries[inf.n] = trainEntry{si, off}
				inf.n++
			}
		}
		scanned += len(s)
	}

	// Фаза 2: жадный отбор непересекающихся вхождений.
	type candidate struct {
		seg string
		inf *info
	}
	cands := make([]candidate, 0, len(counts))
	for seg, inf := range counts {
		if inf.freq >= trainMinSegmentFreq {
			cands = append(cands, candidate{seg, inf})
		}
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].inf.freq != cands[j].inf.freq {
			return cands[i].inf.freq > cands[j].inf.freq
		}
		return cands[i].seg < cands[j].seg
	})

	taken := make([][]trainEntry, len(samples)) // занятые диапазоны по образцам
	history := make([]byte, 0, dictSize)
	for _, c := range cands {
		if len(history) >= dictSize {
			break
		}
		for k := 0; k < c.inf.n; k++ {
			e := c.inf.entries[k]
			if overlapsSegment(taken[e.sample], e.off) {
				continue
			}
			taken[e.sample] = append(taken[e.sample], e)
			history = append(history, c.seg...)
			break
		}
	}
	return history
}

// overlapsSegment сообщает, пересекает ли сегмент с началом off какой-нибудь
// из уже выбранных диапазонов длинной trainSegmentSize.
func overlapsSegment(taken []trainEntry, off int) bool {
	for _, e := range taken {
		if off < e.off+trainSegmentSize && e.off < off+trainSegmentSize {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------- активный словарь пакета

// Активный словарь — один на процесс (путь из content.zstd_dictionary).
// Смена словаря на лету не предусматривается: переобученный файл
// подхватывается при следующей инициализации (SetDictFile), старый словарь
// остаётся нужным для распаковки записей, сжатых им (T-042 зафиксирует
// хранение dict_id в метаданных паков).
var (
	dictMu  sync.RWMutex
	activeD *Dict
)

// SetDict устанавливает словарь для CompressWithDict/DecompressWithDict.
// nil — отключает (сжатие без словаря).
func SetDict(d *Dict) {
	dictMu.Lock()
	activeD = d
	dictMu.Unlock()
}

// SetDictFile загружает словарь из файла (путь из content.zstd_dictionary).
// Пустой путь или отсутствующий файл — не ошибка: возвращается false,
// сервис работает без словаря (Compress/CompressWithDict одинаково падают
// в каркасное сжатие); отсутствие отмечается в логе. Битый файл — ошибка:
// молчаливое сжатие без словаря скрыло бы проблему конфигурации.
func SetDictFile(path string, log *slog.Logger) (bool, error) {
	if path == "" {
		SetDict(nil)
		return false, nil
	}
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		if log != nil {
			log.Info("словарь zstd не найден, сжатие контента без словаря", "path", path)
		}
		SetDict(nil)
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("словарь zstd: %w", err)
	}
	d, err := NewDict(raw)
	if err != nil {
		return false, fmt.Errorf("словарь zstd %s: %w", path, err)
	}
	SetDict(d)
	if log != nil {
		log.Info("словарь zstd загружен", "path", path, "id", d.ID(), "size", len(raw))
	}
	return true, nil
}

// WarnDictMismatch сверяет активный словарь пакета с dictID заголовка
// новейшего пака каталога dir — диагностика при старте сервиса (T-043).
// Декодер распаковывает кадры только зарегистрированным в процессе
// словарём (SetDict/SetDictFile): после переобучения кадры, сжатые
// прежним словарём, нечитаемы, и расхождение показывается оператору
// сразу (Warn), а не списком ошибок чтения контента. Паков нет или
// заголовок новейшего нечитаем — молча: это не расхождение словаря.
// Реестра словарей в хранилище нет (docs/configuration.md) — Warn
// призыв вручную вернуть прежний файл словаря.
func WarnDictMismatch(dir string, log *slog.Logger) {
	if log == nil || dir == "" {
		return
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	var max int32
	for _, e := range ents {
		if id, ok := packIDFromName(e.Name()); ok && id > max {
			max = id
		}
	}
	if max == 0 {
		return // паков нет — сверять не с чем
	}
	f, err := os.Open(filepath.Join(dir, packFileName(max)))
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()
	hdr, err := readPackHeader(f)
	if err != nil {
		return
	}
	if id := activeDictID(); id != hdr.dictID {
		log.Warn("словарь zstd не соответствует новейшему паку — кадры прежнего словаря нечитаемы (верните прежний файл словаря)",
			"pack", max, "pack_dict_id", fmt.Sprintf("%08x", hdr.dictID), "active_dict_id", fmt.Sprintf("%08x", id))
	}
}

// CompressWithDict сжимает src активным словарём пакета (SetDict /
// SetDictFile); без установленного словаря — то же, что Compress:
// запись контента не зависит от того, обучен ли уже словарь. Точка
// подключения хранилища контента (T-042).
func CompressWithDict(src []byte) []byte {
	dictMu.RLock()
	d := activeD
	dictMu.RUnlock()
	if d == nil {
		return Compress(src)
	}
	return d.Compress(src)
}

// DecompressWithDict распаковывает кадры, сжатые CompressWithDict
// (активным словарём на момент сжатия) и Compress.
func DecompressWithDict(src []byte) ([]byte, error) {
	dictMu.RLock()
	d := activeD
	dictMu.RUnlock()
	if d == nil {
		return Decompress(src)
	}
	return d.Decompress(src)
}

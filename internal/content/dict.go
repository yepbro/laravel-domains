// The trainable zstd dictionary (spec 5.7, task T-043; the registry of
// retired dictionaries is T-103).
//
// Compressing small HTML pages (parking pages, hosting suspension pages —
// the crawler's main content) is far more efficient with a trained
// dictionary: the typical template lands in the dictionary and is not
// encoded in every frame. Training is a pure implementation on top of the
// klauspost/compress package (zstd.BuildDict, available since v1.18):
// no external `zstd --train` binary and no new dependencies are needed.
// The dictionary is retrained on demand by the crawlerctl dict train
// command and placed into the file at the content.zstd_dictionary path;
// the retired dictionary is archived into the registry at the same time
// (dict-<id>.zstd next to the active file) — already written content
// stays readable (T-103).

package content

import (
	"encoding/binary"
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

// Training parameters. The limits protect the one-off CLI retraining
// operation from unbounded memory: the dictionary is built from a corpus
// subsample that is amply sufficient for stable template statistics.
const (
	// DefaultDictSize — the target size of the dictionary contents: 110 KiB, like
	// maxdict's default for `zstd --train`.
	DefaultDictSize = 110 << 10
	// MinDictSize/MaxDictSize — the bounds of TrainOptions.DictSize.
	MinDictSize = 1 << 10
	MaxDictSize = 1 << 20

	// trainSegmentSize — the length of a candidate segment: short repeating
	// HTML fragments (tags, attributes, typical scripts) fit into 16 bytes,
	// longer ones are assembled from adjacent segments without gaps.
	trainSegmentSize = 16
	// trainMinSegmentFreq — a candidate segment must occur at least twice:
	// single occurrences do not help the dictionary.
	trainMinSegmentFreq = 2
	// trainScanLimit — the amount of corpus over which segment frequencies
	// are counted.
	trainScanLimit = 4 << 20
	// trainMaxEntries — segment occurrences remembered: the first one
	// that does not overlap the already selected ones is taken.
	trainMaxEntries = 4
	// DefaultTrainCorpusLimit — the total size of samples handed to
	// zstd.BuildDict (literal and sequence statistics).
	DefaultTrainCorpusLimit = 64 << 20
)

// trainEntry — a training segment occurrence (sample, offset).
type trainEntry struct{ sample, off int }

// ---------------------------------------------------------------- dictionary

// Dict — a trained zstd dictionary and the codecs configured for it. It satisfies
// the zstd dictionary format (header + tables + contents); it is created
// by training (TrainDict) or by loading from a file (LoadDict). The methods
// are safe for concurrent use from multiple goroutines:
// klauspost/compress EncodeAll/DecodeAll do not mutate the codecs.
type Dict struct {
	raw []byte
	id  uint32
	enc *zstd.Encoder
	dec *zstd.Decoder
}

// NewDict creates codecs on top of a dictionary in zstd format (a
// `zstd --train` file, crawlerctl dict train, or LoadDict).
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
		_ = enc.Close() // best-effort cleanup: the primary error is the decoder
		return nil, fmt.Errorf("декодер со словарём: %w", err)
	}
	return &Dict{raw: raw, id: insp.ID(), enc: enc, dec: dec}, nil
}

// ID returns the dictionary identifier (dictID in the header of compressed frames).
func (d *Dict) ID() uint32 { return d.id }

// Raw returns the dictionary in zstd format — for writing it to a file
// (content.zstd_dictionary) and passing it to other processes.
func (d *Dict) Raw() []byte { return d.raw }

// Compress compresses src with the trained dictionary. Small pages of the same
// template the dictionary was trained on compress noticeably better than with Compress.
func (d *Dict) Compress(src []byte) []byte {
	return d.enc.EncodeAll(src, nil)
}

// Decompress decompresses frames compressed by this dictionary's Compress; frames
// without a dictionary (dictID 0) are also readable — the decoder picks the dictionary
// by the frame header.
func (d *Dict) Decompress(src []byte) ([]byte, error) {
	return d.dec.DecodeAll(src, nil)
}

// Save writes the dictionary to path atomically (a temporary file in the same
// directory + rename): a service reading the file at startup will not see
// a half-written retrained dictionary. The data is synced
// before the rename (a writer's Close does not guarantee it hits the medium), and rename is
// a directory write, so the directory is synced too: otherwise a power failure
// right after Save leaves an empty/partial dictionary, and services
// refuse to start on a broken file (T-043).
func (d *Dict) Save(path string) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*")
	if err != nil {
		return fmt.Errorf("словарь: временный файл: %w", err)
	}
	name := tmp.Name()
	if _, err := tmp.Write(d.raw); err != nil {
		_ = tmp.Close() // best-effort cleanup: the primary error is the write
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
	// The name-to-file link lives in the directory — best effort: a failure here
	// is rare, and the dictionary is reproducible by retraining (train).
	if dh, err := os.Open(dir); err == nil {
		_ = dh.Sync()
		_ = dh.Close()
	}
	return nil
}

// LoadDict reads the dictionary from a file (the format is the same as
// `zstd --train`). A missing file is not wrapped: errors.Is(err, fs.ErrNotExist).
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

// ---------------------------------------------------------------- training

// TrainOptions — TrainDict training parameters. A zero DictSize means
// DefaultDictSize.
type TrainOptions struct {
	// DictSize — the target size of the dictionary contents, bytes
	// (MinDictSize..MaxDictSize). The actual size may be smaller
	// if the corpus has few repeating fragments.
	DictSize int
}

// TrainDict trains a zstd dictionary on samples (HTML pages) and returns a
// Dict ready for use.
//
// The dictionary contents (History in zstd.BuildDict terms) are assembled from
// the most frequently repeating corpus segments: a greedy selection by descending
// frequency; within one sample the selected occurrences do not overlap —
// a long repeat is assembled in full from adjacent segments without duplication.
// The dictionary's statistical tables (literals, sequences) are built
// by the zstd.BuildDict library itself over the whole corpus — the dictionary format
// is standard, and real zstd reads the frames too.
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

	// The dictionary ID is a deterministic function of the contents: the same
	// corpus yields the same ID, different dictionary versions differ
	// (the decoder picks a dictionary from the registered ones by the frame's dictID).
	// ID 0 is forbidden by the format.
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

// trainHistory selects the dictionary contents: segments of trainSegmentSize bytes
// with frequency ≥ trainMinSegmentFreq, by descending frequency; a segment occurrence
// in a sample is taken only if it does not overlap the already selected
// occurrences of the same sample. Deterministic: the candidate order is total
// (frequency, then lexicographic), the corpus is traversed in sample order.
func trainHistory(samples [][]byte, dictSize int) []byte {
	type info struct {
		freq    int
		entries [trainMaxEntries]trainEntry
		n       int
	}

	// Phase 1: segment frequencies over a corpus prefix (up to trainScanLimit).
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
			inf := counts[string(seg)] // a lookup without allocation
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

	// Phase 2: greedy selection of non-overlapping occurrences.
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

	taken := make([][]trainEntry, len(samples)) // occupied ranges per sample
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

// overlapsSegment reports whether a segment starting at off overlaps any
// of the already selected ranges of length trainSegmentSize.
func overlapsSegment(taken []trainEntry, off int) bool {
	for _, e := range taken {
		if off < e.off+trainSegmentSize && e.off < off+trainSegmentSize {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------- the package's active dictionary

// The active dictionary is one per process (the path from
// content.zstd_dictionary). Changing the dictionary on the fly is not
// supported: a retrained file is picked up at the next initialization
// (SetDictFile). Retired dictionaries are not lost: crawlerctl dict train
// archives them into the dict-<id>.zstd registry next to the active file
// (RetireDict); reading a block with a dictID foreign to the process lazily
// registers the dictionary from the registry and memoizes it (T-103) —
// retraining does not make written content unreadable.
var (
	dictMu  sync.RWMutex
	activeD *Dict
	// dictDir — the directory of the retired-dictionary registry: the same one
	// the active file lives in (set by SetDictFile). retiredD — memoization of
	// dictionaries loaded from the registry by dictID.
	dictDir  string
	retiredD map[uint32]*Dict
)

// SetDict sets the dictionary for CompressWithDict/DecompressWithDict.
// nil disables it (compression without a dictionary). It does not touch the
// retired-dictionary registry: its directory is determined by the configuration
// file (SetDictFile).
func SetDict(d *Dict) {
	dictMu.Lock()
	activeD = d
	dictMu.Unlock()
}

// SetDictFile loads the dictionary from a file (the path from
// content.zstd_dictionary) and wires up the retired-dictionary registry —
// the registry's directory becomes the directory of the active file. An empty
// path or a missing file is not an error: false is returned and the service
// runs without a dictionary (Compress/CompressWithDict fall back to the
// framework compression identically); the absence is noted in the log, and
// the registry stays wired up — blocks compressed with retired dictionaries
// are readable even without the active file. A broken file is
// an error: silently compressing without a dictionary would hide a configuration
// problem.
func SetDictFile(path string, log *slog.Logger) (bool, error) {
	// A path change resets the registry and the memoization; until the function
	// returns, reads via the old directory are impossible (dictDir is already the new one).
	dictMu.Lock()
	dictDir = ""
	if path != "" {
		dictDir = filepath.Dir(path)
	}
	retiredD = nil
	dictMu.Unlock()
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

// WarnDictMismatch cross-checks the package's active dictionary against the
// dictID of the header of the newest pack of the dir directory — startup
// diagnostics (T-043). A frame of a retired dictionary is read via late
// registration from the registry (T-103): a mismatch with a dictionary archived
// in the registry is Info (readability holds); Warn only if the retired
// dictionary is absent from the registry too
// (the frames of this pack are unreadable, the text names the recovery
// procedure — runbooks.md §1). No packs or an unreadable newest header —
// silently: that is not a dictionary mismatch.
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
		return // no packs — nothing to compare against
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
		packDict, actDict := fmt.Sprintf("%08x", hdr.dictID), fmt.Sprintf("%08x", id)
		dictMu.RLock()
		reg := dictDir
		dictMu.RUnlock()
		if reg != "" {
			// Checking file existence is cheaper than loading codecs at startup:
			// the lazy late registration itself happens on the first read.
			if _, err := os.Stat(DictRegistryPath(reg, hdr.dictID)); err == nil {
				log.Info("словарь zstd новее пака — кадры прежнего словаря читаются через реестр",
					"pack", max, "pack_dict_id", packDict, "active_dict_id", actDict, "registry", reg)
				return
			}
		}
		log.Warn("словарь zstd не соответствует новейшему паку и не найден в реестре прежних словарей — кадры этого словаря нечитаемы (верните файл словаря в реестр, runbooks.md §1)",
			"pack", max, "pack_dict_id", packDict, "active_dict_id", actDict)
	}
}

// CompressWithDict compresses src with the package's active dictionary (SetDict /
// SetDictFile); with no dictionary set it is the same as Compress:
// content writing does not depend on whether the dictionary has been trained
// yet. Writing always uses the active dictionary — the retired-dictionary
// registry does not participate in compression.
func CompressWithDict(src []byte) []byte {
	dictMu.RLock()
	d := activeD
	dictMu.RUnlock()
	if d == nil {
		return Compress(src)
	}
	return d.Compress(src)
}

// DecompressWithDict decompresses frames compressed by CompressWithDict
// (with the dictionary active at compression time) and by Compress. A frame
// compressed by a retired dictionary (after retraining) is late-registered from
// the retired-dictionary registry — a lazy load of dict-<id>.zstd by the frame's
// dictID plus memoization (T-103). No dictionary in the registry — an error with the
// original zstd.ErrUnknownDictionary and the registry path in the text.
func DecompressWithDict(src []byte) ([]byte, error) {
	dictMu.RLock()
	d := activeD
	dictMu.RUnlock()
	var (
		raw []byte
		err error
	)
	if d != nil {
		raw, err = d.Decompress(src)
	} else {
		raw, err = Decompress(src)
	}
	if err == nil || !errors.Is(err, zstd.ErrUnknownDictionary) {
		return raw, err
	}
	// A frame of a dictionary not registered in this process: the dictID comes
	// from the frame header, the dictionary from the retired registry.
	id := frameDictID(src)
	if id == 0 || (d != nil && id == d.ID()) {
		// The header did not parse or the ID matches the active one — this is not
		// a foreign dictionary but frame corruption: the original error is more honest.
		return raw, err
	}
	rd, rerr := dictFromRegistry(id)
	if rerr != nil {
		return nil, rerr
	}
	return rd.Decompress(src)
}

// DictRegistryPath — the path of dictionary id's file in the retired-dictionary
// registry of the dir directory: the registry lives next to the active file
// (the directory of content.zstd_dictionary), the name is derived from the
// dictionary's dictID.
func DictRegistryPath(dir string, id uint32) string {
	return filepath.Join(dir, fmt.Sprintf("dict-%08x.zstd", id))
}

// RetireDict archives a dictionary into the retired-dictionary registry of the dir
// directory (an atomic Save write, T-103): crawlerctl dict train calls it before
// writing the new active file — a failure leaves the retired dictionary
// active. Idempotent: a file of the same ID is overwritten with the same
// contents.
func RetireDict(d *Dict, dir string) error {
	return d.Save(DictRegistryPath(dir, d.ID()))
}

// dictFromRegistry returns the dictionary for a dictID: first the memoization, then
// a lazy load from the registry (the directory is set by SetDictFile); a loaded
// dictionary is remembered for the life of the process — reading old packs
// does not hit the FS for every block. A negative result is not memoized:
// a file placed into the registry on a live service is picked up by the next
// read without a restart.
func dictFromRegistry(id uint32) (*Dict, error) {
	dictMu.RLock()
	d := retiredD[id]
	dir := dictDir
	dictMu.RUnlock()
	if d != nil {
		return d, nil
	}
	if dir == "" {
		return nil, fmt.Errorf("%w: %08x — реестр прежних словарей не подключён (каталог задаётся content.zstd_dictionary)",
			zstd.ErrUnknownDictionary, id)
	}
	path := DictRegistryPath(dir, id)
	d, err := LoadDict(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: словарь %08x отсутствует в реестре (%s) — кадры этого словаря нечитаемы, верните файл в реестр (runbooks.md §1)",
				zstd.ErrUnknownDictionary, id, path)
		}
		return nil, fmt.Errorf("%w: словарь %08x из реестра %s: %s",
			zstd.ErrUnknownDictionary, id, path, err)
	}
	if d.ID() != id {
		return nil, fmt.Errorf("%w: файл реестра %s содержит словарь %08x, кадр требует %08x (имя файла не соответствует содержимому)",
			zstd.ErrUnknownDictionary, path, d.ID(), id)
	}
	dictMu.Lock()
	if retiredD == nil {
		retiredD = make(map[uint32]*Dict)
	}
	retiredD[id] = d
	dictMu.Unlock()
	return d, nil
}

// frameDictID extracts the dictionary ID from a zstd frame header (0 — a frame
// without a dictionary or an unparsable header: a short truncation, foreign bytes).
// The header: magic, the frame descriptor (bits 0x20 — Single_Segment,
// 0x03 — the Dictionary_ID size), an optional window-descriptor byte
// (without Single_Segment), then the ID itself, 1/2/4 bytes. It does not
// return an error: it is called after the decoder failed — the decoder names
// the cause in the end.
func frameDictID(frame []byte) uint32 {
	if len(frame) < 5 || binary.LittleEndian.Uint32(frame[:4]) != 0xFD2FB528 {
		return 0
	}
	var size int
	switch fhd := frame[4]; fhd & 3 {
	case 1:
		size = 1
	case 2:
		size = 2
	case 3:
		size = 4
	}
	if size == 0 {
		return 0 // a frame without a dictionary
	}
	pos := 5
	if frame[4]&0x20 == 0 {
		pos++ // the window descriptor is present without Single_Segment
	}
	if len(frame) < pos+size {
		return 0
	}
	switch size {
	case 1:
		return uint32(frame[pos])
	case 2:
		return uint32(binary.LittleEndian.Uint16(frame[pos:]))
	default:
		return binary.LittleEndian.Uint32(frame[pos:])
	}
}

// Потоковая распаковка входных архивов (T-133; расширение ТЗ 5.1,
// потоковость — ТЗ 3.5). Конвейер файла (pipeline.go) по расширению имени
// ветвится на распаковку: каждый член читается как io.Reader и проходит
// существующие Parse/Validator/Stage — материализации членов на диске нет,
// память — как у txt-конвейера (окно парсера + пачка COPY).
//
// Форматы — stdlib: archive/zip (файл открывается как ReaderAt, размер —
// по факту открытия), archive/tar, compress/gzip; .tar.gz/.tgz — gzip
// поверх tar (двойное открытие); .gz — один член с именем архива без
// расширения. rar/7z не поддерживаются (не stdlib) — отклонённый вариант.
//
// Семантика (T-133): зона и действие — только из каталога архива,
// внутренние пути членов на зону не влияют (члены — списки доменов зоны
// каталога; самостоятельный выбор зоны из пути члена небезопасен) — путь
// члена используется только в адресации rejected-лога «архив!член:строка»;
// каталоги, скрытые файлы и мусор macOS (__MACOSX/, ._*, .DS_Store)
// пропускаются с Warn; вложенные архивы не распаковываются — один уровень:
// член-архив даёт бинарный мусор → доля ошибок → failed.
package ingest

import (
	"archive/tar"
	"archive/zip"
	"compress/flate"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"strings"
)

// DefaultArchiveMaxUncompressed — потолок суммарного несжатого объёма
// членов архива по умолчанию (ingest.archive_max_uncompressed).
// Дублирует config.Default, чтобы конвейер работал и без конфигурации
// (стиль DefaultBatchSize). 8 GiB: несжатая зона .com ≈ 1,5 ГБ — с
// запасом на рост зон и склейки, без чрезмерного окна для zip-бомб.
const DefaultArchiveMaxUncompressed = int64(8 << 30)

// DefaultArchiveMaxFiles — потолок числа записей архива по умолчанию
// (ingest.archive_max_files): реальные дампы зон — единицы файлов.
const DefaultArchiveMaxFiles = 1000

// errArchiveRejected — архив ошибочен по своей сути (битый, превышены
// лимиты, член выше порога ошибок): файл — кандидат в failed/, как
// txt-файл с долей отказов выше ingest.max_error_ratio (ТЗ 5.1);
// обёрнутые причины доступны по цепочке (errors.Is/As). Текст — без
// повтора «ошибочен»: failFile приставляет свой вердикт.
var errArchiveRejected = errors.New("архив не принят")

// archiveKind — формат входного архива по расширению имени (T-133).
type archiveKind uint8

const (
	archiveNone  archiveKind = iota // *.txt — не архив
	archiveZip                      // .zip
	archiveTar                      // .tar
	archiveTarGz                    // .tar.gz, .tgz
	archiveGz                       // .gz без tar-слоя
)

// archiveKindOf — формат по суффиксу имени, без учёта регистра; .tar.gz
// проверяется раньше .gz, .tgz — самостоятельный суффикс.
func archiveKindOf(name string) archiveKind {
	lower := strings.ToLower(name)
	switch {
	case strings.HasSuffix(lower, ".zip"):
		return archiveZip
	case strings.HasSuffix(lower, ".tar.gz"):
		return archiveTarGz
	case strings.HasSuffix(lower, ".tgz"):
		return archiveTarGz
	case strings.HasSuffix(lower, ".tar"):
		return archiveTar
	case strings.HasSuffix(lower, ".gz"):
		return archiveGz
	}
	return archiveNone
}

// isArchiveName — имя входного файла — поддерживаемый архив (T-133).
func isArchiveName(name string) bool { return archiveKindOf(name) != archiveNone }

// gzMemberName — имя единственного члена .gz-архива: имя файла без
// расширения (com/add/zone.gz → zone; T-133).
func gzMemberName(p string) string {
	base := path.Base(p)
	return strings.TrimSuffix(base, path.Ext(base))
}

// junkMember — записи архива без списков доменов (T-133): скрытые файлы
// и каталоги (компонент пути начинается с «.» — включая мусор macOS
// ._*, .DS_Store и скрытые деревья целиком) и ресурсный каталог
// __MACOSX (любой компонент). Внутренние пути на зону не влияют, к
// файловой системе не применяются — имя только адресует строки.
func junkMember(name string) bool {
	for _, part := range strings.Split(name, "/") {
		if part == "" || part == "." {
			continue
		}
		if strings.HasPrefix(part, ".") || part == "__MACOSX" {
			return true
		}
	}
	return false
}

// overErrorRatio — объединённый порог доли ошибок (отказы парсера плюс
// валидатора, как у txt-файла в streamStage): файл без содержательных
// строк ошибочным не считается, равная порогу доля — не роняет (ТЗ 5.1).
func overErrorRatio(ratio float64, validatorTotal, validatorRejected, parserRejected int) bool {
	if total := validatorTotal + parserRejected; total > 0 {
		return float64(validatorRejected+parserRejected)/float64(total) > ratio
	}
	return false
}

// isCorruptRead — ошибка чтения, означающая повреждённые данные архива
// (битый — failed/, T-133), а не сбой инфраструктуры: известные ошибки
// форматов stdlib и обрыв потока. Прочее (ошибки диска) — инфраструктура:
// файл остаётся в incoming (ТЗ 3.4).
func isCorruptRead(err error) bool {
	switch {
	case errors.Is(err, zip.ErrFormat),
		errors.Is(err, zip.ErrChecksum),
		errors.Is(err, zip.ErrAlgorithm),
		errors.Is(err, gzip.ErrHeader),
		errors.Is(err, gzip.ErrChecksum),
		errors.Is(err, tar.ErrHeader),
		errors.Is(err, io.ErrUnexpectedEOF):
		return true
	}
	var fl flate.CorruptInputError
	return errors.As(err, &fl)
}

// archiveOpenErr — ошибка открытия архива (zip.NewReader/gzip.NewReader):
// пустой или мусорный файл — битый архив; сбой чтения диска — нет
// (классификация как у членов).
func archiveOpenErr(f File, err error) error {
	if isCorruptRead(err) || errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: архив %s не читается: %w", errArchiveRejected, f.Path, err)
	}
	return fmt.Errorf("ingest: чтение архива %s: %w", f.Path, err)
}

// --- источник членов --------------------------------------------------------

// member — обрабатываемый член архива: имя (внутренний путь, очищенный
// path.Clean) для адресации rejected-лога и поток содержимого. closer —
// ресурс записи zip (члены tar/gz — потоки распаковщика, закрывать нечего).
type member struct {
	name   string
	r      io.Reader
	closer io.Closer
}

// memberIter — последовательный источник членов архива: записи без
// списков доменов (каталоги, скрытые, мусор macOS) пропускаются внутри
// с Warn; io.EOF — архив исчерпан. Ошибки природы архива промаркированы
// errArchiveRejected, прочие — инфраструктурные.
type memberIter interface {
	next() (member, error)
}

// zipIter — члены zip: заголовки центрального каталога, данные — по
// смещениям через ReaderAt. Несжатый объём члена известен до чтения
// (UncompressedSize64) и проверяется против остатка лимита заранее;
// фактическое чтение тоже считается — заголовок может лгать.
type zipIter struct {
	p      *Pipeline
	f      File
	files  []*zip.File
	i      int
	budget *archiveBudget
}

func (it *zipIter) next() (member, error) {
	for {
		if it.i >= len(it.files) {
			return member{}, io.EOF
		}
		e := it.files[it.i]
		it.i++
		if strings.HasSuffix(e.Name, "/") {
			it.p.log.Warn("запись архива пропущена (каталог)",
				"archive", it.f.Path, "entry", e.Name)
			continue
		}
		name := path.Clean(e.Name)
		if junkMember(name) {
			it.p.log.Warn("запись архива пропущена (скрытая или мусор macOS)",
				"archive", it.f.Path, "entry", e.Name)
			continue
		}
		if e.UncompressedSize64 > uint64(it.budget.left) {
			return member{}, fmt.Errorf(
				"%w: архив %s: несжатый объём члена %s (%d байт) не укладывается в остаток ingest.archive_max_uncompressed (%d байт)",
				errArchiveRejected, it.f.Path, name, e.UncompressedSize64, it.budget.left)
		}
		rc, err := e.Open()
		if err != nil {
			// Открытие записи не трогает данные: ошибки — класса формата
			// (метод сжатия и пр.); сбой чтения диска маловероятен, но
			// классифицируем его как инфраструктуру (ТЗ 3.4).
			if isCorruptRead(err) {
				return member{}, fmt.Errorf("%w: архив %s: член %s: %w",
					errArchiveRejected, it.f.Path, name, err)
			}
			return member{}, fmt.Errorf("ingest: архив %s: член %s: %w", it.f.Path, name, err)
		}
		return member{name: name, r: it.budget.reader(rc), closer: rc}, nil
	}
}

// tarIter — члены tar (поверх голого файла или gzip). Несжатый объём
// считает budgetReader на уровне всего распакованного потока (заголовки
// tar плюс содержимое членов — «счётчиком по потоку», T-133), число
// записей — каждый заголовок, включая каталоги и пропускаемый мусор
// (как у zip по центральному каталогу). Читатель члена — сам tar.Reader:
// строки текущей записи до её конца.
type tarIter struct {
	p       *Pipeline
	f       File
	tr      *tar.Reader
	entries int
}

func (it *tarIter) next() (member, error) {
	for {
		hdr, err := it.tr.Next()
		if err == io.EOF {
			return member{}, io.EOF
		}
		if err != nil {
			switch {
			case errors.Is(err, errArchiveRejected): // бюджет потока
				return member{}, err
			case isCorruptRead(err):
				return member{}, fmt.Errorf("%w: архив %s повреждён: %w", errArchiveRejected, it.f.Path, err)
			default:
				return member{}, fmt.Errorf("ingest: чтение архива %s: %w", it.f.Path, err)
			}
		}
		// Лимит записей — до фильтров: каталоги и мусор тоже записи
		// (бомба метаданными), Warn-поток ограничен тем же числом.
		it.entries++
		if it.entries > it.p.opts.ArchiveMaxFiles {
			return member{}, fmt.Errorf("%w: архив %s: записей %d больше ingest.archive_max_files (%d)",
				errArchiveRejected, it.f.Path, it.entries, it.p.opts.ArchiveMaxFiles)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			it.p.log.Warn("запись архива пропущена (каталог)",
				"archive", it.f.Path, "entry", hdr.Name)
			continue
		case tar.TypeReg: // регулярные файлы (TypeRegA приводится к TypeReg при чтении)
		default:
			it.p.log.Warn("запись архива пропущена (не файл)",
				"archive", it.f.Path, "entry", hdr.Name)
			continue
		}
		name := path.Clean(hdr.Name)
		if junkMember(name) {
			it.p.log.Warn("запись архива пропущена (скрытая или мусор macOS)",
				"archive", it.f.Path, "entry", hdr.Name)
			continue
		}
		return member{name: name, r: it.tr}, nil
	}
}

// gzIter — единственный член .gz-архива: всё содержимое потока; имя —
// имя архива без расширения (T-133). Многосегментный gzip (склейка
// потоков) читается одним членом — multistream-режим gzip.Reader.
type gzIter struct {
	m    member
	done bool
}

func (it *gzIter) next() (member, error) {
	if it.done {
		return member{}, io.EOF
	}
	it.done = true
	return it.m, nil
}

// --- бюджет несжатого объёма -----------------------------------------------

// archiveBudget — остаток суммарного несжатого объёма членов архива
// (ingest.archive_max_uncompressed, T-133): для tar/gz — единственная
// проверка (счётчиком по потоку), для zip — страховка к проверке
// заголовков (заголовок может лгать).
type archiveBudget struct {
	left  int64
	limit int64
	arch  string
}

// reader оборачивает поток члена счётчиком остатка.
func (b *archiveBudget) reader(r io.Reader) io.Reader {
	return &budgetReader{r: r, b: b}
}

// budgetReader считает прочитанные байты против общего остатка архива;
// превышение — ошибка природы архива (промаркирована errArchiveRejected),
// прерывающая чтение. Ошибка отдаётся вызовом, СЛЕДУЮЩИМ за исчерпанием:
// io.ReadAtLeast (tar-заголовки) отбрасывает ошибку полностью
// заполненного буфера, и ошибка, отданная вместе с последними байтами,
// была бы потеряна. Ошибка носителя (диск), совпавшая с пересечением
// лимита, сохраняется и отдаётся вместо бюджетной — сбой инфраструктуры
// не маскируется «битым архивом».
type budgetReader struct {
	r   io.Reader
	b   *archiveBudget
	err error // ошибка носителя на пересёкшем лимит чтении
}

func (l *budgetReader) Read(p []byte) (int, error) {
	if l.b.left < 0 {
		if l.err != nil {
			return 0, l.err
		}
		return 0, fmt.Errorf(
			"%w: архив %s: несжатый объём потока превысил ingest.archive_max_uncompressed (%d байт)",
			errArchiveRejected, l.b.arch, l.b.limit)
	}
	n, err := l.r.Read(p)
	if n > 0 {
		l.b.left -= int64(n)
	}
	if l.b.left < 0 {
		if err != nil && !errors.Is(err, io.EOF) {
			l.err = err
		}
		return n, nil // ошибка — следующим вызовом (см. комментарий типа)
	}
	return n, err
}

// --- суммы по членам и потоковая обработка ----------------------------------

// archiveResult — итог потоковой обработки архива (T-133): sha256 байтов
// архива (файла целиком — сжатая форма), суммы счётчиков по членам и
// число обработанных членов.
type archiveResult struct {
	SHA256  []byte
	Stats   Stats
	Result  FileResult
	Members int
}

// archiveTotals — суммы счётчиков по обработанным членам архива (T-133):
// отчёт ingest_files один на архив, значения — суммы по членам.
type archiveTotals struct {
	stats    Stats
	content  int // содержательные строки (итоги валидаторов членов)
	accepted int
	rejected int // отклонения валидаторов
	reasons  map[Reason]int
	members  int
}

func (a *archiveTotals) add(st Stats, res FileResult, prejects map[Reason]int) {
	a.stats.Total += st.Total
	a.stats.Skipped += st.Skipped
	a.stats.Rejected += st.Rejected
	a.stats.Data += st.Data
	a.content += res.Total
	a.accepted += res.Accepted
	a.rejected += res.Rejected
	a.reasons = mergeReasons(a.reasons, mergeReasons(prejects, res.Reasons))
	a.members++
}

// streamArchive — потоковая обработка архива (T-133): члены читаются по
// очереди как io.Reader и проходят существующие Parse/Validator/Stage;
// зона и действие — из каталога архива (f), внутренние пути членов на
// зону не влияют. sha256 — хэш байтов архива: tar/gz — тот же
// единственный проход (TeeReader под распаковщиком, ТЗ 3.5) с дочиткой
// остатка потока по завершении (tar.Reader останавливается на
// EOF-блоках, не доходя до паддинга и трейлера gzip — без дочитки хэш
// был бы префиксом, а CRC gzip не проверился бы); zip — отдельный
// последовательный проход до чтения членов (формат с произвольным
// доступом — ReaderAt, последовательный поток поверх невозможен).
// Порог ошибок действует на член (любой член выше
// ingest.max_error_ratio — архив ошибочен), счётчики суммируются по
// обработанным членам; строки всех членов идут в одну партию со
// сквозной нумерацией (уникальность line_no, T-017).
func (p *Pipeline) streamArchive(ctx context.Context, fh *os.File, f File, kind archiveKind, batch Batch, zoneIDs map[string]int16) (archiveResult, error) {
	h := sha256.New()
	budget := &archiveBudget{left: p.opts.ArchiveMaxUncompressed, limit: p.opts.ArchiveMaxUncompressed, arch: f.Path}

	var (
		it    memberIter
		drain io.Reader // остаток потока: завершает sha256 и проверяет трейлер gzip
	)
	switch kind {
	case archiveZip:
		if _, err := io.Copy(h, fh); err != nil {
			return archiveResult{}, fmt.Errorf("ingest: чтение архива %s: %w", f.Path, err)
		}
		fi, err := fh.Stat()
		if err != nil {
			return archiveResult{}, fmt.Errorf("ingest: архив %s: %w", f.Path, err)
		}
		zr, err := zip.NewReader(fh, fi.Size())
		if err != nil {
			return archiveResult{}, archiveOpenErr(f, err)
		}
		// Число записей — до чтения членов: центральный каталог уже
		// разобран открытием (stdlib), счёт ограничивает последующую
		// обработку; сам каталог ограничен размером входного файла
		// (incoming — зона ответственности оператора, ТЗ 5.1).
		if len(zr.File) > p.opts.ArchiveMaxFiles {
			return archiveResult{SHA256: h.Sum(nil)}, fmt.Errorf(
				"%w: архив %s: записей %d больше ingest.archive_max_files (%d)",
				errArchiveRejected, f.Path, len(zr.File), p.opts.ArchiveMaxFiles)
		}
		it = &zipIter{p: p, f: f, files: zr.File, budget: budget}
	case archiveTar:
		counted := budget.reader(io.TeeReader(fh, h))
		it = &tarIter{p: p, f: f, tr: tar.NewReader(counted)}
		drain = counted
	case archiveTarGz:
		gz, err := gzip.NewReader(io.TeeReader(fh, h))
		if err != nil {
			return archiveResult{}, archiveOpenErr(f, err)
		}
		counted := budget.reader(gz)
		it = &tarIter{p: p, f: f, tr: tar.NewReader(counted)}
		drain = counted
	case archiveGz:
		gz, err := gzip.NewReader(io.TeeReader(fh, h))
		if err != nil {
			return archiveResult{}, archiveOpenErr(f, err)
		}
		counted := budget.reader(gz)
		it = &gzIter{m: member{name: gzMemberName(f.Path), r: counted}}
		drain = counted
	default:
		return archiveResult{}, fmt.Errorf("ingest: архив %s: неподдерживаемый формат (%d)", f.Path, kind)
	}

	var (
		agg     archiveTotals
		lineOff int64 // сквозной номер строки архива: line_no партии растёт
	)
	// Построчные отказы парсера пишутся тем же rejected-логом, что и
	// отказы валидатора (стиль streamStage); nil-логгер — записи подавлены.
	rejectLog := p.opts.RejectLog
	if rejectLog == nil {
		rejectLog = slog.New(slog.DiscardHandler)
	}

	produce := func(emit func(Row) error) error {
		for {
			m, err := it.next()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err // классифицирован источником членов
			}
			// Валидатор члена: адресация отклонений «архив!член» (T-133),
			// зона — каталога архива (внутренний путь на зону не влияет).
			v, verr := NewValidator(ValidatorOptions{
				File:             f.Path + "!" + m.name,
				Zone:             f.Zone,
				AllowedZonePaths: p.opts.AllowedZonePaths,
				MaxErrorRatio:    p.opts.MaxErrorRatio,
				RejectLog:        p.opts.RejectLog,
			})
			if verr != nil {
				if m.closer != nil {
					_ = m.closer.Close()
				}
				return fmt.Errorf("ingest: архив %s: %w", f.Path, verr)
			}
			prejects := make(map[Reason]int)
			mst, perr := Parse(m.r, Options{
				OnLine: func(l Line) error {
					e, _, ok := v.Line(l.Num, l.Text)
					if !ok {
						return nil // отклонение учтено валидатором (rejected-лог)
					}
					return p.emitRow(ctx, zoneIDs, emit, e, lineOff+int64(l.Num))
				},
				OnReject: func(rj Reject) error {
					prejects[rj.Reason]++
					rejectLog.Warn("строка отклонена",
						"file", f.Path+"!"+m.name,
						"line", rj.Num,
						"text", rj.Text,
						"reason", string(rj.Reason),
					)
					return nil
				},
			})
			if m.closer != nil {
				// CRC zip-члена проверяется чтением до конца; Close —
				// освобождение ресурса, ошибка уже всплыла Read-ом.
				_ = m.closer.Close()
			}
			lineOff += int64(mst.Total)
			if perr != nil {
				if errors.Is(perr, errArchiveRejected) {
					return perr // лимит объёма по потоку (budgetReader)
				}
				if isCorruptRead(perr) {
					return fmt.Errorf("%w: архив %s повреждён при чтении члена %s: %w",
						errArchiveRejected, f.Path, m.name, perr)
				}
				return fmt.Errorf("член %s: %w", m.name, perr) // инфраструктура
			}
			res := v.Finish()
			agg.add(mst, res, prejects)
			if overErrorRatio(p.opts.MaxErrorRatio, res.Total, res.Rejected, mst.Rejected) {
				return fmt.Errorf("%w: %s!%s: доля отклонённых строк %d из %d выше ingest.max_error_ratio (%g)",
					errArchiveRejected, f.Path, m.name, res.Rejected+mst.Rejected, res.Total+mst.Rejected, p.opts.MaxErrorRatio)
			}
		}
	}

	_, serr, perr := p.stageStream(ctx, batch, produce)
	// Дочитать остаток потока — только на чистом исходе: tar.Reader
	// останавливается на EOF-блоках, не потребив паддинг, а gzip
	// проверяет CRC только по трейлеру — без дочитки sha256 был бы
	// хэшем префикса, а битый трейлер .tar.gz прошёл бы незамеченным.
	// Ошибочная/оборванная обработка не дочитывается: хэш префикса
	// ненадёжен (ниже обнуляется), а чтение могло бы тратить бюджет
	// лимита на уже отвергнутый архив.
	if drain != nil && serr == nil && perr == nil {
		if _, derr := io.Copy(io.Discard, drain); derr != nil {
			switch {
			case errors.Is(derr, errArchiveRejected): // бюджет потока
				perr = derr
			case isCorruptRead(derr):
				perr = fmt.Errorf("%w: архив %s повреждён: %w", errArchiveRejected, f.Path, derr)
			default:
				perr = fmt.Errorf("ingest: чтение архива %s: %w", f.Path, derr)
			}
		}
	}
	out := archiveResult{
		Stats: agg.stats,
		Result: FileResult{ValidatorStats: ValidatorStats{
			Total:    agg.content,
			Accepted: agg.accepted,
			Rejected: agg.rejected,
			Reasons:  agg.reasons,
		}},
		Members: agg.members,
	}
	if serr == nil && perr == nil {
		out.SHA256 = h.Sum(nil) // файл прочитан целиком (с дочиткой)
	}
	if serr != nil {
		return out, serr
	}
	if perr != nil {
		if errors.Is(perr, errArchiveRejected) {
			return out, perr
		}
		return out, fmt.Errorf("ingest: разбор архива %s: %w", f.Path, perr)
	}
	return out, nil
}

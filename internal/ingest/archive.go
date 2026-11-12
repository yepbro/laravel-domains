// Streaming unpacking of input archives (T-133; an extension of spec
// 5.1, streaming — spec 3.5). The file pipeline (pipeline.go) branches
// to unpacking by the file name extension: each member is read as an
// io.Reader and goes through the existing Parse/Validator/Stage — no
// materialization of members on disk, memory like the txt pipeline (the
// parser window + a COPY chunk).
//
// The formats are stdlib: archive/zip (the file is opened as ReaderAt,
// the size — by the fact of opening), archive/tar, compress/gzip;
// .tar.gz/.tgz — gzip over tar (a double opening); .gz — one member
// named as the archive without the extension. rar/7z are not supported
// (not stdlib) — a rejected option.
//
// The semantics (T-133): the zone and the action come only from the
// archive directory; the members' internal paths do not affect the zone
// (the members are the domain lists of the directory's zone; choosing
// the zone from a member path on its own is unsafe) — a member path is
// used only to address the rejected log entries "archive!member:line";
// directories, hidden files and macOS junk (__MACOSX/, ._*, .DS_Store)
// are skipped with a Warn; nested archives are not unpacked — one level:
// an archive member yields binary junk → an error share → failed.
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

// DefaultArchiveMaxUncompressed — the default cap on the total
// uncompressed size of the archive members
// (ingest.archive_max_uncompressed). Duplicates config.Default so the
// pipeline also works without configuration (the DefaultBatchSize
// style). 8 GiB: an uncompressed .com zone ≈ 1.5 GB — with a margin for
// zone growth and concatenations, without an excessive window for zip
// bombs.
const DefaultArchiveMaxUncompressed = int64(8 << 30)

// DefaultArchiveMaxFiles — the default cap on the number of archive
// entries (ingest.archive_max_files): real zone dumps are a few files.
const DefaultArchiveMaxFiles = 1000

// errArchiveRejected — the archive is bad by its nature (corrupt, the
// limits exceeded, a member over the error threshold): the file is a
// failed/ candidate, like a txt file with a rejection share over
// ingest.max_error_ratio (spec 5.1); the wrapped reasons are available
// via the chain (errors.Is/As). The text — without repeating "bad":
// failFile appends its own verdict.
var errArchiveRejected = errors.New("архив не принят")

// archiveKind — the format of the input archive by the name extension
// (T-133).
type archiveKind uint8

const (
	archiveNone  archiveKind = iota // *.txt — not an archive
	archiveZip                      // .zip
	archiveTar                      // .tar
	archiveTarGz                    // .tar.gz, .tgz
	archiveGz                       // .gz without a tar layer
)

// archiveKindOf — the format by the name suffix, case-insensitive;
// .tar.gz is checked before .gz, .tgz is a suffix of its own.
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

// isArchiveName — whether the input file name is a supported archive
// (T-133).
func isArchiveName(name string) bool { return archiveKindOf(name) != archiveNone }

// gzMemberName — the name of the single member of a .gz archive: the
// file name without the extension (com/add/zone.gz → zone; T-133).
func gzMemberName(p string) string {
	base := path.Base(p)
	return strings.TrimSuffix(base, path.Ext(base))
}

// junkMember — archive entries without domain lists (T-133): hidden
// files and directories (a path component starting with "." — including
// macOS junk ._*, .DS_Store and hidden trees in full) and the resource
// directory __MACOSX (any component). Internal paths do not affect the
// zone and are not applied to the file system — the name only addresses
// the rows.
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

// overErrorRatio — the combined error-share threshold (parser plus
// validator rejections, like a txt file in streamStage): a file without
// meaningful rows does not count as bad, a share equal to the threshold
// does not sink it (spec 5.1).
func overErrorRatio(ratio float64, validatorTotal, validatorRejected, parserRejected int) bool {
	if total := validatorTotal + parserRejected; total > 0 {
		return float64(validatorRejected+parserRejected)/float64(total) > ratio
	}
	return false
}

// isCorruptRead — a read error meaning corrupted archive data (a corrupt
// one — failed/, T-133), not an infrastructure failure: the known
// stdlib format errors and a truncated stream. Anything else (disk
// errors) — infrastructure: the file stays in incoming (spec 3.4).
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

// archiveOpenErr — an archive opening error
// (zip.NewReader/gzip.NewReader): an empty or junk file — a corrupt
// archive; a disk read failure — not (the classification is as for
// members).
func archiveOpenErr(f File, err error) error {
	if isCorruptRead(err) || errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: архив %s не читается: %w", errArchiveRejected, f.Path, err)
	}
	return fmt.Errorf("ingest: чтение архива %s: %w", f.Path, err)
}

// --- member sources ---------------------------------------------------------

// member — an archive member being processed: the name (the internal
// path, cleaned by path.Clean) for addressing the rejected log and the
// content stream. closer — a zip entry resource (tar/gz members are
// decompressor streams, nothing to close).
type member struct {
	name   string
	r      io.Reader
	closer io.Closer
}

// memberIter — a sequential source of archive members: entries without
// domain lists (directories, hidden, macOS junk) are skipped inside
// with a Warn; io.EOF — the archive is exhausted. Errors of the
// archive's nature are marked with errArchiveRejected, the rest are
// infrastructure.
type memberIter interface {
	next() (member, error)
}

// zipIter — the zip members: the central directory headers, the data —
// by offsets via ReaderAt. A member's uncompressed size is known before
// reading (UncompressedSize64) and is checked against the remaining
// budget in advance; the actual reading is counted too — the header may
// lie.
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
			// Opening an entry does not touch the data: the errors are of
			// the format class (the compression method etc.); a disk read
			// failure is unlikely, but we classify it as infrastructure
			// (spec 3.4).
			if isCorruptRead(err) {
				return member{}, fmt.Errorf("%w: архив %s: член %s: %w",
					errArchiveRejected, it.f.Path, name, err)
			}
			return member{}, fmt.Errorf("ingest: архив %s: член %s: %w", it.f.Path, name, err)
		}
		return member{name: name, r: it.budget.reader(rc), closer: rc}, nil
	}
}

// tarIter — the tar members (over a bare file or gzip). The uncompressed
// size is counted by budgetReader over the whole decompressed stream
// (the tar headers plus the members' content — "a counter over the
// stream", T-133); the number of entries — each header, including
// directories and skippable junk (like zip over the central directory).
// A member's reader is tar.Reader itself: the rows of the current entry
// until its end.
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
			case errors.Is(err, errArchiveRejected): // the stream budget
				return member{}, err
			case isCorruptRead(err):
				return member{}, fmt.Errorf("%w: архив %s повреждён: %w", errArchiveRejected, it.f.Path, err)
			default:
				return member{}, fmt.Errorf("ingest: чтение архива %s: %w", it.f.Path, err)
			}
		}
		// The entry limit — before the filters: directories and junk are
		// entries too (a metadata bomb), and the Warn stream is bounded
		// by the same number.
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
		case tar.TypeReg: // regular files (TypeRegA is normalized to TypeReg when reading)
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

// gzIter — the single member of a .gz archive: the entire stream
// content; the name — the archive name without the extension (T-133).
// Multi-segment gzip (concatenated streams) is read as one member — the
// multistream mode of gzip.Reader.
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

// --- uncompressed size budget -----------------------------------------------

// archiveBudget — the remaining total uncompressed size of the archive
// members (ingest.archive_max_uncompressed, T-133): for tar/gz — the
// only check (a counter over the stream), for zip — a backstop to the
// header check (the header may lie).
type archiveBudget struct {
	left  int64
	limit int64
	arch  string
}

// reader wraps the member stream with a remaining-budget counter.
func (b *archiveBudget) reader(r io.Reader) io.Reader {
	return &budgetReader{r: r, b: b}
}

// budgetReader counts the bytes read against the archive's total
// remainder; exceeding it — an error of the archive's nature (marked
// with errArchiveRejected) interrupting the read. The error is given to
// the call FOLLOWING the exhaustion: io.ReadAtLeast (the tar headers)
// discards the error of a fully filled buffer, and an error given along
// with the last bytes would be lost. A carrier error (the disk) that
// coincides with crossing the limit is saved and returned instead of the
// budget one — an infrastructure failure is not masked as a "corrupt
// archive".
type budgetReader struct {
	r   io.Reader
	b   *archiveBudget
	err error // the carrier error on the read that crossed the limit
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
		return n, nil // the error — on the next call (see the type comment)
	}
	return n, err
}

// --- per-member totals and streaming ----------------------------------------

// archiveResult — the result of the streaming archive processing
// (T-133): the sha256 of the archive bytes (the whole file — the
// compressed form), the sums of the counters over the members and the
// number of processed members.
type archiveResult struct {
	SHA256  []byte
	Stats   Stats
	Result  FileResult
	Members int
}

// archiveTotals — the sums of the counters over the processed archive
// members (T-133): the ingest_files report is one per archive, the
// values are the sums over the members.
type archiveTotals struct {
	stats    Stats
	content  int // meaningful rows (the members' validator totals)
	accepted int
	rejected int // validator rejections
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

// streamArchive — the streaming processing of an archive (T-133): the
// members are read one by one as io.Reader and go through the existing
// Parse/Validator/Stage; the zone and the action come from the archive
// directory (f), the members' internal paths do not affect the zone.
// sha256 — the hash of the archive bytes: tar/gz — the same single pass
// (a TeeReader under the decompressor, spec 3.5) with a drain of the
// stream remainder at the end (tar.Reader stops at the EOF blocks
// without reaching the padding and the gzip trailer — without the drain
// the hash would be a prefix and the gzip CRC would not be verified);
// zip — a separate sequential pass before reading the members (a random
// access format — ReaderAt, a sequential stream over it is impossible).
// The error threshold applies to a member (any member over
// ingest.max_error_ratio makes the archive bad), the counters are summed
// over the processed members; the rows of all members go into one batch
// with continuous numbering (line_no uniqueness, T-017).
func (p *Pipeline) streamArchive(ctx context.Context, fh *os.File, f File, kind archiveKind, batch Batch, zoneIDs map[string]int16) (archiveResult, error) {
	h := sha256.New()
	budget := &archiveBudget{left: p.opts.ArchiveMaxUncompressed, limit: p.opts.ArchiveMaxUncompressed, arch: f.Path}

	var (
		it    memberIter
		drain io.Reader // the stream remainder: finishes the sha256 and verifies the gzip trailer
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
		// The number of entries — before reading the members: the central
		// directory is already parsed by the opening (stdlib), the count
		// bounds the subsequent processing; the directory itself is
		// bounded by the input file size (incoming — the operator's
		// responsibility, spec 5.1).
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
		lineOff int64 // the archive's continuous row number: the batch's line_no grows
	)
	// The parser's per-row rejections are written to the same rejected
	// log as the validator's (the streamStage style); a nil logger —
	// records suppressed.
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
				return err // classified by the member source
			}
			// The member's validator: addressing the rejections
			// "archive!member" (T-133); the zone — the archive directory's
			// (an internal path does not affect the zone).
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
						return nil // the rejection is accounted for by the validator (the rejected log)
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
				// A zip member's CRC is verified by reading to the end;
				// Close is resource release, the error has already
				// surfaced via Read.
				_ = m.closer.Close()
			}
			lineOff += int64(mst.Total)
			if perr != nil {
				if errors.Is(perr, errArchiveRejected) {
					return perr // the stream size limit (budgetReader)
				}
				if isCorruptRead(perr) {
					return fmt.Errorf("%w: архив %s повреждён при чтении члена %s: %w",
						errArchiveRejected, f.Path, m.name, perr)
				}
				return fmt.Errorf("член %s: %w", m.name, perr) // infrastructure
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
	// Drain the stream remainder — only on a clean outcome: tar.Reader
	// stops at the EOF blocks without consuming the padding, and gzip
	// verifies the CRC only by the trailer — without the drain the sha256
	// would be a prefix hash, and a corrupt .tar.gz trailer would pass
	// unnoticed. An errored/interrupted processing is not drained: the
	// prefix hash is unreliable (zeroed below), and the reading could
	// spend the limit's budget on an already rejected archive.
	if drain != nil && serr == nil && perr == nil {
		if _, derr := io.Copy(io.Discard, drain); derr != nil {
			switch {
			case errors.Is(derr, errArchiveRejected): // the stream budget
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
		out.SHA256 = h.Sum(nil) // the whole file has been read (with the drain)
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

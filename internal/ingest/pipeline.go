// Сквозной конвейер обработки одного входного файла (T-021; ТЗ 5.1):
// событие internal/watch → ParsePath (T-014) → EnsureZone (T-014) →
// один проход по файлу: потоковый разбор (T-015) и sha256 содержимого
// через io.TeeReader — чтение ровно одно, второго прохода по файлу нет
// (ТЗ 3.5) → валидация строк с rejected-логом (T-016) → загрузка в
// staging (T-017) → применение add/delete (T-018/T-019) → отчёт в
// ingest_files (report.go) → перемещение файла в processed/ или failed/.
// Архивы (.zip/.tar/.tar.gz/.tgz/.gz) ветвятся по расширению на
// потоковую распаковку — archive.go (T-133): члены проходят те же
// Parse/Validator/Stage, единица учёта — архив.
//
// Идемпотентность (ТЗ 3.4, 5.1; T-113): защита от повторной обработки —
// точная повторная доставка, совпадение (зона, действие, sha256) в
// ingest_files; конвейер проверяет хэш после загрузки staging (хэш
// известен только к концу чтения) и до применения.
// Классы исходов и решение «файл остаётся или уходит в архив» — Outcome.
package ingest

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"crawler/internal/model"
)

// DefaultBatchSize — размер пачки COPY загрузки staging по умолчанию
// (ingest.batch_size, ТЗ 5.10). Дублирует config.Default, чтобы конвейер
// работал и без конфигурации (стиль DefaultMaxErrorRatio, T-016).
const DefaultBatchSize = 50000

// Каталоги архива приёма — верхний уровень incoming (ТЗ 5.1):
// processed/ — успешно обработанные файлы, failed/ — файлы с ошибкой и
// лог-отчётом рядом. internal/watch их не наблюдает (T-013).
const (
	DirProcessed = "processed"
	DirFailed    = "failed"
)

// stageChanSize — буфер канала строк между разбором файла и загрузкой
// staging: парсер не ждёт БД на каждой строке, а память ограничена
// (ТЗ 3.5).
const stageChanSize = 1024

// errStopped — производитель строк остановлен потребителем (загрузка
// прервана ошибкой БД): парсер завершается досрочно.
var errStopped = errors.New("ingest: загрузка партии остановлена")

// PipelineOptions — параметры конвейера; нулевые значения заменяются
// дефолтами (стиль watch.Options, T-013).
type PipelineOptions struct {
	// Dir — наблюдаемый каталог incoming (ingest.dir): файлы приходят
	// путями внутри него, там же размещается архив processed/failed
	// (ТЗ 5.1).
	Dir string
	// BatchSize — строк на пачку COPY в staging (ingest.batch_size).
	BatchSize int
	// MaxErrorRatio — порог доли отклонённых строк файла
	// (ingest.max_error_ratio); 0 — DefaultMaxErrorRatio. Для архивов
	// действует на член (T-133): член выше порога роняет архив целиком.
	MaxErrorRatio float64
	// ArchiveMaxUncompressed — потолок суммарного несжатого объёма членов
	// архива (ingest.archive_max_uncompressed, T-133); 0 — дефолт.
	ArchiveMaxUncompressed int64
	// ArchiveMaxFiles — потолок числа записей архива
	// (ingest.archive_max_files, T-133); 0 — дефолт.
	ArchiveMaxFiles int
	// AllowedZonePaths — многоуровневые зоны, которым разрешено быть
	// каталогами incoming (ingest.allowed_zone_paths, ТЗТ §5.1):
	// тестовые зоны testbed (crawl.test); nil/пусто — рабочие каталоги
	// только TLD. Файл запрещённого многоуровневого каталога — ошибка
	// уровня файла (failed/, T-021).
	AllowedZonePaths []string
	// RejectLog — логгер rejected (NewRejectedLogger, T-016); nil —
	// записи подавлены (счётчики ведутся всё равно).
	RejectLog *slog.Logger
	// Log — логгер сервиса для итогов обработки; nil — подавлен.
	Log *slog.Logger
	// Now — источник времени для processed_at (заглушка в тестах);
	// nil — time.Now.
	Now func() time.Time
}

// Pipeline — сквозная обработка готового входного файла (ТЗ 5.1) из
// готовых ступеней T-014…T-020. Самостоятельного состояния между файлами
// не держит: идемпотентность — по тройке (зона, действие, sha256) в
// ingest_files, а не в памяти.
type Pipeline struct {
	zones ZoneStore
	files FileStore
	pool  ApplyDB
	opts  PipelineOptions
	log   *slog.Logger
	now   func() time.Time
}

// NewPipeline — конвейер над хранилищами: ZoneStore (T-014), FileStore
// (журнал ingest_files, report.go) и пул ApplyDB для staging и применения
// (применение add — в одной транзакции, T-205).
func NewPipeline(zones ZoneStore, files FileStore, pool ApplyDB, opts PipelineOptions) (*Pipeline, error) {
	if zones == nil || files == nil || pool == nil {
		return nil, errors.New("ingest: конвейер: обязательны ZoneStore, FileStore и пул БД")
	}
	if opts.Dir == "" {
		return nil, errors.New("ingest: конвейер: не задан каталог incoming (PipelineOptions.Dir)")
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = DefaultBatchSize
	}
	if opts.MaxErrorRatio == 0 {
		opts.MaxErrorRatio = DefaultMaxErrorRatio
	}
	if opts.ArchiveMaxUncompressed <= 0 {
		opts.ArchiveMaxUncompressed = DefaultArchiveMaxUncompressed
	}
	if opts.ArchiveMaxFiles <= 0 {
		opts.ArchiveMaxFiles = DefaultArchiveMaxFiles
	}
	log := opts.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Pipeline{zones: zones, files: files, pool: pool, opts: opts, log: log, now: now}, nil
}

// OutcomeKind — чем закончилась обработка файла.
type OutcomeKind string

const (
	// OutcomeApplied — файл применён к БД, отчёт записан в ingest_files,
	// файл перемещён в processed/.
	OutcomeApplied OutcomeKind = "applied"
	// OutcomeDuplicate — точная повторная доставка: та же тройка
	// (зона, действие, sha256) уже в журнале ingest_files (ТЗ 5.1,
	// T-113): загрузка отброшена, файл перемещён в processed/ без
	// нового отчёта.
	OutcomeDuplicate OutcomeKind = "duplicate"
	// OutcomeFailed — файл ошибочен по своей сути (структура пути,
	// имя зоны, доля отказов выше порога): перемещён в failed/ с
	// лог-отчётом рядом; отчёт в ingest_files не пишется — тот же хэш
	// не занимает журнал, исправленный файл можно загрузить снова.
	OutcomeFailed OutcomeKind = "failed"
	// OutcomeLeft — инфраструктурный сбой (БД, чтение файла): файл
	// остаётся в incoming и переработается после перезапуска сервиса
	// или изменения файла (доставка at-least-once, ТЗ 3.4); обработка
	// следующих файлов продолжается.
	OutcomeLeft OutcomeKind = "left"
)

// Outcome — итог обработки файла: класс, счётчики (заполнены в меру
// дошедшего) и причина (для failed/left — в лог).
type Outcome struct {
	Kind   OutcomeKind
	Report FileReport
	Err    error
}

// Process обрабатывает готовый файл (событие internal/watch, T-013):
// полный конвейер ТЗ 5.1 и перемещение файла по итогу. Возвращаемая
// ошибка не nil только для OutcomeLeft (сбой инфраструктуры — файл
// остался в incoming): это не «файловая» проблема, её видит вызывающий;
// applied/duplicate/failed — плановые итоги, ошибка возврата nil,
// причина failed — в Outcome.Err.
func (p *Pipeline) Process(ctx context.Context, abs string) (Outcome, error) {
	o := p.process(ctx, abs)
	if o.Kind == OutcomeLeft {
		return o, o.Err
	}
	return o, nil
}

// process — реализация Process без ветвления возврата.
func (p *Pipeline) process(ctx context.Context, abs string) Outcome {
	rel := filepath.ToSlash(abs)
	if r, err := filepath.Rel(p.opts.Dir, abs); err == nil {
		rel = filepath.ToSlash(r)
	}
	// Путь вне incoming/ошибки Rel уходят в ParsePath как есть — он
	// отвергает недопустимую структуру сам (T-014).

	f, err := ParsePath(rel)
	if err != nil {
		// Структура пути и имя зоны не изменятся от повтора — файл
		// ошибочен (ТЗ 5.1: failed/ с лог-отчётом).
		return p.failFile(abs, FileReport{Path: rel}, err)
	}
	rep := FileReport{Zone: f.Zone, Action: f.Action, Path: f.Path}

	// Многоуровневый каталог зоны — только из allowlist (ТЗТ §5.1):
	// проверка до EnsureZone и чтения файла — запрещённый каталог не
	// создаёт зону в справочнике и не грузит партию, файл сразу
	// ошибочен (failed/ с причиной, строки не разбираются, T-021).
	// Та же проверка стоит в NewValidator (страховка прямых вызовов
	// валидатора вне конвейера).
	if err := checkZonePath(f.Zone, p.opts.AllowedZonePaths); err != nil {
		return p.failFile(abs, rep, fmt.Errorf("ingest: файл %s: %w", f.Path, err))
	}

	// Зона зона-каталога находится или создаётся автоматически при первом
	// файле (ТЗ 5.1; EnsureZone, T-014) и сразу согревает кэш зон строк.
	// Зона-less файл (f.Zone == "") зон upfront не знает: зоны создаются
	// лениво по первым строкам (emitRow), кэш — на файл. Сбой —
	// инфраструктурный: файл остаётся (at-least-once, ТЗ 3.4).
	zoneIDs := make(map[string]int16)
	if f.Zone != "" {
		z, zerr := EnsureZone(ctx, p.zones, f.Zone)
		if zerr != nil {
			return Outcome{Kind: OutcomeLeft, Report: rep, Err: zerr}
		}
		rep.ZoneID = z.ID
		zoneIDs[f.Zone] = z.ID
	}
	zoneID := rep.ZoneID // 0 — зона-less файл (отчёт: zone_id NULL)

	id, err := NewBatchID()
	if err != nil {
		return Outcome{Kind: OutcomeLeft, Report: rep, Err: err}
	}
	batch := Batch{ID: id, ZoneID: zoneID, Action: f.Action}

	fh, err := os.Open(abs)
	if err != nil {
		return Outcome{Kind: OutcomeLeft, Report: rep, Err: fmt.Errorf("ingest: чтение файла %s: %w", abs, err)}
	}
	// Файл открыт только для чтения: ошибка закрытия безвредна.
	defer func() { _ = fh.Close() }()

	// Один проход по файлу (ТЗ 3.5): байты одновременно идут в sha256
	// (TeeReader) и в парсер; валидные строки грузятся в staging по мере
	// разбора. Хэш известен к концу чтения — проверка дубля ниже.
	// Архивы (T-133) ветвятся по расширению: tar/gz — тот же единственный
	// проход (TeeReader под распаковщиками), zip — проход хэша плюс чтение
	// членов по смещениям (ReaderAt: формат с произвольным доступом).
	h := sha256.New()
	var (
		st   Stats
		vres FileResult
		arch archiveResult
	)
	if kind := archiveKindOf(f.Path); kind == archiveNone {
		st, vres, _, err = p.streamStage(ctx, io.TeeReader(fh, h), f, batch, zoneIDs)
		rep.SHA256 = h.Sum(nil)
	} else {
		arch, err = p.streamArchive(ctx, fh, f, kind, batch, zoneIDs)
		st, vres = arch.Stats, arch.Result
		rep.SHA256 = arch.SHA256
		rep.Members = arch.Members
	}
	rep.LinesTotal = int64(st.Total)
	rep.Skipped = int64(st.Skipped)
	rep.Rejected = int64(st.Rejected + vres.Rejected)
	rep.Accepted = int64(vres.Accepted)
	rep.Reasons = vres.Reasons
	if err != nil {
		if errors.Is(err, errArchiveRejected) || errors.Is(err, errZoneNotAllowed) {
			// Ошибка по сути файла: архив ошибочен (битый, лимиты, член
			// выше порога — T-133) либо многоуровневая зона каталога вне
			// allowlist (страховочная ветка NewValidator — рабочая
			// проверка стоит в начале process; сюда попадает только при
			// рассинхроне опций, классифицируем файлом, не вечным
			// retry): как txt-файл с долей отказов выше порога — строки
			// не применяются, партия отбрасывается (ТЗ 5.1), файл целиком
			// в failed/ с лог-отчётом; частично принятые строки не
			// применялись, повторная подача идемпотентна (ON CONFLICT
			// DO NOTHING, ТЗ 5.1).
			if derr := p.clearBatch(ctx, id); derr != nil {
				p.discard(id)
				return Outcome{Kind: OutcomeLeft, Report: rep, Err: derr}
			}
			return p.failFile(abs, rep, err)
		}
		p.discard(id)
		return Outcome{Kind: OutcomeLeft, Report: rep, Err: err}
	}

	if vres.Failed {
		// Доля отказов выше порога (T-016): файл ошибочен целиком,
		// строки не применяются — партия отбрасывается (ТЗ 5.1).
		if derr := p.clearBatch(ctx, id); derr != nil {
			// Очистка не прошла (в т.ч. отмена ctx при graceful
			// shutdown): retry собственным контекстом — строки партии
			// не должны оставаться в staging (batch_id не встретится).
			p.discard(id)
			return Outcome{Kind: OutcomeLeft, Report: rep, Err: derr}
		}
		// Числитель/знаменатель — как в вердикте streamStage: отказы
		// парсера (invalid_utf8, line_len) плюс отказы валидатора.
		rejectedAll := int64(st.Rejected) + int64(vres.Rejected)
		contentAll := int64(vres.Total) + int64(st.Rejected)
		return p.failFile(abs, rep, fmt.Errorf(
			"доля отклонённых строк %d из %d выше ingest.max_error_ratio (%g)",
			rejectedAll, contentAll, p.opts.MaxErrorRatio))
	}

	// Идемпотентность (ТЗ 5.1; T-113): пропускается только точная
	// повторная доставка — совпадение (зона, действие, sha256); у
	// зона-less файла «зона» журнала — NULL (зон у файла нет).
	// Дубль с другим действием (undo тем же списком: add/, затем
	// delete/) и дубль в другой зоне обрабатываются: безвредность
	// повторного применения гарантируют операции над доменами (T-097).
	known, err := p.files.FilesBySHA256(ctx, rep.SHA256)
	if err != nil {
		p.discard(id)
		return Outcome{Kind: OutcomeLeft, Report: rep,
			Err: fmt.Errorf("ingest: проверка дубля файла %s: %w", f.Path, err)}
	}
	var dup, crossZone, crossAction *model.IngestFile
	for i := range known {
		k := &known[i]
		sameZone := (zoneID != 0 && k.ZoneID != nil && *k.ZoneID == zoneID) ||
			(zoneID == 0 && k.ZoneID == nil)
		sameAction := k.Action != nil && *k.Action == f.Action
		switch {
		case sameZone && sameAction:
			dup = k // точная повторная доставка
		case sameAction:
			if crossZone == nil {
				crossZone = k // то же действие в другой зоне
			}
		default:
			if crossAction == nil {
				crossAction = k // другое действие, зона любая
			}
		}
	}

	if dup != nil {
		if derr := p.clearBatch(ctx, id); derr != nil {
			// Дубль не применяется, но партия уже загружена: сбой
			// очистки (в т.ч. отмена ctx) ретрается discard'ом — как в
			// ветке порога ошибок выше.
			p.discard(id)
			return Outcome{Kind: OutcomeLeft, Report: rep, Err: derr}
		}
		if _, merr := p.moveFile(DirProcessed, f.Path, abs); merr != nil {
			return Outcome{Kind: OutcomeLeft, Report: rep, Err: merr}
		}
		p.log.Info("файл уже обработан (дубль — та же зона, действие и sha256) — перемещён в processed",
			"path", f.Path, "sha256", fmt.Sprintf("%x", rep.SHA256),
			"first_path", derefString(dup.Path),
			"first_processed_at", derefTime(dup.ProcessedAt))
		return Outcome{Kind: OutcomeDuplicate, Report: rep}
	}

	// Точной повторной доставки нет — файл применяется. То же
	// содержимое уже доставлялось иначе: повторная обработка в другой
	// зоне при том же действии легитимна (домены той зоны первая
	// доставка не трогала), но оператору полезно её видеть (Warn,
	// ING-P2-5); другое действие — плановый undo (Info).
	if crossZone != nil {
		p.log.Warn("дубль по sha256 в другой зоне при том же действии — обрабатывается",
			"path", f.Path, "action", f.Action.String(), "zone_id", zoneID,
			"first_path", derefString(crossZone.Path),
			"first_zone_id", derefInt16(crossZone.ZoneID),
			"first_processed_at", derefTime(crossZone.ProcessedAt))
	} else if crossAction != nil {
		p.log.Info("то же содержимое уже доставлялось другим действием — обрабатывается (undo)",
			"path", f.Path, "action", f.Action.String(), "zone_id", zoneID,
			"first_path", derefString(crossAction.Path),
			"first_action", derefAction(crossAction.Action),
			"first_processed_at", derefTime(crossAction.ProcessedAt))
	}

	// Применение по действию файла (ТЗ 5.1): add → ApplyAdd,
	// delete → ApplyDelete. Оба идемпотентны (ТЗ 3.4), сбой — файл
	// остаётся в incoming, повтор сойдётся к тому же итогу.
	switch f.Action {
	case model.ActionAdd:
		// Пачки применения — тем же ingest.batch_size, что и пачки COPY
		// в staging (T-205).
		res, aerr := ApplyAdd(ctx, p.pool, batch, p.opts.BatchSize)
		if aerr != nil {
			p.discard(id)
			return Outcome{Kind: OutcomeLeft, Report: rep, Err: aerr}
		}
		rep.Added = res.Inserted + res.Revived
		rep.Revived = res.Revived
		rep.Existed = res.Existing
	case model.ActionDelete:
		res, derr := ApplyDelete(ctx, p.pool, batch)
		if derr != nil {
			p.discard(id)
			return Outcome{Kind: OutcomeLeft, Report: rep, Err: derr}
		}
		rep.Deleted = res.Deleted
		rep.NotFound = res.NotFound
	}

	// Отчёт — после обработки файла (ТЗ 5.1: «после обработки файла
	// пишется отчёт»). Уникальность (zone_id, action, sha256) — второй
	// рубеж защиты от дублей: конкурентный отчёт отвергается схемой
	// (sqlFileRecord).
	if rerr := p.files.RecordFile(ctx, rep.record(p.now())); rerr != nil {
		return Outcome{Kind: OutcomeLeft, Report: rep,
			Err: fmt.Errorf("ingest: отчёт файла %s: %w", f.Path, rerr)}
	}

	if _, merr := p.moveFile(DirProcessed, f.Path, abs); merr != nil {
		// Отчёт уже в журнале: повторная обработка найдёт дубль и лишь
		// перенесёт файл — самовосстановление без повторного применения.
		return Outcome{Kind: OutcomeLeft, Report: rep, Err: merr}
	}

	p.log.Info("файл обработан",
		"path", f.Path, "zone", f.Zone, "zones", len(zoneIDs),
		"action", f.Action.String(),
		"lines", rep.LinesTotal, "added", rep.Added, "revived", rep.Revived,
		"existed", rep.Existed, "deleted", rep.Deleted, "not_found", rep.NotFound,
		"rejected", rep.Rejected, "sha256", fmt.Sprintf("%x", rep.SHA256))
	return Outcome{Kind: OutcomeApplied, Report: rep}
}

// emitRow — строка файла в строку staging: зона записи (Entry.Zone — зона
// пути либо, у зона-less файла, выведенная из имени) резолвится в zones.id
// через кэш файла; отсутствующая зона создаётся автоматически (EnsureZone,
// ТЗ 5.1) — по первой строке зона-less файла или первой строке новой зоны.
// Кэш держит одну размороженную EnsureZone-проверку на зону файла:
// файл зона-каталога — один entry, зона-less — по числу зон файла.
func (p *Pipeline) emitRow(ctx context.Context, cache map[string]int16, emit func(Row) error, e Entry, lineNo int64) error {
	id, ok := cache[e.Zone]
	if !ok {
		z, err := EnsureZone(ctx, p.zones, e.Zone)
		if err != nil {
			return fmt.Errorf("ingest: зона строки %s: %w", e.Zone, err)
		}
		id = z.ID
		cache[e.Zone] = id
	}
	return emit(Row{ZoneID: id, Label: e.Label, LineNo: lineNo})
}

// streamStage — один проход по файлу (повторного чтения нет, ТЗ 3.5):
// поток r идёт в парсер (T-015), значимые строки валидируются (T-016) и
// грузятся в staging (T-017) по мере разбора. Возвращает счётчики
// парсера, итог валидации (Reasons — уже объединённые с построчными
// отклонениями парсера; Failed — по доле отказов валидатора и парсера
// вместе) и число загруженных строк. Ошибка — сбой чтения файла или
// загрузки; счётчики соответствуют отданному.
//
// Парсер толкающий (OnLine), Stage — тянущий (RowIter); связка —
// stageStream (горутина-производитель и буферизованный канал).
func (p *Pipeline) streamStage(ctx context.Context, r io.Reader, f File, batch Batch, zoneIDs map[string]int16) (Stats, FileResult, int64, error) {
	v, err := NewValidator(ValidatorOptions{
		File:             f.Path,
		Zone:             f.Zone,
		AllowedZonePaths: p.opts.AllowedZonePaths,
		MaxErrorRatio:    p.opts.MaxErrorRatio,
		RejectLog:        p.opts.RejectLog,
	})
	if err != nil {
		return Stats{}, FileResult{}, 0, fmt.Errorf("ingest: файл %s: %w", f.Path, err)
	}
	// Построчные отказы парсера пишутся тем же rejected-логом, что и
	// отказы валидатора (формат — Validator.reject): Reject.Text парсер
	// уже обрезал (maxRejectText), nil-логгер — записи подавлены.
	rejectLog := p.opts.RejectLog
	if rejectLog == nil {
		rejectLog = slog.New(slog.DiscardHandler)
	}

	var (
		st       Stats
		prejects = make(map[Reason]int) // построчные отклонения парсера
	)
	staged, serr, perr := p.stageStream(ctx, batch, func(emit func(Row) error) error {
		var perr error
		st, perr = Parse(r, Options{
			OnLine: func(l Line) error {
				e, _, ok := v.Line(l.Num, l.Text)
				if !ok {
					return nil // отклонение учтено валидатором (rejected-лог)
				}
				return p.emitRow(ctx, zoneIDs, emit, e, int64(l.Num))
			},
			OnReject: func(rj Reject) error {
				prejects[rj.Reason]++
				rejectLog.Warn("строка отклонена",
					"file", f.Path,
					"line", rj.Num,
					"text", rj.Text,
					"reason", string(rj.Reason),
				)
				return nil
			},
		})
		return perr
	})

	if serr != nil {
		return st, FileResult{}, staged, serr
	}
	if perr != nil && !errors.Is(perr, errStopped) {
		return st, FileResult{}, staged, fmt.Errorf("ingest: разбор файла %s: %w", f.Path, perr)
	}
	res := v.Finish()
	res.Reasons = mergeReasons(prejects, res.Reasons)
	// Построчные отказы парсера (invalid_utf8, line_len) до v.Line не
	// доходят и потому не в счётчиках Finish — досчитываем их в порог
	// сами: доля ошибок считается по всем содержательным строкам файла
	// (битый дамп — именно не-UTF-8, docs/incoming-files.md §4), иначе
	// файл целиком из мусора (Data = 0) прошёл бы как успешный (ING-P1-2).
	res.Failed = overErrorRatio(p.opts.MaxErrorRatio, res.Total, res.Rejected, st.Rejected)
	return st, res, staged, nil
}

// stageStream — связка производителя строк и загрузки staging (ТЗ 3.5):
// produce толкает валидные строки через emit, Stage тянет их из
// буферизованного канала пачками COPY. Память ограничена stageChanSize и
// размером пачки; при ошибке загрузки потребитель закрывает stop,
// производитель завершается без утечки. Возвращает ошибки загрузки и
// производителя раздельно: остановленный потребителем производитель
// (errStopped) — не ошибка, cause — в stageErr. Счётчики производителя
// остаются его замыканиям (после возврата горутина закончена — чтение
// безопасно).
func (p *Pipeline) stageStream(ctx context.Context, batch Batch, produce func(emit func(Row) error) error) (staged int64, stageErr, produceErr error) {
	rows := make(chan Row, stageChanSize)
	stop := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		done <- produce(func(r Row) error {
			select {
			case rows <- r:
				return nil
			case <-stop:
				return errStopped
			}
		})
		close(rows)
	}()

	staged, serr := Stage(ctx, p.pool, batch, p.opts.BatchSize, func() (Row, error) {
		select {
		case row, ok := <-rows:
			if !ok {
				return Row{}, io.EOF
			}
			return row, nil
		case <-ctx.Done():
			return Row{}, ctx.Err()
		}
	})

	close(stop)
	perr := <-done

	if serr != nil {
		return staged, serr, nil
	}
	return staged, nil, perr
}

// clearBatch — очистка партии в staging; ошибка оборачивается контекстом
// (после ошибки обработки или для неприменяемой партии).
func (p *Pipeline) clearBatch(ctx context.Context, id BatchID) error {
	if _, err := ClearBatch(ctx, p.pool, id); err != nil {
		return fmt.Errorf("ingest: отбрасывание партии в %s: %w", StageTable, err)
	}
	return nil
}

// discardTimeout — запас на очистку партии после сбоя: отдельный
// контекст с таймаутом, чтобы best-effort очистка не висела вечно.
const discardTimeout = 5 * time.Second

// discard — отбросить партию после инфраструктурного сбоя (best effort):
// каждая попытка обработки берёт новый batch_id, строки неудачной партии
// мертвы — чистим, чтобы staging не рос (T-017). Вызывается и с уже
// отменённым ctx (graceful shutdown в момент загрузки): очистка обязана
// переживать отмену — иначе строки партии оставались бы в staging
// навсегда (batch_id больше не встретится) и таблица росла от рестарта
// к рестарту. Контекст — свой, с таймаутом; ошибка очистки не затеняет
// основную: логируется.
func (p *Pipeline) discard(id BatchID) {
	ctx, cancel := context.WithTimeout(context.Background(), discardTimeout)
	defer cancel()
	if err := p.clearBatch(ctx, id); err != nil {
		p.log.Warn("очистка отброшенной партии", "err", err)
	}
}

// failFile — файл ошибочен: перемещение в failed/ и лог-отчёт рядом
// (ТЗ 5.1). rel назначения — разобранный путь (зеркалит зону/операцию);
// путь из неразобранного отчёта не доверяется (Process — публичный API,
// абсолютный путь или «..» в компонентах filepath.Join вывел бы за
// пределы failed/): файл кладётся по базовому имени. Имя отчёта —
// <файл>.report.
func (p *Pipeline) failFile(abs string, rep FileReport, reason error) Outcome {
	rel := archiveRel(rep.Path, abs)
	dst, err := p.moveFile(DirFailed, rel, abs)
	if err != nil {
		return Outcome{Kind: OutcomeLeft, Report: rep, Err: err}
	}
	// Файл уже убран из incoming: сбой записи отчёта не возвращает его —
	// причина ошибки сохранена в логе сервиса.
	if werr := os.WriteFile(dst+".report", []byte(renderReport(rep, "ошибочен — "+reason.Error())), 0o644); werr != nil {
		p.log.Warn("лог-отчёт рядом с файлом не записан",
			"path", dst+".report", "err", werr)
	}
	p.log.Warn("файл ошибочен — перемещён в failed",
		"path", rep.Path, "failed", dst, "err", reason)
	return Outcome{Kind: OutcomeFailed, Report: rep, Err: reason}
}

// moveFile перемещает файл в архив processed/failed, сохраняя структуру
// зоны и операции (incoming/com/add/x.txt → processed/com/add/x.txt):
// происхождение файла видно в архиве без журнала. rel — путь назначения
// внутри каталога архива. os.Rename на Unix молча заменяет цель, поэтому
// занятое имя проверяется явно; коллизия (другой экземпляр файла с тем
// же именем) получает числовой суффикс: x.txt → x-2.txt, x-3.txt, …
func (p *Pipeline) moveFile(archive, rel, abs string) (string, error) {
	dst := filepath.Join(p.opts.Dir, archive, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return "", fmt.Errorf("ingest: каталог архива %s: %w", filepath.Dir(dst), err)
	}
	ext := filepath.Ext(dst)
	stem := strings.TrimSuffix(dst, ext)
	for i := 1; ; i++ {
		target := dst
		if i > 1 {
			target = fmt.Sprintf("%s-%d%s", stem, i, ext)
		}
		if _, err := os.Lstat(target); err == nil {
			continue // имя занято другим экземпляром
		} else if !errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("ingest: архив %s: %w", target, err)
		}
		if err := os.Rename(abs, target); err != nil {
			return "", fmt.Errorf("ingest: перемещение %s → %s: %w", abs, target, err)
		}
		return target, nil
	}
}

// archiveRel — безопасный относительный путь назначения внутри каталога
// архива. Разобранный ParsePath путь безопасен по построению (path.Clean,
// без «..» — T-014); неразобранный rep.Path не доверяется: абсолютный
// путь или «..» в компонентах после filepath.Join выходили бы за пределы
// processed/failed (ING-P3-1) — подставляется базовое имя файла.
func archiveRel(rel, abs string) string {
	if rel != "" && !path.IsAbs(rel) {
		if c := path.Clean(rel); c != "." && c != ".." && !strings.HasPrefix(c, "../") {
			return c
		}
	}
	return filepath.Base(abs)
}

// derefString — значение NULL-колонки журнала для лога (пустая строка).
func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// derefTime — значение NULL-колонки журнала для лога (нулевой момент).
func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// derefAction — значение NULL-колонки журнала для лога (пустое имя).
func derefAction(a *model.Action) string {
	if a == nil {
		return ""
	}
	return a.String()
}

// derefInt16 — значение NULL-колонки журнала для лога (ноль).
func derefInt16(v *int16) int16 {
	if v == nil {
		return 0
	}
	return *v
}

// Package watch — наблюдение за каталогом входных файлов ingest (ТЗ 5.1).
//
// Два источника событий: fsnotify (создание/запись в наблюдаемом дереве
// каталогов) и периодический полный обход как страховка — fsnotify может
// потерять события при переполнении внутреннего буфера, а обход подхватывает
// и файлы, существовавшие до старта наблюдателя. Каталог наблюдается
// рекурсивно (<зона>/add|delete/…; смысл пути разбирает уже потребитель,
// T-014).
//
// Готовность файла (ТЗ 5.1): размер и mtime стабильны StableAfter секунд
// (ingest.stable_seconds, по умолчанию 10 с). Дозаписываемые файлы не
// выдаются: каждое событие записи начинает окно стабильности заново.
// Отслеживаются только регулярные файлы *.txt и архивы поддерживаемых
// форматов (.zip, .tar, .tar.gz, .tgz, .gz — T-133) — временные (*.tmp,
// *.part), скрытые файлы и служебные каталоги processed/ и failed/
// (архив ingest) игнорируются; переименование *.tmp → готовое имя по
// ТЗ 5.1 означает готовность и приходит как событие создания нового имени.
//
// Готовые файлы отдаются в канал; повторная выдача одного файла исключена
// (учёт в памяти; постоянная идемпотентность по sha256 — ingest_files,
// T-021). Если потребитель не читает канал, наблюдатель блокируется —
// файлы не теряются (backpressure), а копящиеся события fsnotify подберёт
// очередной обход-страховка.
package watch

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Значения по умолчанию — дублируют config.Default (ingest.*), чтобы пакет
// работал и без конфигурации.
const (
	// DefaultStableAfter — окно стабильности размера/mtime
	// (ingest.stable_seconds, ТЗ 5.10).
	DefaultStableAfter = 10 * time.Second
	// DefaultScanInterval — интервал полного обхода-страховки
	// (ingest.scan_interval, расширение к ТЗ 5.10).
	DefaultScanInterval = 30 * time.Second
	// DefaultQueueSize — буфер канала готовых файлов.
	DefaultQueueSize = 128
)

// File — входной файл с завершённой записью, готовый к обработке.
type File struct {
	// Path — путь файла (Options.Dir + относительный путь).
	Path string
	// Size и ModTime — размер и mtime на момент проверки стабильности.
	Size    int64
	ModTime time.Time
}

// Options — параметры наблюдателя; нулевые значения заменяются дефолтами.
type Options struct {
	// Dir — наблюдаемый каталог (ingest.dir). Отсутствующий каталог
	// создаётся; исчезнувший на ходу восстанавливается очередным обходом.
	Dir string
	// StableAfter — сколько размер и mtime файла должны быть неизменны,
	// чтобы файл считался готовым (ingest.stable_seconds).
	StableAfter time.Duration
	// ScanInterval — интервал периодического полного обхода каталога —
	// страховка к fsnotify (ingest.scan_interval).
	ScanInterval time.Duration
	// QueueSize — буфер канала готовых файлов; при переполнении
	// наблюдатель блокируется до чтения (backpressure без потерь).
	QueueSize int
	// Log — логгер событий наблюдения; nil — не логировать.
	Log *slog.Logger
	// Now — источник времени (заглушка в тестах); nil — time.Now.
	Now func() time.Time
}

// Dir запускает наблюдение за opts.Dir и возвращает канал готовых файлов;
// канал закрывается при отмене ctx. Ошибка возвращается только на старте
// (каталог не удалось создать, fsnotify недоступен); дальнейшие проблемы —
// потерянные события, отвалившийся каталог — логируются и переживаются
// обходом-страховкой.
func Dir(ctx context.Context, opts Options) (<-chan File, error) {
	if opts.Dir == "" {
		return nil, errors.New("watch: не задан наблюдаемый каталог (Options.Dir)")
	}
	o := opts.withDefaults()

	// Каталога может не быть (том монтируется позже) — создаём и
	// наблюдаем как обычно.
	if err := os.MkdirAll(o.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("watch: каталог %s: %w", o.Dir, err)
	}
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("watch: fsnotify: %w", err)
	}

	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	now := o.Now
	if now == nil {
		now = time.Now
	}

	w := &watcher{
		opts:    o,
		log:     log,
		now:     now,
		fw:      fw,
		tracker: newTracker(o.StableAfter, now),
		watched: make(map[string]struct{}),
	}
	files := make(chan File, o.QueueSize)
	log.Info("наблюдаю каталог входных файлов",
		"dir", o.Dir,
		"stable_after", o.StableAfter.String(),
		"scan_interval", o.ScanInterval.String())

	go w.run(ctx, files)
	return files, nil
}

func (o Options) withDefaults() Options {
	if o.StableAfter <= 0 {
		o.StableAfter = DefaultStableAfter
	}
	if o.ScanInterval <= 0 {
		o.ScanInterval = DefaultScanInterval
	}
	if o.QueueSize <= 0 {
		o.QueueSize = DefaultQueueSize
	}
	return o
}

// watcher — состояние цикла наблюдения. Всё, кроме files, принадлежит
// горутине run: гонок между fsnotify и обходом нет — оба источника
// обрабатываются одним select.
type watcher struct {
	opts    Options
	log     *slog.Logger
	now     func() time.Time
	fw      *fsnotify.Watcher
	tracker *tracker
	watched map[string]struct{} // каталоги с установленным watch
}

// checkInterval — период проверки готовности кандидатов: четверть окна
// стабильности, в пределах 1 мс..250 мс.
func (w *watcher) checkInterval() time.Duration {
	d := w.opts.StableAfter / 4
	switch {
	case d < time.Millisecond:
		return time.Millisecond
	case d > 250*time.Millisecond:
		return 250 * time.Millisecond
	default:
		return d
	}
}

func (w *watcher) run(ctx context.Context, files chan<- File) {
	defer close(files)
	defer func() { _ = w.fw.Close() }()

	check := time.NewTicker(w.checkInterval())
	defer check.Stop()
	scan := time.NewTicker(w.opts.ScanInterval)
	defer scan.Stop()

	w.scan() // стартовый обход: каталоги и файлы, существовавшие до запуска

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-w.fw.Events:
			if !ok {
				return
			}
			w.handleEvent(ev)
		case err, ok := <-w.fw.Errors:
			if !ok {
				return
			}
			// Чаще всего это переполнение буфера событий: часть событий
			// потеряна, итог исправит очередной полный обход.
			w.log.Warn("fsnotify", "err", err)
		case <-check.C:
			w.emitReady(ctx, files)
		case <-scan.C:
			w.scan()
		}
	}
}

// handleEvent — событие fsnotify в наблюдаемом дереве. На Linux
// переименование в наблюдаемый каталог (x.tmp → x.txt) приходит как
// Create нового имени, уход из него — как Rename старого.
func (w *watcher) handleEvent(ev fsnotify.Event) {
	switch {
	case ev.Has(fsnotify.Create) || ev.Has(fsnotify.Write):
		fi, err := os.Lstat(ev.Name)
		if err != nil {
			w.tracker.forget(ev.Name) // исчез между событием и stat
			return
		}
		switch {
		case fi.IsDir():
			if w.skipDir(ev.Name) {
				return
			}
			// Новый каталог (зона, add/delete): наблюдаем и сразу
			// обходим — файлы могли появиться раньше watch.
			w.addWatch(ev.Name)
			w.walk(ev.Name)
		case fi.Mode().IsRegular() && !w.skipDir(filepath.Dir(ev.Name)) && w.wantFile(ev.Name):
			// Событие записи всегда начинает окно стабильности заново —
			// на ФС с грубым mtime дозапись может не менять сигнатуру.
			w.tracker.observe(ev.Name, fi.Size(), fi.ModTime(), true)
		}
	case ev.Has(fsnotify.Remove) || ev.Has(fsnotify.Rename):
		// Пути больше нет: забываем кандидата (удаление до стабильности,
		// перенос в processed/) и watch каталога (ядро снимает его само).
		w.tracker.forget(ev.Name)
		delete(w.watched, ev.Name)
	default:
		// Chmod: права доступа не меняют ни размер, ни mtime.
	}
}

// emitReady выдаёт стабильно готовые файлы. Каждый кандидат перепроверяется
// свежим stat: изменившийся или исчезнувший файл не выдаётся.
func (w *watcher) emitReady(ctx context.Context, files chan<- File) {
	for _, path := range w.tracker.paths() {
		fi, err := os.Lstat(path)
		if err != nil {
			w.tracker.forget(path) // удалён до стабильности
			continue
		}
		f, ok := w.tracker.ready(path, fi.Size(), fi.ModTime())
		if !ok {
			continue
		}
		w.log.Debug("файл стабилен", "path", f.Path, "size", f.Size)
		select {
		case files <- f: // backpressure: ждём потребителя, ничего не теряем
		case <-ctx.Done():
			return
		}
	}
}

// scan — полный обход наблюдаемого каталога (страховка к fsnotify).
func (w *watcher) scan() { w.walk(w.opts.Dir) }

// walk обходит каталог root: обновляет watch подкаталогов и состояние
// кандидатов. Скрытые каталоги и архив ingest processed/failed (ТЗ 5.1,
// верхний уровень) не обходятся.
func (w *watcher) walk(root string) {
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if path == root {
				// Наблюдаемый каталог исчез (том отвалился): вернём его
				// и watch; обход повторится по следующему тику.
				w.log.Warn("наблюдаемый каталог недоступен", "dir", root, "err", err)
				if mkErr := os.MkdirAll(root, 0o755); mkErr != nil {
					w.log.Warn("создание каталога", "dir", root, "err", mkErr)
				} else {
					w.addWatch(root)
				}
				return fs.SkipAll
			}
			w.log.Warn("обход каталога", "path", path, "err", err)
			return nil // пропустить запись, продолжить обход
		}
		if d.IsDir() {
			// Корень обхода не фильтруется: скрытое имя наблюдаемого
			// каталога (ingest.dir = /data/.incoming) — валидная
			// конфигурация, skipDir на корне молча отключал наблюдение
			// целиком (ING-P3-4). Подкаталоги фильтруются как раньше:
			// walk вызывается и для нового подкаталога по событию —
			// внутрь processed/failed и скрытых заходить не нужно.
			if path != root && w.skipDir(path) {
				return fs.SkipDir
			}
			w.addWatch(path)
			return nil
		}
		if d.Type().IsRegular() && !w.skipDir(filepath.Dir(path)) && w.wantFile(path) {
			if info, err := d.Info(); err == nil {
				w.tracker.observe(path, info.Size(), info.ModTime(), false)
			}
		}
		return nil
	})
	if err != nil {
		w.log.Warn("обход каталога", "dir", root, "err", err)
	}
}

// skipDir — каталоги без входных файлов: processed/ и failed/ верхнего
// уровня (архив ingest, ТЗ 5.1) и скрытые каталоги.
func (w *watcher) skipDir(path string) bool {
	base := filepath.Base(path)
	if strings.HasPrefix(base, ".") {
		return true
	}
	rel, err := filepath.Rel(w.opts.Dir, path)
	if err != nil {
		return false
	}
	top, _, _ := strings.Cut(rel, string(filepath.Separator))
	return top == "processed" || top == "failed"
}

// wantExts — отслеживаемые суффиксы имён (нижний регистр): *.txt (ТЗ 5.1)
// и архивы поддерживаемых форматов (T-133); .tar.gz покрывается суффиксом
// .gz, .tgz — отдельный суффикс. Список зеркалит приём: ParsePath
// (internal/ingest) разбирает всё выданное наблюдателем.
var wantExts = []string{".txt", ".zip", ".tar", ".tgz", ".gz"}

// wantFile — файлы, готовность которых отслеживается: не скрытые *.txt
// и архивы поддерживаемых форматов (.zip, .tar, .tar.gz, .tgz, .gz —
// T-133). Временные (*.tmp, *.part) и прочие расширения не
// отслеживаются: по ТЗ 5.1 готовность — появление/переименование в
// отслеживаемое имя.
func (w *watcher) wantFile(path string) bool {
	base := filepath.Base(path)
	if strings.HasPrefix(base, ".") {
		return false
	}
	lower := strings.ToLower(base)
	for _, ext := range wantExts {
		if strings.HasSuffix(lower, ext) {
			return true
		}
	}
	return false
}

// addWatch устанавливает watch каталога (идемпотентно); неудача не фатальна
// — каталог появится в обходе, и watch будет повторен.
func (w *watcher) addWatch(dir string) {
	if _, ok := w.watched[dir]; ok {
		return
	}
	if err := w.fw.Add(dir); err != nil {
		w.log.Warn("fsnotify watch", "dir", dir, "err", err)
		return
	}
	w.watched[dir] = struct{}{}
}

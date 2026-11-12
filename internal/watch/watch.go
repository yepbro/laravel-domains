// Package watch — monitoring of the ingest input file directory (spec 5.1).
//
// Two event sources: fsnotify (creation/writes in the watched directory
// tree) and a periodic full pass as a safety net — fsnotify may lose events
// when its internal buffer overflows, and the pass also picks up files that
// existed before the watcher started. The directory is watched recursively
// (<zone>/add|delete/…; the meaning of the path is parsed by the consumer,
// T-014).
//
// File readiness (spec 5.1): size and mtime are stable for StableAfter
// seconds (ingest.stable_seconds, 10 s by default). Files still being
// appended to are not delivered: every write event restarts the stability
// window. Only regular *.txt files and archives of supported formats
// (.zip, .tar, .tar.gz, .tgz, .gz — T-133) are tracked — temporary files
// (*.tmp, *.part), hidden files, and the processed/ and failed/ service
// directories (the ingest archive) are ignored; a rename *.tmp → the final
// name per spec 5.1 means readiness and arrives as a create event of the
// new name.
//
// Ready files are pushed to a channel; redelivery of the same file is
// excluded (in-memory accounting; permanent idempotency by sha256 —
// ingest_files, T-021). If the consumer does not read the channel, the
// watcher blocks — files are not lost (backpressure), and the accumulating
// fsnotify events will be picked up by the next safety-net pass.
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

// Defaults duplicate config.Default (ingest.*) so that the package
// works without a configuration too.
const (
	// DefaultStableAfter is the size/mtime stability window
	// (ingest.stable_seconds, spec 5.10).
	DefaultStableAfter = 10 * time.Second
	// DefaultScanInterval is the interval of the full safety-net pass
	// (ingest.scan_interval, an extension to spec 5.10).
	DefaultScanInterval = 30 * time.Second
	// DefaultQueueSize is the buffer of the ready-files channel.
	DefaultQueueSize = 128
)

// File is an input file with a completed record, ready for processing.
type File struct {
	// Path is the file path (Options.Dir + relative path).
	Path string
	// Size and ModTime are the size and mtime at the stability check moment.
	Size    int64
	ModTime time.Time
}

// Options is the watcher parameters; zero values are replaced with defaults.
type Options struct {
	// Dir is the watched directory (ingest.dir). A missing directory
	// is created; one that disappears on the fly is restored by the next pass.
	Dir string
	// StableAfter is how long the file size and mtime must remain unchanged
	// for the file to be considered ready (ingest.stable_seconds).
	StableAfter time.Duration
	// ScanInterval is the interval of the periodic full directory pass —
	// the safety net for fsnotify (ingest.scan_interval).
	ScanInterval time.Duration
	// QueueSize is the buffer of the ready-files channel; on overflow
	// the watcher blocks until a read (backpressure without losses).
	QueueSize int
	// Log is the logger of watch events; nil means no logging.
	Log *slog.Logger
	// Now is the time source (a stub in tests); nil means time.Now.
	Now func() time.Time
}

// Dir starts watching opts.Dir and returns the channel of ready files;
// the channel is closed when ctx is cancelled. An error is returned only at
// start (the directory could not be created, fsnotify is unavailable);
// further problems — lost events, a dropped directory — are logged and
// survived via the safety-net pass.
func Dir(ctx context.Context, opts Options) (<-chan File, error) {
	if opts.Dir == "" {
		return nil, errors.New("watch: watched directory not set (Options.Dir)")
	}
	o := opts.withDefaults()

	// The directory may not exist yet (the volume is mounted later) — create
	// and watch it as usual.
	if err := os.MkdirAll(o.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("watch: directory %s: %w", o.Dir, err)
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
	log.Info("watching the incoming files directory",
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

// watcher is the state of the watch loop. Everything except files belongs
// to the run goroutine: there are no races between fsnotify and the pass —
// both sources are processed by a single select.
type watcher struct {
	opts    Options
	log     *slog.Logger
	now     func() time.Time
	fw      *fsnotify.Watcher
	tracker *tracker
	watched map[string]struct{} // directories with a watch installed
}

// checkInterval is the period of candidate readiness checks: a quarter of
// the stability window, within 1 ms..250 ms.
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

	w.scan() // initial pass: directories and files that existed before the start

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
			// Most often this is an event buffer overflow: part of the events
			// is lost; the next full pass will fix the result.
			w.log.Warn("fsnotify", "err", err)
		case <-check.C:
			w.emitReady(ctx, files)
		case <-scan.C:
			w.scan()
		}
	}
}

// handleEvent is an fsnotify event in the watched tree. On Linux a rename
// into the watched directory (x.tmp → x.txt) arrives as a Create of the new
// name, and the move out of it as a Rename of the old one.
func (w *watcher) handleEvent(ev fsnotify.Event) {
	switch {
	case ev.Has(fsnotify.Create) || ev.Has(fsnotify.Write):
		fi, err := os.Lstat(ev.Name)
		if err != nil {
			w.tracker.forget(ev.Name) // vanished between the event and the stat
			return
		}
		switch {
		case fi.IsDir():
			if w.skipDir(ev.Name) {
				return
			}
			// A new directory (a zone, add/delete): watch it and walk it
			// right away — files could have appeared before the watch.
			w.addWatch(ev.Name)
			w.walk(ev.Name)
		case fi.Mode().IsRegular() && !w.skipDir(filepath.Dir(ev.Name)) && w.wantFile(ev.Name):
			// A write event always restarts the stability window —
			// on filesystems with coarse mtime an append may not
			// change the signature.
			w.tracker.observe(ev.Name, fi.Size(), fi.ModTime(), true)
		}
	case ev.Has(fsnotify.Remove) || ev.Has(fsnotify.Rename):
		// The path is gone: forget the candidate (a deletion before
		// stability, a move to processed/) and the directory watch
		// (the kernel removes it itself).
		w.tracker.forget(ev.Name)
		delete(w.watched, ev.Name)
	default:
		// Chmod: access rights change neither the size nor the mtime.
	}
}

// emitReady delivers stably ready files. Every candidate is re-checked with
// a fresh stat: a changed or vanished file is not delivered.
func (w *watcher) emitReady(ctx context.Context, files chan<- File) {
	for _, path := range w.tracker.paths() {
		fi, err := os.Lstat(path)
		if err != nil {
			w.tracker.forget(path) // deleted before stability
			continue
		}
		f, ok := w.tracker.ready(path, fi.Size(), fi.ModTime())
		if !ok {
			continue
		}
		w.log.Debug("file is stable", "path", f.Path, "size", f.Size)
		select {
		case files <- f: // backpressure: waiting for the consumer, nothing is lost
		case <-ctx.Done():
			return
		}
	}
}

// scan is a full pass over the watched directory (the safety net for fsnotify).
func (w *watcher) scan() { w.walk(w.opts.Dir) }

// walk walks the root directory: updates the watches of subdirectories and
// the candidate states. Hidden directories and the ingest archive
// processed/failed (spec 5.1, top level) are not walked.
func (w *watcher) walk(root string) {
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if path == root {
				// The watched directory vanished (the volume dropped): restore
				// it and the watch; the pass will repeat on the next tick.
				w.log.Warn("watched directory unavailable", "dir", root, "err", err)
				if mkErr := os.MkdirAll(root, 0o755); mkErr != nil {
					w.log.Warn("creating directory", "dir", root, "err", mkErr)
				} else {
					w.addWatch(root)
				}
				return fs.SkipAll
			}
			w.log.Warn("directory walk", "path", path, "err", err)
			return nil // skip the entry, continue the walk
		}
		if d.IsDir() {
			// The walk root is not filtered: a hidden name of the watched
			// directory (ingest.dir = /data/.incoming) is a valid
			// configuration; skipDir on the root silently disabled the whole
			// watch (ING-P3-4). Subdirectories are filtered as before:
			// walk is also called for a new subdirectory by event —
			// there is no need to descend into processed/failed and
			// hidden ones.
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
		w.log.Warn("directory walk", "dir", root, "err", err)
	}
}

// skipDir — directories without input files: processed/ and failed/ at the
// top level (the ingest archive, spec 5.1) and hidden directories.
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

// wantExts is the tracked name suffixes (lower case): *.txt (spec 5.1)
// and archives of supported formats (T-133); .tar.gz is covered by the
// .gz suffix, .tgz is a separate suffix. The list mirrors ingest: ParsePath
// (internal/ingest) parses everything the watcher delivers.
var wantExts = []string{".txt", ".zip", ".tar", ".tgz", ".gz"}

// wantFile is the files whose readiness is tracked: non-hidden *.txt
// and archives of supported formats (.zip, .tar, .tar.gz, .tgz, .gz —
// T-133). Temporary ones (*.tmp, *.part) and other extensions are not
// tracked: per spec 5.1 readiness is the appearance/rename into a
// tracked name.
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

// addWatch installs a directory watch (idempotent); a failure is not fatal
// — the directory will appear in the pass and the watch will be retried.
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

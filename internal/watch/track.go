package watch

import (
	"sort"
	"time"
)

// fileSig is the state of a file whose stability is being checked: while
// the size and mtime are unchanged, the record is considered complete (spec 5.1).
type fileSig struct {
	size    int64
	modTime time.Time
}

// candidate is a candidate file: seen in state sig since moment since.
type candidate struct {
	sig   fileSig
	since time.Time
}

// tracker decides which files are ready for delivery: pure logic without I/O,
// time comes through now (a stub in tests). Redelivery of the same file is
// excluded: a delivered file is remembered by path and signature while the
// path exists in incoming (a missing path removes the entry — forget).
// If a file with a different size/mtime appears at the same path — it is
// a new file and will be delivered again; permanent idempotency (by
// content, regardless of the name) is ingest_files in the DB, task T-021.
type tracker struct {
	stableAfter time.Duration
	now         func() time.Time

	candidates map[string]candidate
	delivered  map[string]fileSig
}

func newTracker(stableAfter time.Duration, now func() time.Time) *tracker {
	return &tracker{
		stableAfter: stableAfter,
		now:         now,
		candidates:  make(map[string]candidate),
		delivered:   make(map[string]fileSig),
	}
}

// observe records the file state (size/modTime). event=true means the state
// came from a write event (Create/Write): the stability window restarts
// always, even with an unchanged signature — on filesystems with coarse
// mtime an append may not change the size/mtime, while the event guarantees
// the file was touched. event=false means a scheduled pass: the window
// resets only when the signature changed.
//
// false — a file with this state has already been delivered and must not be
// delivered again; such a path is no longer tracked.
func (t *tracker) observe(path string, size int64, modTime time.Time, event bool) bool {
	sig := fileSig{size: size, modTime: modTime}
	if d, ok := t.delivered[path]; ok && d == sig {
		return false
	}
	c, ok := t.candidates[path]
	switch {
	case !ok || c.sig != sig:
		t.candidates[path] = candidate{sig: sig, since: t.now()}
	case event:
		c.since = t.now()
		t.candidates[path] = c
	}
	return true
}

// ready checks candidate path: if its state right now (by a fresh stat)
// matches the observed one and has lived longer than stableAfter — the file
// is ready. The candidate is then consumed and moves to delivered: the same
// file is not delivered a second time.
func (t *tracker) ready(path string, size int64, modTime time.Time) (File, bool) {
	c, ok := t.candidates[path]
	if !ok {
		return File{}, false
	}
	sig := fileSig{size: size, modTime: modTime}
	if c.sig != sig || t.now().Sub(c.since) < t.stableAfter {
		return File{}, false
	}
	delete(t.candidates, path)
	t.delivered[path] = sig
	return File{Path: path, Size: size, ModTime: modTime}, true
}

// forget removes the path — the file is gone (deleted before stability or
// moved out of incoming: a transfer to processed/failed, a rename). Along
// with the candidate, the memory of the delivered file is removed: it is
// needed while the path exists in incoming, and a map kept forever would
// grow unboundedly with the number of distinct paths of a long-lived
// service (ING-P3-3). A file re-appearing at the same path with the same
// signature becomes a candidate again — permanent idempotency is provided
// by ingest_files in the DB (T-021): redelivery of the same content ends
// with a duplicate by sha256.
func (t *tracker) forget(path string) {
	delete(t.candidates, path)
	delete(t.delivered, path)
}

// paths returns candidate paths in a deterministic order.
func (t *tracker) paths() []string {
	out := make([]string, 0, len(t.candidates))
	for p := range t.candidates {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

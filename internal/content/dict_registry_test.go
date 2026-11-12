// Tests of the retired zstd dictionary registry (T-103): readability of old
// packs after retraining (lazy late registration of dict-<id>.zstd by the
// frame's dictID), memoization, a clear missing-dictionary error, and the
// reader working without an active dictionary file.

package content

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// dictCorpusAlt — a second deterministic corpus of a different template
// (a "hosting suspension page"): a different set of repeating segments —
// a different dictionary ID (an FNV of the contents), which is exactly what
// the retraining scenarios need.
func dictCorpusAlt(n, pageSize int) [][]byte {
	head := `<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">` +
		`<meta name="viewport" content="width=device-width, initial-scale=1">` +
		`<title>Suspended domain</title><link rel="stylesheet" href="/hosting.css">` +
		`<script src="/vendor/jquery.min.js"></script></head><body>` +
		`<div id="wrap"><header class="hdr"><nav class="crumbs">` +
		`<a href="/en">Home</a><a href="/faq">FAQ</a><a href="/abuse">Abuse</a>` +
		`</nav></header><main><section class="notice"><h1>Account suspended</h1>`
	foot := `</section></main><aside class="side"><p>Hosting Inc.</p>` +
		`<p>contact support@example.invalid</p></aside></div>` +
		`<footer class="ftr"><p>&copy; Hosting Inc. All rights reserved.</p>` +
		`</footer></body></html>`
	rng := rand.New(rand.NewSource(7))
	midSize := pageSize - len(head) - len(foot)
	if midSize < 1 {
		midSize = 1
	}
	pages := make([][]byte, n)
	for i := 0; i < n; i++ {
		m := make([]byte, midSize)
		rng.Read(m)
		pages[i] = append(append([]byte(head), m...), foot...)
	}
	return pages
}

// trainTwoDicts — a pair of dictionaries of different templates (old/new):
// the guarantee of different IDs is the precondition of the retraining
// scenarios.
func trainTwoDicts(t *testing.T) (oldD, newD *Dict) {
	t.Helper()
	var err error
	if oldD, err = TrainDict(dictCorpus(12, 2048), TrainOptions{DictSize: 16 << 10}); err != nil {
		t.Fatalf("TrainDict (old): %v", err)
	}
	if newD, err = TrainDict(dictCorpusAlt(12, 2048), TrainOptions{DictSize: 16 << 10}); err != nil {
		t.Fatalf("TrainDict (new): %v", err)
	}
	if oldD.ID() == newD.ID() {
		t.Fatalf("corpora of different templates produced the same ID %08x — the test does not exercise the registry", oldD.ID())
	}
	return oldD, newD
}

// newDictRegistryStore — a PackStore with a fake pool in the shared directory
// with the dictionaries (as in production: content.dir holds the packs, the
// dict.zst, and the registry files alike).
func newDictRegistryStore(t *testing.T) (*PackStore, *packFakePool, string) {
	t.Helper()
	dir := t.TempDir()
	pool := newPackFakePool()
	st, err := OpenPackStore(pool, PackOptions{Dir: dir, PackSize: 1 << 20, MaxStoreBytes: 1 << 20})
	if err != nil {
		t.Fatalf("OpenPackStore: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	t.Cleanup(func() { _, _ = SetDictFile("", nil) }) // reset the active dictionary and the registry
	return st, pool, dir
}

// TestFrameDictID — parsing the dictID from a frame header: a frame with a
// dictionary names its ID, a dictionary-less frame and garbage give 0.
func TestFrameDictID(t *testing.T) {
	d, err := TrainDict(dictCorpus(12, 2048), TrainOptions{DictSize: 16 << 10})
	if err != nil {
		t.Fatalf("TrainDict: %v", err)
	}
	page := dictCorpus(1, 2048)[0]
	if got := frameDictID(d.Compress(page)); got != d.ID() {
		t.Errorf("frameDictID(a dictionary frame) = %08x, want %08x", got, d.ID())
	}
	if got := frameDictID(Compress(page)); got != 0 {
		t.Errorf("frameDictID(a dictionary-less frame) = %08x, want 0", got)
	}
	frame := d.Compress(page)
	for _, bad := range [][]byte{nil, []byte("junk"), frame[:6]} {
		if got := frameDictID(bad); got != 0 {
			t.Errorf("frameDictID(%d bytes) = %08x, want 0", len(bad), got)
		}
	}
}

// TestDictRegistryOldPacksReadable — the retraining scenario end to end:
// content compressed with the old dictionary is readable via the registry after
// the active one is replaced; new content is written with the new one; reads are
// memoized; it also works without an active dictionary file (the registry is
// wired up by configuration).
func TestDictRegistryOldPacksReadable(t *testing.T) {
	oldD, newD := trainTwoDicts(t)
	st, pool, dir := newDictRegistryStore(t)
	ctx := context.Background()
	dictPath := filepath.Join(dir, "dict.zst")

	// Episode 1: the old dictionary is active, the content is written with it.
	if err := oldD.Save(dictPath); err != nil {
		t.Fatalf("Save (old): %v", err)
	}
	if ok, err := SetDictFile(dictPath, nil); err != nil || !ok {
		t.Fatalf("SetDictFile (old): ok=%v err=%v", ok, err)
	}
	page1 := dictCorpus(1, 2048)[0]
	put1, err := st.Put(ctx, page1)
	if err != nil {
		t.Fatalf("Put (with the old dictionary): %v", err)
	}
	// The block is really compressed with the old dictionary — otherwise the
	// registry is not the point.
	if row, ok := pool.row(put1.Hash); ok {
		frame, err := readPackBlock(dir, row.packID, row.off, row.clen)
		if err != nil {
			t.Fatalf("reading the block: %v", err)
		}
		if id := frameDictID(frame); id != oldD.ID() {
			t.Fatalf("the block compressed with dictionary %08x, want the old %08x", id, oldD.ID())
		}
	} else {
		t.Fatal("the metadata row not found")
	}

	// Retraining: the retired dictionary goes to the registry (RetireDict, as
	// crawlerctl dict train does), the new one becomes active.
	if err := RetireDict(oldD, dir); err != nil {
		t.Fatalf("RetireDict: %v", err)
	}
	if err := newD.Save(dictPath); err != nil {
		t.Fatalf("Save (new): %v", err)
	}
	if ok, err := SetDictFile(dictPath, nil); err != nil || !ok {
		t.Fatalf("SetDictFile (new): ok=%v err=%v", ok, err)
	}

	// New content is written with the new dictionary; the old one is read
	// through the registry.
	page2 := dictCorpusAlt(1, 2048)[0]
	put2, err := st.Put(ctx, page2)
	if err != nil {
		t.Fatalf("Put (with the new dictionary): %v", err)
	}
	for _, tc := range []struct {
		name string
		h    Hash
		want []byte
	}{
		{"the old content via the registry", put1.Hash, page1},
		{"the new content with the active one", put2.Hash, page2},
	} {
		got, err := st.Get(ctx, tc.h)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !bytes.Equal(got, tc.want) {
			t.Errorf("%s: the bytes changed", tc.name)
		}
	}

	// Memoization: after the first read the dictionary is in the process —
	// removing the registry file does not break reading.
	if err := os.Remove(DictRegistryPath(dir, oldD.ID())); err != nil {
		t.Fatal(err)
	}
	if got, err := st.Get(ctx, put1.Hash); err != nil || !bytes.Equal(got, page1) {
		t.Errorf("reading after the registry file removal: %v (the memoization did not work)", err)
	}
	// The file is returned: the next scenario re-installs the dictionary
	// (SetDictFile resets the memoization — like a process restart).
	if err := RetireDict(oldD, dir); err != nil {
		t.Fatal(err)
	}

	// There is no active file (not trained/deleted), the configuration path is
	// in place: the registry is wired up, old frames are readable; frames of an
	// unregistered new dictionary give a clear registry error.
	if err := os.Remove(dictPath); err != nil {
		t.Fatal(err)
	}
	if ok, err := SetDictFile(dictPath, nil); err != nil || ok {
		t.Fatalf("SetDictFile without a file: ok=%v err=%v, want (false, nil)", ok, err)
	}
	if got, err := st.Get(ctx, put1.Hash); err != nil || !bytes.Equal(got, page1) {
		t.Errorf("reading the old content without the active dictionary: %v", err)
	}
	_, err = st.Get(ctx, put2.Hash)
	if !errors.Is(err, zstd.ErrUnknownDictionary) {
		t.Errorf("the new dictionary not in the registry: err = %v, want zstd.ErrUnknownDictionary", err)
	}
}

// TestDictRegistryMissingDict — the retired dictionary was not archived (the
// registry file is missing): the read error names the registry and preserves
// the original sentinels (zstd.ErrUnknownDictionary inside ErrCorrupted).
func TestDictRegistryMissingDict(t *testing.T) {
	oldD, newD := trainTwoDicts(t)
	st, _, dir := newDictRegistryStore(t)
	ctx := context.Background()
	dictPath := filepath.Join(dir, "dict.zst")

	if err := oldD.Save(dictPath); err != nil {
		t.Fatal(err)
	}
	if _, err := SetDictFile(dictPath, nil); err != nil {
		t.Fatal(err)
	}
	page := dictCorpus(1, 2048)[0]
	put, err := st.Put(ctx, page)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Retraining "manually": the new dictionary is active, the old one is NOT
	// archived.
	if err := newD.Save(dictPath); err != nil {
		t.Fatal(err)
	}
	if _, err := SetDictFile(dictPath, nil); err != nil {
		t.Fatal(err)
	}

	_, err = st.Get(ctx, put.Hash)
	if err == nil {
		t.Fatal("reading without the dictionary in the registry: an error expected")
	}
	if !errors.Is(err, zstd.ErrUnknownDictionary) {
		t.Errorf("err = %v, want a zstd.ErrUnknownDictionary wrapping", err)
	}
	if !errors.Is(err, ErrCorrupted) {
		t.Errorf("err = %v, the previous ErrCorrupted wrapping lost", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "the registry") || !strings.Contains(msg, DictRegistryPath(dir, oldD.ID())) {
		t.Errorf("the error does not name the registry: %s", msg)
	}

	// A file placed into the registry on a live process is picked up without
	// a restart (a negative result is not memoized).
	if err := RetireDict(oldD, dir); err != nil {
		t.Fatal(err)
	}
	if got, err := st.Get(ctx, put.Hash); err != nil || !bytes.Equal(got, page) {
		t.Errorf("reading after the file returned to the registry: %v", err)
	}
}

// TestDictRegistryNameMismatch — a registry file with foreign contents (the
// dict-<id>.zstd name does not match the dictID inside): a clear error rather
// than the decoder's dry "unknown dictionary".
func TestDictRegistryNameMismatch(t *testing.T) {
	oldD, newD := trainTwoDicts(t)
	st, _, dir := newDictRegistryStore(t)
	ctx := context.Background()
	dictPath := filepath.Join(dir, "dict.zst")

	if err := oldD.Save(dictPath); err != nil {
		t.Fatal(err)
	}
	if _, err := SetDictFile(dictPath, nil); err != nil {
		t.Fatal(err)
	}
	page := dictCorpus(1, 2048)[0]
	put, err := st.Put(ctx, page)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// An archive under a foreign name: newD's dictionary file named with
	// oldD's ID.
	if err := os.WriteFile(DictRegistryPath(dir, oldD.ID()), newD.Raw(), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := newD.Save(dictPath); err != nil {
		t.Fatal(err)
	}
	if _, err := SetDictFile(dictPath, nil); err != nil {
		t.Fatal(err)
	}

	_, err = st.Get(ctx, put.Hash)
	if !errors.Is(err, zstd.ErrUnknownDictionary) {
		t.Fatalf("err = %v, want zstd.ErrUnknownDictionary", err)
	}
	if !strings.Contains(err.Error(), "the file name does not match") {
		t.Errorf("the error does not explain the registry file mismatch: %v", err)
	}
}

// TestDictRegistryLazyLoadOnce — a lazy load happens once: the registry file
// is replaced with garbage after the first read — the memoization keeps the
// dictionary.
func TestDictRegistryLazyLoadOnce(t *testing.T) {
	oldD, newD := trainTwoDicts(t)
	st, _, dir := newDictRegistryStore(t)
	ctx := context.Background()
	dictPath := filepath.Join(dir, "dict.zst")

	if err := oldD.Save(dictPath); err != nil {
		t.Fatal(err)
	}
	if _, err := SetDictFile(dictPath, nil); err != nil {
		t.Fatal(err)
	}
	page := dictCorpus(1, 2048)[0]
	put, err := st.Put(ctx, page)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := RetireDict(oldD, dir); err != nil {
		t.Fatal(err)
	}
	if err := newD.Save(dictPath); err != nil {
		t.Fatal(err)
	}
	if _, err := SetDictFile(dictPath, nil); err != nil {
		t.Fatal(err)
	}

	if got, err := st.Get(ctx, put.Hash); err != nil || !bytes.Equal(got, page) {
		t.Fatalf("the first read via the registry: %v", err)
	}
	// Corrupting the registry file is invisible to the process: the dictionary
	// is already loaded.
	if err := os.WriteFile(DictRegistryPath(dir, oldD.ID()), []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, err := st.Get(ctx, put.Hash); err != nil || !bytes.Equal(got, page) {
		t.Errorf("a repeated read with the registry file corrupted: %v (the load not memoized?)", err)
	}
}

// dictCraftHeaderPack — a header-only pack with a given dictID
// (WarnDictMismatch diagnostics).
func dictCraftHeaderPack(t *testing.T, dir string, id int32, dictID uint32) {
	t.Helper()
	hdr := packHeader{id: id, maxSize: 1 << 20, dictID: dictID}
	if err := os.WriteFile(filepath.Join(dir, packFileName(id)), hdr.bytes(), 0o644); err != nil {
		t.Fatalf("building pack %d: %v", id, err)
	}
}

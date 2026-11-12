// Tests of the trainable zstd dictionary (T-043): training on a deterministic
// synthetic corpus, the fact of compression improvement (without brittle
// percentages), roundtrip, determinism, the fallback without a dictionary,
// loading from a file.

package content

import (
	"bytes"
	"errors"
	"io/fs"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// dictCorpus — a deterministic corpus of "parking pages": a shared
// template (header, navigation, footer) and an incompressible random middle.
// These are exactly the pages the dictionary targets (spec 5.7: parking
// and suspension pages). The generator with a fixed seed keeps the corpus
// stable between runs, as spec 11.1 requires of test data.
func dictCorpus(n, pageSize int) [][]byte {
	head := `<!DOCTYPE html><html lang="ru"><head><meta charset="utf-8">` +
		`<meta name="viewport" content="width=device-width, initial-scale=1">` +
		`<title>The domain is available</title><link rel="stylesheet" href="/parking.css">` +
		`<link rel="stylesheet" href="/vendor/bootstrap.min.css">` +
		`<script src="/vendor/analytics.js" defer></script></head><body>` +
		`<header class="site-header"><nav><ul class="nav-menu">` +
		`<li><a href="/">Home</a></li><li><a href="/about">About</a></li>` +
		`<li><a href="/contacts">Contacts</a></li><li><a href="/order">Order</a></li>` +
		`</ul></nav></header><main class="container"><div class="row">` +
		`<div class="col-md-8"><section class="content"><h1>The domain is for sale</h1>`
	foot := `</section></div><aside class="col-md-4"><div class="banner">` +
		`This domain is for sale: admin@example.invalid</div></aside></div></main>` +
		`<footer class="site-footer"><p>&copy; Parking Ltd. All rights reserved.</p>` +
		`<p><a href="/terms">Terms</a> · <a href="/privacy">Privacy</a></p>` +
		`</footer></body></html>`
	rng := rand.New(rand.NewSource(1))
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

func TestTrainDictImprovesCompression(t *testing.T) {
	corpus := dictCorpus(24, 2048)
	d, err := TrainDict(corpus, TrainOptions{DictSize: 32 << 10})
	if err != nil {
		t.Fatalf("TrainDict: %v", err)
	}

	// Evaluated on a new page of the same template (not from the training
	// corpus).
	page := dictCorpus(1, 2048)[0]
	plain := Compress(page)
	withDict := d.Compress(page)
	if len(withDict) >= len(plain) {
		t.Errorf("the dictionary did not improve the compression: %d bytes without it, %d with it",
			len(plain), len(withDict))
	}
	t.Logf("a %d-byte page: %d without the dictionary, %d with it (%.1f%% of the original)",
		len(page), len(plain), len(withDict), 100*float64(len(withDict))/float64(len(page)))

	got, err := d.Decompress(withDict)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if !bytes.Equal(got, page) {
		t.Error("the dictionary round-trip changed the data")
	}

	// The dictionary-equipped decoder reads dictionary-less frames too.
	got, err = d.Decompress(plain)
	if err != nil {
		t.Fatalf("Decompress of a dictionary-less frame: %v", err)
	}
	if !bytes.Equal(got, page) {
		t.Error("the dictionary-less frame round-trip changed the data")
	}
}

func TestTrainDictDeterministic(t *testing.T) {
	corpus := dictCorpus(12, 2048)
	a, err := TrainDict(corpus, TrainOptions{DictSize: 16 << 10})
	if err != nil {
		t.Fatalf("TrainDict: %v", err)
	}
	b, err := TrainDict(corpus, TrainOptions{DictSize: 16 << 10})
	if err != nil {
		t.Fatalf("TrainDict (repeat): %v", err)
	}
	if !bytes.Equal(a.Raw(), b.Raw()) {
		t.Error("the same corpus produced different dictionaries")
	}
	if a.ID() == 0 {
		t.Error("a dictionary ID of 0 — forbidden by the format")
	}
}

func TestTrainDictValidation(t *testing.T) {
	if _, err := TrainDict(nil, TrainOptions{}); err == nil {
		t.Error("an empty corpus: an error expected")
	}
	// No repeats: random data, every segment is unique.
	random := make([][]byte, 8)
	for i := range random {
		b := make([]byte, 512)
		for j := range b {
			b[j] = byte(i*131 + j*37 + j*j%251)
		}
		random[i] = b
	}
	if _, err := TrainDict(random, TrainOptions{DictSize: 1 << 10}); err == nil {
		t.Error("a corpus without repeats: an error expected")
	}
	for _, size := range []int{MinDictSize - 1, MaxDictSize + 1} {
		if _, err := TrainDict(dictCorpus(4, 1024), TrainOptions{DictSize: size}); err == nil {
			t.Errorf("DictSize=%d: an error expected", size)
		}
	}
	// A zero DictSize is the default; an error only if the corpus cannot reach
	// the minimum.
	if _, err := TrainDict(dictCorpus(16, 4096), TrainOptions{}); err != nil {
		t.Errorf("the default DictSize: %v", err)
	}
}

// TestWarnDictMismatch — cross-checking the active dictionary against the
// newest pack: a mismatch is Warn, a match and an empty directory are
// silence.
func TestWarnDictMismatch(t *testing.T) {
	dict, err := TrainDict(dictCorpus(12, 2048), TrainOptions{DictSize: 16 << 10})
	if err != nil {
		t.Fatalf("TrainDict: %v", err)
	}

	newBuf := func() (*bytes.Buffer, *slog.Logger) {
		buf := &bytes.Buffer{}
		return buf, slog.New(slog.NewTextHandler(buf, nil))
	}
	warned := func(buf *bytes.Buffer) bool {
		return strings.Contains(buf.String(), "does not match the newest pack")
	}

	t.Run("a mismatch — Warn", func(t *testing.T) {
		dir := t.TempDir()
		compactCraftPack(t, dir, 1, nil) // the newest pack (dictID 0 in the header)
		_, _ = SetDictFile("", nil)      // the registry is not wired up — only the active dictionary
		SetDict(dict)                    // an active dictionary with a non-zero ID
		defer SetDict(nil)
		buf, log := newBuf()
		WarnDictMismatch(dir, log)
		if !warned(buf) {
			t.Errorf("the mismatch not noticed: %s", buf.String())
		}
	})

	t.Run("a registry mismatch with no file — Warn", func(t *testing.T) {
		packs, reg := t.TempDir(), t.TempDir()
		newD, err := TrainDict(dictCorpusAlt(12, 2048), TrainOptions{DictSize: 16 << 10})
		if err != nil {
			t.Fatalf("TrainDict: %v", err)
		}
		dictCraftHeaderPack(t, packs, 1, dict.ID()) // a pack of the retired dictionary
		if err := newD.Save(filepath.Join(reg, "dict.zst")); err != nil {
			t.Fatal(err)
		}
		_, _ = SetDictFile(filepath.Join(reg, "dict.zst"), nil) // the registry is wired up, the retired file is absent
		defer func() { _, _ = SetDictFile("", nil) }()
		buf, log := newBuf()
		WarnDictMismatch(packs, log)
		if !warned(buf) {
			t.Errorf("the mismatch without a registry file not noticed: %s", buf.String())
		}
	})

	t.Run("the previous dictionary in the registry — Info, not Warn", func(t *testing.T) {
		packs, reg := t.TempDir(), t.TempDir()
		newD, err := TrainDict(dictCorpusAlt(12, 2048), TrainOptions{DictSize: 16 << 10})
		if err != nil {
			t.Fatalf("TrainDict: %v", err)
		}
		dictCraftHeaderPack(t, packs, 1, dict.ID())
		if err := dict.Save(DictRegistryPath(reg, dict.ID())); err != nil {
			t.Fatal(err)
		}
		if err := newD.Save(filepath.Join(reg, "dict.zst")); err != nil {
			t.Fatal(err)
		}
		_, _ = SetDictFile(filepath.Join(reg, "dict.zst"), nil)
		defer func() { _, _ = SetDictFile("", nil) }()
		buf, log := newBuf()
		WarnDictMismatch(packs, log)
		if warned(buf) {
			t.Errorf("a false Warn with the dictionary in the registry: %s", buf.String())
		}
		if !strings.Contains(buf.String(), "via the registry") {
			t.Errorf("the registry readability not reflected in the log:\n%s", buf.String())
		}
	})

	t.Run("a match — silence", func(t *testing.T) {
		dir := t.TempDir()
		compactCraftPack(t, dir, 1, nil)
		SetDict(nil) // both dictIDs are zero
		buf, log := newBuf()
		WarnDictMismatch(dir, log)
		if warned(buf) {
			t.Errorf("a false Warn on a match: %s", buf.String())
		}
	})

	t.Run("no packs — silence", func(t *testing.T) {
		SetDict(dict)
		defer SetDict(nil)
		buf, log := newBuf()
		WarnDictMismatch(t.TempDir(), log)
		if warned(buf) {
			t.Errorf("the Warn on an empty directory: %s", buf.String())
		}
	})
}

func TestDictSaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dict.zst")

	d, err := TrainDict(dictCorpus(12, 2048), TrainOptions{DictSize: 16 << 10})
	if err != nil {
		t.Fatalf("TrainDict: %v", err)
	}
	if err := d.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := LoadDict(path)
	if err != nil {
		t.Fatalf("LoadDict: %v", err)
	}
	if loaded.ID() != d.ID() {
		t.Errorf("the ID after the load %d, want %d", loaded.ID(), d.ID())
	}
	page := dictCorpus(1, 2048)[0]
	got, err := loaded.Decompress(loaded.Compress(page))
	if err != nil || !bytes.Equal(got, page) {
		t.Errorf("the loaded dictionary round-trip: %v", err)
	}

	// No temporary files were left behind by Save.
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Errorf("something other than the dictionary remained in the directory: %v", names)
	}

	// A missing file is fs.ErrNotExist, a broken one is a format error.
	if _, err := LoadDict(filepath.Join(dir, "nope.zst")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("an absent file: err = %v, want fs.ErrNotExist", err)
	}
	bad := filepath.Join(dir, "bad.zst")
	if err := os.WriteFile(bad, []byte("this is not a dictionary"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDict(bad); err == nil {
		t.Error("a broken dictionary file: an error expected")
	}
}

func TestWithDictPackageFallback(t *testing.T) {
	t.Cleanup(func() { SetDict(nil) })

	// Without a dictionary — the same compression as the framework's.
	src := []byte("<html>domain parking</html>")
	SetDict(nil)
	if got := CompressWithDict(src); !bytes.Equal(got, Compress(src)) {
		t.Error("without a dictionary set, CompressWithDict must match Compress")
	}
	got, err := DecompressWithDict(Compress(src))
	if err != nil || !bytes.Equal(got, src) {
		t.Errorf("the fallback decompression: %v", err)
	}

	// With a dictionary — dictionary compression, and decompression understands
	// both kinds of frames.
	d, err := TrainDict(dictCorpus(12, 2048), TrainOptions{DictSize: 16 << 10})
	if err != nil {
		t.Fatalf("TrainDict: %v", err)
	}
	SetDict(d)
	page := dictCorpus(1, 2048)[0]
	got, err = DecompressWithDict(CompressWithDict(page))
	if err != nil || !bytes.Equal(got, page) {
		t.Errorf("the round-trip via the active dictionary: %v", err)
	}
	got, err = DecompressWithDict(Compress(page))
	if err != nil || !bytes.Equal(got, page) {
		t.Errorf("the active dictionary does not read a dictionary-less frame: %v", err)
	}
}

func TestSetDictFile(t *testing.T) {
	t.Cleanup(func() { SetDict(nil) })
	var logBuf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logBuf, nil))

	dir := t.TempDir()
	path := filepath.Join(dir, "dict.zst")
	d, err := TrainDict(dictCorpus(12, 2048), TrainOptions{DictSize: 16 << 10})
	if err != nil {
		t.Fatalf("TrainDict: %v", err)
	}
	if err := d.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	ok, err := SetDictFile(path, log)
	if err != nil || !ok {
		t.Fatalf("SetDictFile: ok=%v err=%v", ok, err)
	}
	if !strings.Contains(logBuf.String(), "zstd dictionary loaded") {
		t.Errorf("the load not reflected in the log:\n%s", logBuf.String())
	}

	// A missing file is not an error, the service runs without a dictionary.
	logBuf.Reset()
	ok, err = SetDictFile(filepath.Join(dir, "absent.zst"), log)
	if err != nil || ok {
		t.Fatalf("an absent file: ok=%v err=%v, want (false, nil)", ok, err)
	}
	if !strings.Contains(logBuf.String(), "without the dictionary") {
		t.Errorf("the fallback not reflected in the log:\n%s", logBuf.String())
	}
	if got := CompressWithDict(nil); !bytes.Equal(got, Compress(nil)) {
		t.Error("after the fallback CompressWithDict must match Compress")
	}

	// An empty path is just a switch-off, with nothing logged.
	logBuf.Reset()
	if ok, err := SetDictFile("", log); err != nil || ok {
		t.Fatalf("an empty path: ok=%v err=%v", ok, err)
	}
	if logBuf.Len() != 0 {
		t.Errorf("an empty path must not be logged:\n%s", logBuf.String())
	}

	// A broken file is an error (a silent fallback would hide the problem).
	bad := filepath.Join(dir, "bad.zst")
	if err := os.WriteFile(bad, []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := SetDictFile(bad, log); err == nil {
		t.Error("a broken dictionary file: an error expected")
	}
}

func TestTrainDictSyntheticScanLimit(t *testing.T) {
	// A corpus larger than trainScanLimit: training takes the prefix and works.
	corpus := dictCorpus(64, 128<<10)
	if _, err := TrainDict(corpus, TrainOptions{DictSize: 16 << 10}); err != nil {
		t.Fatalf("TrainDict on a corpus beyond the scan limit: %v", err)
	}
}

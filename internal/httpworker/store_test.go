// Unit tests of the content reconciliation (T-056/T-147): the worker's early Put puts
// every downloaded page immediately (the bodies are not held in memory),
// ContentSaver.Save distributes the early Put references — the originals inherit
// their reference, the aliases lose content_hash, the folded variants and the extra
// downloads release the reference by Delete; ErrStoreFull/ErrTooLarge at
// the early Put — the result is alive with a nil hash and the "content not
// stored" sign (the worker's Warn), a truncated body is not stored at all;
// an early Put failure, a ctx cancellation and Delete errors drop the task/Save
// (a repeated delivery); an empty body (T-138) — valid content with a live
// hash, including from the final URLs' cache. The identifiers with the
// store* prefix against collisions with the package's neighboring files.

package httpworker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"crawler/internal/content"
	"crawler/internal/httpfetch"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// --- the fake store --------------------------------------------------------

// storeFake — a fake content.ContentStore with the live semantics of T-041/T-042:
// the same bytes — Created=false and refcount+1 without writing new blocks; a
// newErr failure affects only NEW blocks (as in PackStore.Put: the dedup UPDATE
// runs before the disk threshold, T-046 — the references to live content keep
// working on overflow). badHash — a violation of the HashOf contract (T-041)
// for checking the worker's guard; delErr — a Delete failure (the T-147 reconciliation);
// delMissing — a Delete of a non-existent reference (ErrNotFound).
type storeFake struct {
	mu         sync.Mutex
	blocks     map[content.Hash][]byte
	refs       map[content.Hash]int
	puts       int // all the Put calls
	writes     int // the calls that wrote new bytes
	deletes    int // all the Delete calls
	newErr     func(data []byte) error
	badHash    bool
	delErr     error
	delMissing bool
}

func newStoreFake() *storeFake {
	return &storeFake{blocks: make(map[content.Hash][]byte), refs: make(map[content.Hash]int)}
}

func (f *storeFake) Put(ctx context.Context, data []byte) (content.PutResult, error) {
	if err := ctx.Err(); err != nil {
		return content.PutResult{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	h := content.HashOf(data)
	if f.refs[h] > 0 {
		f.refs[h]++
		return content.PutResult{Hash: h}, nil
	}
	if f.newErr != nil {
		if err := f.newErr(data); err != nil {
			return content.PutResult{}, err
		}
	}
	f.blocks[h] = append([]byte(nil), data...)
	f.refs[h] = 1
	f.writes++
	if f.badHash {
		return content.PutResult{Hash: content.HashOf([]byte("a foreign hash"))}, nil
	}
	return content.PutResult{Hash: h, Created: true}, nil
}

func (f *storeFake) Get(_ context.Context, h content.Hash) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refs[h] <= 0 {
		return nil, content.ErrNotFound
	}
	return append([]byte(nil), f.blocks[h]...), nil
}

func (f *storeFake) Delete(ctx context.Context, h content.Hash) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes++
	if f.delErr != nil {
		return f.delErr
	}
	if f.delMissing || f.refs[h] <= 0 {
		return content.ErrNotFound
	}
	f.refs[h]--
	return nil
}

func (f *storeFake) snapshot() (puts, writes, deletes int, refs map[content.Hash]int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make(map[content.Hash]int, len(f.refs))
	for h, n := range f.refs {
		cp[h] = n
	}
	return f.puts, f.writes, f.deletes, cp
}

// storeSaver — a reconciliation over the store with a log interception.
func storeSaver(t *testing.T, st content.ContentStore, logBuf *bytes.Buffer) *ContentSaver {
	t.Helper()
	s, err := NewContentSaver(st, slog.New(slog.NewTextHandler(logBuf, nil)))
	if err != nil {
		t.Fatalf("NewContentSaver: %v", err)
	}
	return s
}

// storeWorker — a worker over a load scenario (see workerFakeFetch) and
// the early Put store (T-147); PerDomain=1 — a deterministic check
// order (the cache hits are stable, as in the worker tests), logBuf
// intercepts the worker's Warn.
func storeWorker(t *testing.T, script map[string]*workerPageScript, st content.ContentStore, logBuf *bytes.Buffer) *Worker {
	t.Helper()
	fetch, _ := workerFakeFetch(script)
	return workerWorker(t, fetch, func(o *Options) {
		o.Store = st
		o.PerDomain = 1
		if logBuf != nil {
			o.Log = slog.New(slog.NewTextHandler(logBuf, nil))
		}
	})
}

// storePageBody — distinguishable page bodies for the scenarios.
func storePageBody(label string) string {
	return "<html><body>" + strings.Repeat("the page text "+label+" for storing in the content store ", 24) + "</body></html>"
}

// storeRun — a single domain task with the "label" subdomains in the id 0..N order.
func storeRun(t *testing.T, w *Worker, name string, labels ...string) *DomainResult {
	t.Helper()
	subs := make([]queue.HTTPSubdomain, len(labels))
	for i, l := range labels {
		subs[i] = workerSubMsg(int16(i), l)
	}
	res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 1, DomainID: 1, Name: name, Subdomains: subs,
	})
	if err != nil {
		t.Fatalf("ProcessDomain(%s): %v", name, err)
	}
	return res
}

// --- distributing the early Put references (spec 5.5 item 6 "the content is stored once") --

// TestStoreOriginalsClaimEarlyPuts — the TestWorkerProcessDomain scenario: both
// bare checks download the home page (two downloads of one content —
// different starting URLs), www-https takes it from the final URLs' cache; the class
// bare+www is stored by ONE block: the www v0 original inherits its early
// Put reference, the extra reference of the second download is removed by Delete;
// the independent records (the www v1 variant, api) own their own. The alias
// deduplication goes by the hashes — the bodies are not needed by the records (T-147).
func TestStoreOriginalsClaimEarlyPuts(t *testing.T) {
	home, old := storePageBody("home"), storePageBody("old")
	api := storePageBody("api")
	script := map[string]*workerPageScript{
		"example.com": { // both bare checks: a redirect to www
			status: 301, body: home, final: "https://www.example.com/",
			steps: []httpfetch.RedirectStep{{URL: "https://example.com/", Status: 301}},
		},
		"http://www.example.com/": {
			status: 200, body: old, final: "http://old.example.com/",
			steps: []httpfetch.RedirectStep{{URL: "http://www.example.com/", Status: 301}},
		},
		"https://api.example.com/": {err: workerTimeoutErr{}},
		"api.example.com":          {status: 200, body: api},
	}
	st := newStoreFake()
	//	// PerDomain=1: a deterministic check order (as in the worker tests).
	w := storeWorker(t, script, st, nil)
	res := storeRun(t, w, "example.com", "", "www", "api")
	bare, www, apiSub := &res.Subdomains[0], &res.Subdomains[1], &res.Subdomains[2]
	//	// Before Save the alias's hash is still in place (the variant logic's material, T-053).
	if bare.Records[0].ContentHash == nil {
		t.Fatal("before Save the alias hash is absent (the scenario is broken)")
	}
	if err := storeSaver(t, st, &bytes.Buffer{}).Save(context.Background(), res); err != nil {
		t.Fatalf("Save: %v", err)
	}

	//	// Early Puts (T-147): home was downloaded twice (the https and http checks of bare),
	//	// old and api — once each; three new blocks.
	puts, writes, deletes, refs := st.snapshot()
	if puts != 4 || writes != 3 {
		t.Fatalf("Put: %d calls, %d new blocks — expected 4 and 3 (home was fetched twice)", puts, writes)
	}
	//	// The reconciliation removed the extra home reference: one reference per record.
	homeHash := content.HashOf([]byte(home))
	if refs[homeHash] != 1 {
		t.Errorf("the home refcount: %d, one reference expected (the extra fetch was released by Delete)", refs[homeHash])
	}
	if refs[content.HashOf([]byte(old))] != 1 || refs[content.HashOf([]byte(api))] != 1 {
		t.Error("the independent records do not own their references")
	}
	if deletes != 1 {
		t.Errorf("Delete: %d calls, 1 expected (the second home fetch)", deletes)
	}

	//	// The alias: content_hash removed (NULL in the DB), the "not stored" sign is NOT
	//	// set — the class's content is stored by the original record.
	bareRec := &bare.Records[0]
	if bareRec.ContentHash != nil || bareRec.ContentNotStored {
		t.Errorf("the alias: hash=%v not_stored=%v (nil/false expected — the content is at the original)",
			bareRec.ContentHash, bareRec.ContentNotStored)
	}
	//	// The originals: the hash = HashOf(body), the content is available from the store.
	for _, c := range []struct {
		name string
		rec  *Record
		body string
	}{
		{"www v0", &www.Records[0], home},
		{"www v1", &www.Records[1], old},
		{"api", &apiSub.Records[0], api},
	} {
		if c.rec.ContentHash == nil || c.rec.ContentNotStored {
			t.Fatalf("%s: hash=%v not_stored=%v", c.name, c.rec.ContentHash, c.rec.ContentNotStored)
		}
		if *c.rec.ContentHash != content.HashOf([]byte(c.body)) {
			t.Errorf("%s: the record hash ≠ HashOf(the body)", c.name)
		}
		got, err := st.Get(context.Background(), *c.rec.ContentHash)
		if err != nil || string(got) != c.body {
			t.Errorf("%s: Get by the record hash: %v (the bytes match: %v)", c.name, err, string(got) == c.body)
		}
	}
}

// TestStoreDedupAcrossDomains — the store level (§8): the same
// parking page of two domains — the second early Put is deduplicated
// (Created=false, no new bytes), every domain after the folding owns
// one reference; both records hold a live hash. The task level (one block
// per class) — TestStoreOriginalsClaimEarlyPuts.
func TestStoreDedupAcrossDomains(t *testing.T) {
	parking := storePageBody("parking")
	script := func(host string) map[string]*workerPageScript {
		return map[string]*workerPageScript{
			host:                   {status: 200, body: parking},
			"http://" + host + "/": {status: 200, body: parking},
		}
	}
	st := newStoreFake()
	w1 := storeWorker(t, script("park1.test"), st, nil)
	w2 := storeWorker(t, script("park2.test"), st, nil)

	res1 := storeRun(t, w1, "park1.test", "")
	res2 := storeRun(t, w2, "park2.test", "")
	saver := storeSaver(t, st, &bytes.Buffer{})
	for _, res := range []*DomainResult{res1, res2} {
		if err := saver.Save(context.Background(), res); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}

	//	// Each domain downloads the parking twice (https and http — one site,
	//	// the records fold into one): 4 early Puts, 1 new block, the reconciliation
	//	// removed one extra reference each — one reference per domain remains.
	puts, writes, deletes, refs := st.snapshot()
	h := content.HashOf([]byte(parking))
	if puts != 4 || writes != 1 {
		t.Fatalf("Put: %d calls, %d new blocks — expected 4 and 1 (the cross-domain dedup)", puts, writes)
	}
	if deletes != 2 {
		t.Fatalf("Delete: %d calls, 2 expected (the extra fetches of its own protocols)", deletes)
	}
	if refs[h] != 2 {
		t.Errorf("the parking refcount: %d, 2 expected (one reference per domain)", refs[h])
	}
	if res1.Subdomains[0].Records[0].ContentHash == nil || *res1.Subdomains[0].Records[0].ContentHash != h {
		t.Error("the first record does not hold the parking hash")
	}
	if res2.Subdomains[0].Records[0].ContentHash == nil || *res2.Subdomains[0].Records[0].ContentHash != h {
		t.Error("the second record does not hold the parking hash")
	}
}

// --- the overflow and the limits (spec 8, T-046) ---------------------------------------

// TestStoreFullResultAlive — ErrStoreFull at the early Put: the content is not
// stored, but the domain result lives (Save = nil): a record with
// content_hash = nil and the ContentNotStored sign, a Warn in the worker's log.
// A dedup reference to the ALREADY live content passes even on overflow
// (PackStore runs the UPDATE before the disk threshold, T-046): a record with
// an existing hash is stored the usual way.
func TestStoreFullResultAlive(t *testing.T) {
	warm, fresh := storePageBody("warm"), storePageBody("fresh")
	page := func(host, body string) map[string]*workerPageScript {
		return map[string]*workerPageScript{
			host:                   {status: 200, body: body},
			"http://" + host + "/": {status: 200, body: body},
		}
	}
	st := newStoreFake()
	full := false
	//	// The wrapped sentinel — the real Put errors are wrapped into a context,
	//	// the worker distinguishes the branches by errors.Is, not by the text (T-041).
	st.newErr = func([]byte) error {
		if full {
			return errors.Join(content.ErrStoreFull, errors.New("0 bytes free"))
		}
		return nil
	}
	saver := storeSaver(t, st, &bytes.Buffer{})

	//	// 1) before the overflow the warm content was written by the early Put.
	resWarm := storeRun(t, storeWorker(t, page("warm.test", warm), st, nil), "warm.test", "")
	if err := saver.Save(context.Background(), resWarm); err != nil {
		t.Fatalf("Save(warm): %v", err)
	}

	//	// 2) the disk is full: the same content of another domain — the early Put's dedup
	//	// reference passes (without new bytes), the record's hash is live.
	full = true
	resWarm2 := storeRun(t, storeWorker(t, page("warm2.test", warm), st, nil), "warm2.test", "")
	if err := saver.Save(context.Background(), resWarm2); err != nil {
		t.Fatalf("Save(warm2): %v (the dedup reference must pass at a full disk)", err)
	}
	rec := &resWarm2.Subdomains[0].Records[0]
	if rec.ContentHash == nil || rec.ContentNotStored {
		t.Errorf("the dedup reference at a full disk: hash=%v not_stored=%v", rec.ContentHash, rec.ContentNotStored)
	}

	//	// 3) new content on a full disk — an early Put skip: the
	//	// worker's Warn, the result is alive, the records without content.
	var wlog bytes.Buffer
	resFresh := storeRun(t, storeWorker(t, page("fresh.test", fresh), st, &wlog), "fresh.test", "")
	flog := &bytes.Buffer{}
	if err := storeSaver(t, st, flog).Save(context.Background(), resFresh); err != nil {
		t.Fatalf("Save(fresh): %v — the overflow does not fail the result (spec 8)", err)
	}
	rec = &resFresh.Subdomains[0].Records[0]
	if rec.ContentHash != nil || !rec.ContentNotStored {
		t.Errorf("the record at a full disk: hash=%v not_stored=%v (nil/true expected)",
			rec.ContentHash, rec.ContentNotStored)
	}
	if _, _, _, refs := st.snapshot(); refs[content.HashOf([]byte(fresh))] != 0 {
		t.Error("the fresh content got into the store at the overflow")
	}
	if msg := wlog.String(); !strings.Contains(msg, "not stored") || !strings.Contains(msg, "unavailable") {
		t.Errorf("the worker overflow Warn is not written: %q", msg)
	}
	if msg := flog.String(); !strings.Contains(msg, "not stored") {
		t.Errorf("the reconciliation Warn about records without content is not written: %q", msg)
	}
}

// TestStoreTooLarge — ErrTooLarge at the early Put (a body over
// max_store_bytes, spec 5.5): the content is rejected by the store entirely —
// the record is alive, content_hash = nil, ContentNotStored, the worker's Warn;
// Save does not fail.
func TestStoreTooLarge(t *testing.T) {
	small := storePageBody("small")
	big := storePageBody("a large page")
	script := map[string]*workerPageScript{
		"big.test":         {status: 200, body: big},
		"http://big.test/": {status: 200, body: big},
	}
	st := newStoreFake()
	st.newErr = func(data []byte) error {
		if len(data) > len(small) {
			return errors.Join(content.ErrTooLarge, errors.New("300 bytes at the limit of 100"))
		}
		return nil
	}
	//	// Both big.test checks converge on one URL → 1 https record; one
	//	// record is enough for the over-limit body. PerDomain=1 — the check
	//	// order is deterministic.
	var wlog bytes.Buffer
	w := workerWorker(t, mustFetch(script), func(o *Options) {
		o.Store = st
		o.PerDomain = 1
		o.Log = slog.New(slog.NewTextHandler(&wlog, nil))
	})
	res := storeRun(t, w, "big.test", "")

	if err := storeSaver(t, st, &bytes.Buffer{}).Save(context.Background(), res); err != nil {
		t.Fatalf("Save: %v — the over-limit content does not fail the result", err)
	}
	rec := &res.Subdomains[0].Records[0]
	if rec.ContentHash != nil || !rec.ContentNotStored {
		t.Errorf("the record: hash=%v not_stored=%v (nil/true expected)", rec.ContentHash, rec.ContentNotStored)
	}
	if _, writes, _, _ := st.snapshot(); writes != 0 {
		t.Errorf("the over-limit content was stored: %d blocks", writes)
	}
	if msg := wlog.String(); !strings.Contains(msg, "not stored") {
		t.Errorf("the worker limit Warn is not written: %q", msg)
	}
}

// TestStoreTruncatedNotStored — a body truncated by the max_body_bytes cap
// (T-049) receives no early Put at all: the truncated bytes' hash would lie
// during the deduplication; the record lives with a nil hash and the sign, the neighboring normal
// record is stored.
func TestStoreTruncatedNotStored(t *testing.T) {
	huge := strings.Repeat("a", int(httpfetch.DefaultLimitBodyBytes)+128)
	normal := storePageBody("normal")
	script := map[string]*workerPageScript{
		"huge.test":            {status: 200, body: huge},
		"http://huge.test/":    {status: 200, body: huge},
		"ok.huge.test":         {status: 200, body: normal},
		"http://ok.huge.test/": {status: 200, body: normal},
	}
	st := newStoreFake()
	res := storeRun(t, storeWorker(t, script, st, nil), "huge.test", "", "ok")
	hugeRec := &res.Subdomains[0].Records[0]
	if !hugeRec.Truncated || hugeRec.ContentHash == nil {
		t.Fatalf("the scenario is broken: truncated=%v hash=%v", hugeRec.Truncated, hugeRec.ContentHash)
	}
	if hugeRec.ContentStored {
		t.Fatal("the truncated page is marked stored")
	}

	if err := storeSaver(t, st, &bytes.Buffer{}).Save(context.Background(), res); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if hugeRec.ContentHash != nil || !hugeRec.ContentNotStored {
		t.Errorf("the truncated record: hash=%v not_stored=%v (nil/true expected)",
			hugeRec.ContentHash, hugeRec.ContentNotStored)
	}
	//	// ok was downloaded twice (both protocols, one site → 1 record): 2 early
	//	// Puts, the reconciliation removed the extra one — exactly one block and one reference.
	_, writes, deletes, refs := st.snapshot()
	if writes != 1 || refs[content.HashOf([]byte(normal))] != 1 {
		t.Errorf("only the normal record was stored: %d blocks, %d normal references", writes, refs[content.HashOf([]byte(normal))])
	}
	if deletes != 1 {
		t.Errorf("Delete: %d calls, 1 expected (the extra normal fetch)", deletes)
	}
}

// --- the early Put and reconciliation errors: a repeated delivery (spec 3.4) -------------------

// TestStoreWorkerPutErrors — a third-party early Put error and a violation
// of the HashOf contract drop the task in ProcessDomain (T-147): the content
// reference received nothing, silence would leave it unstored until
// the T-030 recheck; a repeated delivery will replay the episode.
func TestStoreWorkerPutErrors(t *testing.T) {
	script := func(host string) map[string]*workerPageScript {
		return map[string]*workerPageScript{
			host:                   {status: 200, body: storePageBody(host)},
			"http://" + host + "/": {status: 200, body: storePageBody(host)},
		}
	}
	t.Run("a foreign Put error", func(t *testing.T) {
		st := newStoreFake()
		st.newErr = func([]byte) error { return errors.New("the DB is unavailable") }
		_, err := storeRunErr(t, storeWorker(t, script("boom.test"), st, nil), "boom.test")
		if err == nil || !strings.Contains(err.Error(), "content saving") {
			t.Fatalf("ProcessDomain at a store failure: %v", err)
		}
	})
	t.Run("a foreign Put hash", func(t *testing.T) {
		st := newStoreFake()
		st.badHash = true
		_, err := storeRunErr(t, storeWorker(t, script("hash.test"), st, nil), "hash.test")
		if err == nil || !strings.Contains(err.Error(), "HashOf") {
			t.Fatalf("ProcessDomain at a foreign hash: %v", err)
		}
	})
	t.Run("the ctx cancellation", func(t *testing.T) {
		st := newStoreFake()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		w := storeWorker(t, script("cancel.test"), st, nil)
		_, err := w.ProcessDomain(ctx, queue.HTTPTask{
			ZoneID: 1, DomainID: 1, Name: "cancel.test",
			Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
		})
		if err == nil {
			t.Fatal("ProcessDomain passed at the ctx cancellation")
		}
	})
}

// storeRunErr — a storeRun with an error (piling up t.Fatalf in negative
// tests is not allowed — the error is the expected outcome itself).
func storeRunErr(t *testing.T, w *Worker, name string) (*DomainResult, error) {
	t.Helper()
	return w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 1, DomainID: 1, Name: name,
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})
}

// mustFetch — a scenario without a journal (for one-line builds).
func mustFetch(script map[string]*workerPageScript) FetchFunc {
	f, _ := workerFakeFetch(script)
	return f
}

// storeManualResult — a minimal result with one storable record
// (a manual assembly: the Save check requires no network). stored — whether there was an early
// Put (the puts registry imitates its successful outcome).
func storeManualResult(name string, body string, stored bool) (*DomainResult, content.Hash) {
	h := content.HashOf([]byte(body))
	res := &DomainResult{
		ZoneID: 1, DomainID: 1, Name: name,
		Subdomains: []SubdomainResult{{
			SubdomainID: 0, Label: "", Hostname: name,
			Records: []Record{{
				Variant: model.VariantMain, Scheme: model.SchemeHTTPS,
				SubdomainID: 0, Label: "", FinalURL: "https://" + name + "/",
				ContentHash: &h, ContentStored: stored, StoreContent: true,
			}},
		}},
	}
	if stored {
		res.puts = []workerPut{{hash: h}}
	}
	return res, h
}

// TestStoreSaveErrors — the reconciliation errors: a Delete failure and a ctx cancellation drop
// Save (a delivery without an XACK will return the task), a reference without an early Put —
// a desynchronization of the worker and the store (a loud error, T-044),
// removing an already non-existent reference — a Warn without an error (a re-delivery
// race); a nil store is rejected by the constructor.
func TestStoreSaveErrors(t *testing.T) {
	t.Run("a Delete failure", func(t *testing.T) {
		//		// A scenario with a slice: both checks of the domain download one content,
		//		// the folded record requires one Delete — it is the one that fails.
		body := storePageBody("del")
		script := map[string]*workerPageScript{
			"del.test":         {status: 200, body: body},
			"http://del.test/": {status: 200, body: body},
		}
		st := newStoreFake()
		st.delErr = errors.New("the DB is unavailable")
		res := storeRun(t, storeWorker(t, script, st, nil), "del.test", "")
		err := storeSaver(t, st, &bytes.Buffer{}).Save(context.Background(), res)
		if err == nil || !strings.Contains(err.Error(), "releasing the unstored content reference") {
			t.Fatalf("Save at a Delete failure: %v", err)
		}
	})
	t.Run("the ctx cancellation", func(t *testing.T) {
		res, _ := storeManualResult("cancel-save.test", "a body", false)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := storeSaver(t, newStoreFake(), &bytes.Buffer{}).Save(ctx, res)
		if err != nil {
			t.Fatalf("Save without store calls at the cancellation: %v", err)
		}
	})
	t.Run("the reference is already gone", func(t *testing.T) {
		//		// A re-delivery race: the extra early Put's reference was removed by
		//		// a competitor — a Warn, Save does not fail (the counter is intact at the winning
		//		// delivery).
		res, h := storeManualResult("gone.test", "a body", true)
		res.puts = append(res.puts, workerPut{hash: h}) // an extra Put without a record
		st := &storeFake{blocks: map[content.Hash][]byte{}, refs: map[content.Hash]int{}, delMissing: true}
		var logBuf bytes.Buffer
		if err := storeSaver(t, st, &logBuf).Save(context.Background(), res); err != nil {
			t.Fatalf("Save at a released reference: %v", err)
		}
		if !strings.Contains(logBuf.String(), "the reference is already gone") {
			t.Errorf("the race Warn is not written: %q", logBuf.String())
		}
	})
	t.Run("a record without an early Put", func(t *testing.T) {
		//		// A desynchronization: a record demands a reference nobody put —
		//		// a reference without a counter would drop the accounting on a swap (T-044).
		res, _ := storeManualResult("desync.test", "a body", true)
		res.puts = nil
		err := storeSaver(t, newStoreFake(), &bytes.Buffer{}).Save(context.Background(), res)
		if err == nil || !strings.Contains(err.Error(), "without an early Put") {
			t.Fatalf("Save at a desync: %v", err)
		}
	})
	t.Run("the store is not set", func(t *testing.T) {
		if _, err := NewContentSaver(nil, nil); err == nil {
			t.Fatal("NewContentSaver(nil) passed")
		}
	})
	t.Run("a nil result", func(t *testing.T) {
		if err := storeSaver(t, newStoreFake(), &bytes.Buffer{}).Save(context.Background(), nil); err != nil {
			t.Fatalf("Save(nil): %v", err)
		}
	})
}

// --- the OnResult → store integration in the Run loop ------------------------------------

// TestStoreOnResultIntegration — the whole loop (as in the service build,
// cmd/http-worker): a q:http delivery → ProcessDomain (early Puts) →
// OnResult (Save before the XACK) → Ack; the content in the store, the record's hash is live,
// the extra references are removed by the reconciliation.
func TestStoreOnResultIntegration(t *testing.T) {
	page := storePageBody("integration")
	script := map[string]*workerPageScript{
		"ack.test":         {status: 200, body: page},
		"http://ack.test/": {status: 200, body: page},
	}
	st := newStoreFake()
	w := storeWorker(t, script, st, nil)
	q := &workerFakeQueue{deliveries: []queue.Delivery{workerDelivery(t, queue.HTTPTask{
		TaskID: "task-1", ZoneID: 5, DomainID: 42, Name: "ack.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})}}
	saver := storeSaver(t, st, &bytes.Buffer{})

	var stored *DomainResult
	opts := Options{
		Store: st, // the same fake as the worker's (the early Puts go into it)
		OnResult: func(c context.Context, res *DomainResult) error {
			//			// The service build's order: the content reconciliation before the future
			//			// T-058 write (both — before the XACK).
			if err := saver.Save(c, res); err != nil {
				return err
			}
			stored = res
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, w, opts) }()

	deadline := time.After(5 * time.Second)
	for q.ackedCount() == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("the delivery was not acknowledged within the allotted time")
		case <-time.After(2 * time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run after the cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not finish after the ctx cancellation")
	}

	if stored == nil || stored.RecordCount() != 1 {
		t.Fatalf("OnResult: the result %v", stored)
	}
	rec := &stored.Subdomains[0].Records[0]
	h := content.HashOf([]byte(page))
	if rec.ContentHash == nil || *rec.ContentHash != h || rec.ContentNotStored {
		t.Fatalf("the record after Save: hash=%v not_stored=%v", rec.ContentHash, rec.ContentNotStored)
	}
	//	// Both downloads of the page (https and http) passed as early Puts, the reconciliation
	//	// left one reference to the record.
	_, writes, deletes, refs := st.snapshot()
	if writes != 1 || refs[h] != 1 || deletes != 1 {
		t.Fatalf("the store: %d blocks, %d references, %d Deletes — 1/1/1 expected", writes, refs[h], deletes)
	}
	if got, err := st.Get(context.Background(), h); err != nil || string(got) != page {
		t.Errorf("Get of the stored content: %v", err)
	}
	if q.ackedCount() != 1 {
		t.Errorf("%d acknowledgments, 1 expected", q.ackedCount())
	}
}

// --- an empty body — valid content (T-138) -----------------------------------

// TestStoreEmptyBodyKeepsHash — an empty response body (Content-Length: 0) —
// valid content with a valid hash: empty 200 and 400/404/500 give records
// with a live HashOf of empty bytes, the early Put stores the empty bytes like
// any others, all the empty pages of different domains — one deduplication
// block (§8). Before T-138 ToUTF8 returned nil, the "record body
// unavailable" guard extinguished content_hash at ~4% of the tasks — parkings and empty
// error pages.
func TestStoreEmptyBodyKeepsHash(t *testing.T) {
	fetchScript := map[string]*workerPageScript{
		"empty-200.test": {status: 200},
		"empty-400.test": {status: 400},
		"empty-404.test": {status: 404},
		"empty-500.test": {status: 500},
	}
	st := newStoreFake()
	w := storeWorker(t, fetchScript, st, nil)
	domains := []string{"empty-200.test", "empty-400.test", "empty-404.test", "empty-500.test"}

	//	// The empty bytes' hash is the same for all the empty pages — HashOf(nil) ==
	//	// HashOf([]byte{}) (the T-041 contract), the empty pages' equivalence
	//	// classes are built by the same hash.
	emptyHash := content.HashOf(nil)

	var logBuf bytes.Buffer
	saver := storeSaver(t, st, &logBuf)
	for i, name := range domains {
		res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
			ZoneID: 1, DomainID: int64(i + 1), Name: name,
			Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
		})
		if err != nil {
			t.Fatalf("ProcessDomain(%s): %v", name, err)
		}
		if len(res.Subdomains[0].Records) != 1 {
			t.Fatalf("%s: %d records, 1 expected (both protocols — one empty site)", name, len(res.Subdomains[0].Records))
		}
		rec := &res.Subdomains[0].Records[0]
		if rec.HTTPStatus < 200 || rec.ErrClass != ErrNone {
			t.Fatalf("%s: the scenario is broken — status %d, error %v", name, rec.HTTPStatus, rec.ErrClass)
		}
		if rec.ContentHash == nil || *rec.ContentHash != emptyHash {
			t.Fatalf("%s: the record hash %v, the empty body hash %s expected", name, rec.ContentHash, emptyHash)
		}
		if !rec.ContentStored {
			t.Fatalf("%s: the empty body was not stored by the early Put — an empty body is valid (T-138)", name)
		}
		if err := saver.Save(context.Background(), res); err != nil {
			t.Fatalf("Save(%s): %v", name, err)
		}
	}
	if strings.Contains(logBuf.String(), "unavailable") {
		t.Errorf("the fuse tripped on an empty body:\n%s", logBuf.String())
	}

	//	// Each domain downloaded the empty page twice (both protocols), the record
	//	// is one — the reconciliation removed the extra references; one block for all the empty pages.
	puts, writes, deletes, refs := st.snapshot()
	if puts != 2*len(domains) {
		t.Fatalf("Put: %d calls, %d expected (two fetches per domain)", puts, 2*len(domains))
	}
	if writes != 1 {
		t.Fatalf("%d new blocks, 1 expected (the empty pages — one block)", writes)
	}
	if deletes != len(domains) {
		t.Fatalf("Delete: %d calls, %d expected (an extra fetch of each domain)", deletes, len(domains))
	}
	if refs[emptyHash] != len(domains) {
		t.Errorf("the empty block refcount: %d, %d expected", refs[emptyHash], len(domains))
	}
	if got, err := st.Get(context.Background(), emptyHash); err != nil || len(got) != 0 {
		t.Errorf("Get of the empty block: %v, %d bytes (0 expected)", err, len(got))
	}
}

// TestStoreEmptyBodyCachePath — the final URLs' cache (§5.5 item 6) with an empty
// page: the cache outcome carries the stored empty content as valid
// (T-138), the cache-hit record becomes the class's original (T-055) and
// inherits the early Put reference — content_hash is extinguished neither at the cache record
// nor at the alias neighbor.
func TestStoreEmptyBodyCachePath(t *testing.T) {
	old := storePageBody("old")
	script := map[string]*workerPageScript{
		"example.com": { // both bare checks: a redirect to the empty www
			status: 301, final: "https://www.example.com/",
			steps: []httpfetch.RedirectStep{{URL: "https://example.com/", Status: 301}},
		},
		"https://www.example.com/": {status: 200}, // an empty page
		"http://www.example.com/": { // another site — variant 1
			status: 200, body: old, final: "http://old.example.com/",
			steps: []httpfetch.RedirectStep{{URL: "http://www.example.com/", Status: 301}},
		},
	}
	fetch, log := workerFakeFetch(script)
	st := newStoreFake()
	//	// PerDomain=1 — a deterministic check order (the www-https cache
	//	// hit is stable).
	w := workerWorker(t, fetch, func(o *Options) { o.Store = st; o.PerDomain = 1 })
	res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 1, DomainID: 7, Name: "example.com",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, ""), workerSubMsg(1, "www")},
	})
	if err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}

	//	// A cache hit: the empty www-https page was downloaded once
	//	// (by the bare check), there was no network for www-https.
	if n := log.countHost("www.example.com"); n != 1 {
		t.Fatalf("www fetches: %d, 1 expected (https — from the final URL cache)", n)
	}
	bare, www := &res.Subdomains[0], &res.Subdomains[1]
	if len(bare.Records) != 1 || len(www.Records) != 2 {
		t.Fatalf("records: bare %d, www %d — 1 and 2 expected", len(bare.Records), len(www.Records))
	}
	//	// The class's original (www v0, the cache record) inherits the stored empty
	//	// content (T-138): the early Put passed, the reference waits for the record.
	wwwHome := &www.Records[0]
	if wwwHome.ContentHash == nil || *wwwHome.ContentHash != content.HashOf(nil) {
		t.Fatalf("the cache-hit record: hash=%v, the empty body hash expected", wwwHome.ContentHash)
	}
	if !wwwHome.ContentStored {
		t.Fatal("the cache-hit record: the empty content was not stored by the early Put (T-138)")
	}

	var logBuf bytes.Buffer
	if err := storeSaver(t, st, &logBuf).Save(context.Background(), res); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if strings.Contains(logBuf.String(), "unavailable") {
		t.Errorf("the fuse tripped on the empty body of the cache-hit record:\n%s", logBuf.String())
	}

	//	// Early Puts: the empty page was downloaded twice (both bare checks), old
	//	// — once; the reconciliation removed the extra empty reference. Two blocks — the empty
	//	// page (the bare+www class's original) and the old site; the empty body's hash
	//	// is live at the original, the alias references through alias_subdomain_id.
	puts, writes, deletes, refs := st.snapshot()
	emptyHash, oldHash := content.HashOf(nil), content.HashOf([]byte(old))
	if puts != 3 || writes != 2 {
		t.Fatalf("Put: %d calls, %d new blocks — expected 3 and 2", puts, writes)
	}
	if deletes != 1 {
		t.Fatalf("Delete: %d calls, 1 expected (the second empty-page fetch)", deletes)
	}
	if refs[emptyHash] != 1 || refs[oldHash] != 1 {
		t.Errorf("the refcount: empty %d, old %d — 1 and 1 expected", refs[emptyHash], refs[oldHash])
	}
	if wwwHome.ContentHash == nil || *wwwHome.ContentHash != emptyHash || wwwHome.ContentNotStored {
		t.Errorf("the original after Save: hash=%v not_stored=%v", wwwHome.ContentHash, wwwHome.ContentNotStored)
	}
	if bare.Records[0].ContentHash != nil || bare.Records[0].ContentNotStored {
		t.Errorf("the alias: hash=%v not_stored=%v — the class content is at the original",
			bare.Records[0].ContentHash, bare.Records[0].ContentNotStored)
	}
	if got, err := st.Get(context.Background(), emptyHash); err != nil || len(got) != 0 {
		t.Errorf("Get of the empty block: %v, %d bytes (0 expected)", err, len(got))
	}
}

// --- the slot memory: a structural invariant (T-147) -------------------------------

// TestStoreNoBodyFieldsInResult — the page bodies are not held by the
// slot's memory: SchemeCheck and Record (everything that lives in the task until the end
// of the domain) have no body-byte fields — the content is represented by a hash and a
// storage sign, the bytes lie in the store. A regression invariant: a new
// []byte field in these structures — a mechanical rollback of T-147.
func TestStoreNoBodyFieldsInResult(t *testing.T) {
	for _, tp := range []reflect.Type{
		reflect.TypeOf(SchemeCheck{}),
		reflect.TypeOf(Record{}),
		reflect.TypeOf(workerPage{}),
	} {
		for i := 0; i < tp.NumField(); i++ {
			f := tp.Field(i)
			if f.Type.Kind() == reflect.Slice && f.Type.Elem().Kind() == reflect.Uint8 {
				t.Errorf("%s.%s: a []byte field in the result structure (the bodies are held by the slot memory — T-147)",
					tp.Name(), f.Name)
			}
		}
	}
}

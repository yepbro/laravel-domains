// Rule set reloader tests (T-068): the initial load, replacing the set on
// reload, keeping the previous set on error, subscribing to changes, the
// SIGHUP entry point. The "fake FS" is t.TempDir with rewritable files
// plus the load seam for substitution without a disk.

package rules

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const reloadRules = `
rules:
  - id: a_cookie
    target: cookie_name
    match: "XSRF-TOKEN"
    weight: 0.25
`

const reloadRulesV2 = `
rules:
  - id: a_cookie
    target: cookie_name
    match: "XSRF-TOKEN"
    weight: 0.4
`

func rewriteRules(t *testing.T, dir, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "rules.yaml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// syncBuf — a bytes.Buffer under a lock: the slog handler writes
// synchronously, but the test reads the buffer after the calls (the T-M06
// pattern).
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *syncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// TestReloaderLogging — the reloader's lifecycle log: the initial load, a
// no-op reload, replacing the set (with prev_version) and keeping the
// previous set on error. The logs are observable behavior in operation
// (make logs), captured with a text handler.
func TestReloaderLogging(t *testing.T) {
	var logs syncBuf
	logger := slog.New(slog.NewTextHandler(&logs, nil))

	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, logger)
	if err != nil {
		t.Fatal(err)
	}
	if s := logs.String(); !strings.Contains(s, `msg="rule set loaded"`) ||
		!strings.Contains(s, "rules=1") || !strings.Contains(s, "rules_version=") {
		t.Errorf("the initial-load log is incomplete:\n%s", s)
	}

	if err := r.Reload(); err != nil {
		t.Fatal(err)
	}
	if s := logs.String(); !strings.Contains(s, `msg="rule set unchanged"`) {
		t.Errorf("the no-op reload log is missing:\n%s", s)
	}

	rewriteRules(t, dir, reloadRulesV2)
	if err := r.Reload(); err != nil {
		t.Fatal(err)
	}
	if s := logs.String(); !strings.Contains(s, `msg="rule set reloaded"`) ||
		!strings.Contains(s, "prev_version=") {
		t.Errorf("the set-replacement log is incomplete (prev_version is needed):\n%s", s)
	}

	rewriteRules(t, dir, "rules: [\n")
	if err := r.Reload(); err == nil {
		t.Fatal("a reload error is expected")
	}
	if s := logs.String(); !strings.Contains(s, "rule reload failed") {
		t.Errorf("the reload-error log is missing:\n%s", s)
	}
}

func TestReloaderInitialLoad(t *testing.T) {
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil) // a nil logger is allowed (silent)
	if err != nil {
		t.Fatal(err)
	}
	set := r.Set()
	if set == nil || len(set.Rules) != 1 || set.Rules[0].ID != "a_cookie" {
		t.Fatalf("the initial set was not loaded: %+v", set)
	}
	if r.Set() != set {
		t.Error("Set must return the same snapshot between reloads")
	}
}

func TestReloaderNewError(t *testing.T) {
	if _, err := NewReloader(filepath.Join(t.TempDir(), "нет-такого"), nil); err == nil {
		t.Error("an initial load error is expected")
	}
}

func TestReloaderReloadReplacesSet(t *testing.T) {
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	old := r.Set()

	rewriteRules(t, dir, reloadRulesV2)
	if err := r.Reload(); err != nil {
		t.Fatal(err)
	}
	got := r.Set()
	if got == old {
		t.Fatal("the set was not replaced")
	}
	if got.Rules[0].Weight != 0.4 {
		t.Errorf("after the reload weight = %v, want 0.4", got.Rules[0].Weight)
	}
	if got.Version == old.Version {
		t.Error("a set change must change rules_version")
	}
}

func TestReloaderReloadFailureKeepsOldSet(t *testing.T) {
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	old := r.Set()

	// A broken set: the reload fails, the service runs on the previous one.
	rewriteRules(t, dir, "rules: [\n")
	if err := r.Reload(); err == nil {
		t.Error("a reload error is expected")
	}
	if r.Set() != old {
		t.Error("on a reload error the set must survive")
	}

	// The directory disappeared entirely — the same thing.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := r.Reload(); err == nil {
		t.Error("a reload error is expected (no directory)")
	}
	if r.Set() != old {
		t.Error("on a reload error the set must survive")
	}
}

func TestReloaderHandleSIGHUP(t *testing.T) {
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	old := r.Set()

	// The signal entry point returns no error and does not panic —
	// the error is already logged, the service keeps running.
	rewriteRules(t, dir, reloadRulesV2)
	r.HandleSIGHUP()
	if r.Set() == old {
		t.Error("HandleSIGHUP must reload the set")
	}
	rewriteRules(t, dir, "rules: [\n")
	r.HandleSIGHUP()
	if r.Set().Rules[0].Weight != 0.4 {
		t.Error("an error in HandleSIGHUP must not touch the working set")
	}
}

// TestReloaderReloadClearsCompileCache — the compilation cache does not
// survive a set change: the removed rules' patterns (and the cached
// errors) do not accumulate for the whole lifetime of the process; an
// unchanged set does not touch the cache.
func TestReloaderReloadClearsCompileCache(t *testing.T) {
	const probe = `CACHE-PROBE-V1-` // a unique pattern, not found outside this test
	dir := writeRules(t, `
rules:
  - id: r_html
    target: html
    regex: "`+probe+`"
    weight: 0.3
`)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := compileCache.Load(probe); ok {
		t.Fatalf("the cache is polluted before the test: %s", probe)
	}
	r.Set().Evaluate(Page{HTML: []byte("x " + probe)}) // compile and cache
	if _, ok := compileCache.Load(probe); !ok {
		t.Fatal("the pattern did not get into the compile cache")
	}

	// The same set (a no-op by version) — the cache is not cleared.
	if err := r.Reload(); err != nil {
		t.Fatal(err)
	}
	if _, ok := compileCache.Load(probe); !ok {
		t.Fatal("a no-op reload must not clear the cache")
	}

	rewriteRules(t, dir, `
rules:
  - id: r_html
    target: html
    regex: "OTHER"
    weight: 0.4
`)
	if err := r.Reload(); err != nil {
		t.Fatal(err)
	}
	if _, ok := compileCache.Load(probe); ok {
		t.Error("a rules_version change must clear the compile cache")
	}
}

func TestReloaderReloadSameSetIsNoop(t *testing.T) {
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	old := r.Set()
	sub, cancel := r.Subscribe()
	defer cancel()

	if err := r.Reload(); err != nil {
		t.Fatal(err)
	}
	if r.Set() != old {
		t.Error("re-reading the same set is a no-op, the snapshot need not change")
	}
	select {
	case <-sub:
		t.Error("no notification is needed when rules_version did not change")
	default:
	}
}

func TestReloaderSubscribe(t *testing.T) {
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}

	sub, cancel := r.Subscribe()
	defer cancel()

	rewriteRules(t, dir, reloadRulesV2)
	if err := r.Reload(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-sub:
		if got != r.Set() {
			t.Error("the subscriber must receive the new snapshot")
		}
	case <-time.After(time.Second):
		t.Fatal("the set-change notification did not arrive")
	}
}

func TestReloaderSubscribeCancel(t *testing.T) {
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}

	sub, cancel := r.Subscribe()
	cancel()

	rewriteRules(t, dir, reloadRulesV2)
	if err := r.Reload(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sub:
		t.Error("after cancel no notifications arrive")
	default:
	}
}

func TestReloaderSubscribeSlowConsumerGetsLatest(t *testing.T) {
	// The subscriber does not read the channel for two reloads in a row:
	// Reload does not block, the freshest notification stays in the channel.
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	sub, _ := r.Subscribe()

	rewriteRules(t, dir, reloadRulesV2)
	if err := r.Reload(); err != nil {
		t.Fatal(err)
	}
	const third = `
rules:
  - id: a_cookie
    target: cookie_name
    match: "XSRF-TOKEN"
    weight: 0.6
`
	rewriteRules(t, dir, third)
	if err := r.Reload(); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-sub:
		if got.Rules[0].Weight != 0.6 {
			t.Errorf("there must be a fresh notification in the channel, weight = %v", got.Rules[0].Weight)
		}
	default:
		t.Error("the notification is lost")
	}
}

func TestReloaderLoadSeam(t *testing.T) {
	// The load seam: substituting the set source without a file system; a
	// seam error behaves like a load error — the set is not changed.
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	old := r.Set()

	boom := errors.New("a disk failure")
	r.load = func(string) (*Set, error) { return nil, boom }
	if err := r.Reload(); !errors.Is(err, boom) {
		t.Errorf("Reload must return the seam error, got %v", err)
	}
	if r.Set() != old {
		t.Error("a seam error must not change the set")
	}
}

func TestReloaderConcurrent(t *testing.T) {
	// Set readers in parallel with reloads: the snapshots are consistent
	// (also run under -race: a race between the set replacement and reads).
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				if s := r.Set(); s == nil || len(s.Rules) != 1 {
					t.Error("an inconsistent snapshot")
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 10 {
			_ = r.Reload()
		}
	}()
	wg.Wait()
}

// Unit tests of the domain content point drain on a find removal by
// re-analysis (T-187): the aggregateDomain signal (the laravel_domains
// row existed and was removed by a delivery without fresh) → after the
// delivery commits, the worker calls the DrainDomain seam with the
// domain key and the retention window — the residual loss class
// behind the T-185 drain cursor (docs/content.md) is closed. The
// control scenario, the event bounds (fresh, no transition, a
// remaining find) and fault tolerance (a drain failure does not fail
// the delivery). The seam is replaced by a recorder — the DrainDomain
// content semantics is covered by the internal/content tests
// (retentiondomain_test.go).
package analyzer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"crawler/internal/content"
)

// anDrainCall is a recorded drain seam call (T-187).
type anDrainCall struct {
	pool      content.RetentionDB
	retention time.Duration
	zone      int16
	domain    int64
}

// anDrainRecorder replaces Options.DrainDomain: it writes calls into
// the fixture journal (the anEvDrain event — the order against commit)
// and answers with the prepared stats/error.
type anDrainRecorder struct {
	j     *anJournal
	stats content.RetentionStats
	err   error

	mu    sync.Mutex
	calls []anDrainCall
}

func (r *anDrainRecorder) drain(ctx context.Context, pool content.RetentionDB,
	retention time.Duration, zoneID int16, domainID int64) (content.RetentionStats, error) {
	r.mu.Lock()
	r.calls = append(r.calls, anDrainCall{pool: pool, retention: retention, zone: zoneID, domain: domainID})
	r.mu.Unlock()
	r.j.record(anEvDrain)
	return r.stats, r.err
}

func (r *anDrainRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *anDrainRecorder) last() anDrainCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[len(r.calls)-1]
}

// TestUnfoundDrain_ControlScenario — re-analysis (fresh=false) removed
// the find: the aggregation signal becomes a domain point drain —
// the seam is called AFTER the transaction commits (the findings and
// the aggregate are already in the DB) with the domain key and the worker's retention window.
func TestUnfoundDrain_ControlScenario(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	// A past episode found signs — the laravel_domains row exists; the
	// delivery analyzes a page without signs (fresh=false — re-analysis).
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), nil)
	f.pool.domains[anDomainKey{anKey.zone, anKey.domain}] = &anDomain{
		score: 0.9, foundOnMask: mustMask(t, 1), rulesVersion: 999, updatedAt: time.Now(),
	}
	rec := &anDrainRecorder{j: f.pool.j, stats: content.RetentionStats{Rows: 3, Refs: 2}}
	f.w.opt.DrainDomain = rec.drain
	f.w.opt.Retention = 6 * time.Hour

	if wrote, err := f.w.ProcessResult(context.Background(), anTask); err != nil || !wrote {
		t.Fatalf("ProcessResult = %v, %v; want success with a write", wrote, err)
	}
	if _, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain); ok {
		t.Fatal("the laravel_domains row was not removed — no event")
	}
	if rec.count() != 1 {
		t.Fatalf("drain calls = %d, want 1", rec.count())
	}
	call := rec.last()
	if call.zone != anKey.zone || call.domain != anKey.domain {
		t.Errorf("the drain key = %d/%d, want %d/%d", call.zone, call.domain, anKey.zone, anKey.domain)
	}
	if call.retention != 6*time.Hour {
		t.Errorf("the drain window = %v, want 6h (the worker's retention)", call.retention)
	}
	if p, ok := call.pool.(*anFakePool); !ok || p != f.pool {
		t.Error("the drain was not passed the worker's pool")
	}
	// The order: the drain strictly after the commit — the event has
	// already happened, a drain failure must not fail the delivery (TestUnfoundDrain_Error).
	if f.pool.j.indexOf(anEvCommit) > f.pool.j.indexOf(anEvDrain) {
		t.Fatalf("the drain before the commit: %v", f.pool.j.snapshot())
	}
}

// TestUnfoundDrain_EventBounds — the drain only on a transition without fresh:
// a fresh find loss rewrites the rows ahead of the cursor (T-058),
// no transition (the row did not exist — DELETE 0) and a remaining
// find need no drain.
func TestUnfoundDrain_EventBounds(t *testing.T) {
	cases := []struct {
		name      string
		fresh     bool
		laravel   bool // whether to seed the laravel_domains row (was there a transition)
		confident bool // the delivery finds confident signs
		want      int
	}{
		{"a fresh find loss — ahead of the cursor", true, true, false, 0},
		{"no transition (there was no row)", false, false, false, 0},
		{"the find remains (confident signs)", false, true, true, 0},
		{"a repeat empty re-analysis after the removal", false, false, false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
			rec := &anDrainRecorder{j: f.pool.j}
			f.w.opt.DrainDomain, f.w.opt.Retention = rec.drain, 6*time.Hour

			if tc.confident {
				hash := anTestHash(7)
				f.store.blocks[hash] = anPageHTML()
				f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])
			} else {
				f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), nil)
			}
			if tc.laravel {
				f.pool.domains[anDomainKey{anKey.zone, anKey.domain}] = &anDomain{
					score: 0.9, foundOnMask: mustMask(t, 1), rulesVersion: 999, updatedAt: time.Now(),
				}
			}

			task := anTask
			task.Fresh = tc.fresh
			if _, err := f.w.ProcessResult(context.Background(), task); err != nil {
				t.Fatalf("ProcessResult: %v", err)
			}
			if rec.count() != tc.want {
				t.Fatalf("drain calls = %d, want %d (the event = a transition without fresh)", rec.count(), tc.want)
			}
		})
	}
}

// TestUnfoundDrain_Error — a drain failure does not fail the delivery:
// the findings, the aggregate and the row removal are committed,
// ProcessResult answers success (the XACK will happen; the domain's
// rows stay behind the cursor — the pre-T-187 class, a Warn in the worker log).
func TestUnfoundDrain_Error(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6, Retention: 6 * time.Hour})
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), nil)
	f.pool.domains[anDomainKey{anKey.zone, anKey.domain}] = &anDomain{
		score: 0.9, foundOnMask: mustMask(t, 1), rulesVersion: 999, updatedAt: time.Now(),
	}
	rec := &anDrainRecorder{j: f.pool.j, err: errors.New("drain boom")}
	f.w.opt.DrainDomain = rec.drain

	wrote, err := f.w.ProcessResult(context.Background(), anTask)
	if err != nil || !wrote {
		t.Fatalf("ProcessResult = %v, %v; the delivery must succeed (the analysis is committed)", wrote, err)
	}
	if rec.count() != 1 {
		t.Fatalf("drain calls = %d, want 1 (the attempt was made)", rec.count())
	}
	if _, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain); ok {
		t.Fatal("the laravel_domains row was not removed on the drain failure")
	}
}

// TestUnfoundDrain_Disabled — the drain disabled (retention = 0 — content
// is kept forever; the seam is not set — an isolated build): the event
// happens, there is no call, the delivery does not fail.
func TestUnfoundDrain_Disabled(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts Options
	}{
		{"retention off", Options{MinScore: 0.6, Retention: 0, DrainDomain: (&anDrainRecorder{}).drain}},
		{"the seam is not set", Options{MinScore: 0.6, Retention: 6 * time.Hour}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newANFixture(t, anSet(1001), tc.opts)
			f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), nil)
			f.pool.domains[anDomainKey{anKey.zone, anKey.domain}] = &anDomain{
				score: 0.9, foundOnMask: mustMask(t, 1), rulesVersion: 999, updatedAt: time.Now(),
			}
			if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
				t.Fatalf("ProcessResult: %v", err)
			}
			if _, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain); ok {
				t.Fatal("the laravel_domains row was not removed")
			}
		})
	}
}

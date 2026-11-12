// Free-space control of the content directory (spec 8; tasks T-046, T-080).
//
// Spec 8: "Disk overflow: services pause content writing and signal it
// with an alert; the threshold is configurable". The mechanics:
//
//   - before writing a new block (a Put after a deduplication miss) and
//     periodically (DiskWatchLoop) a statfs of the content directory is taken;
//   - when the free-space share is below the content.disk_free_percent threshold —
//     Put returns ErrStoreFull: the worker does not store the content but still
//     writes the result (the spec 8 contract — a pause of content writing, not
//     of results); reads and dedup references continue — no bytes are
//     consumed;
//   - the alert signal is a typed StoreFullEvent event delivered to the
//     PackOptions.OnStoreFull hook (a counter for T-080) and a Warn in the
//     DiskWatchLoop log; the alert itself (metrics, paging) is T-080 and is not
//     implemented here;
//   - ENOSPC on a physical pack write remains the last line of
//     defense (wrapPackWriteErr, T-042) and also fires an event.
//
// There is no separate counter of used space (just as in compaction,
// T-045): the used space is the sum of the pack-file sizes, the free space
// comes from a live statfs, so after rotation and removal of old packs by
// compaction the threshold is honestly recomputed and writing resumes on
// its own.
package content

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"syscall"
	"time"
)

// DefaultDiskWatchInterval — the interval of DiskWatchLoop periodic checks.
// The spec does not set the pace: a statfs is one cheap syscall,
// 30 seconds give the T-080 alert minute-level freshness without noticeable
// load.
const DefaultDiskWatchInterval = 30 * time.Second

// Sources of a StoreFullEvent event (Source).
const (
	// StoreFullPut — Put ran into the free-space threshold: the new block
	// was not appended, the data is not corrupted.
	StoreFullPut = "put"
	// StoreFullENOSPC — the physical pack write ended with an
	// ENOSPC error (the last line of defense after the threshold).
	StoreFullENOSPC = "enospc"
	// StoreFullWatch — the periodic DiskWatchLoop check (entering the
	// pause and the recovery).
	StoreFullWatch = "watch"
)

// SpaceStat — a snapshot of the directory's file system space, bytes.
type SpaceStat struct {
	// Avail — available to processes without the root reserve (statfs.Bavail):
	// the crawler services run unprivileged (spec 10.1).
	Avail uint64
	// Total — the volume capacity (statfs.Blocks).
	Total uint64
}

// SpaceChecker — a source of file-system space information; extracted as
// an interface so unit tests can stub statfs without a real volume.
type SpaceChecker interface {
	// Space returns a snapshot of the dir directory's space.
	Space(dir string) (SpaceStat, error)
}

// OSStatfs — a SpaceChecker over syscall.Statfs (the real file
// system of the content directory).
type OSStatfs struct{}

// compile-time: OSStatfs is a SpaceChecker.
var _ SpaceChecker = OSStatfs{}

// Space takes a statfs of the dir directory.
func (OSStatfs) Space(dir string) (SpaceStat, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return SpaceStat{}, err
	}
	bs := uint64(st.Bsize)
	return SpaceStat{
		Avail: st.Bavail * bs,
		Total: st.Blocks * bs,
	}, nil
}

// StoreFullEvent — a typed event "content writing stopped: the free space
// of the directory is below the threshold" (spec 8). The argument of the
// PackOptions.OnStoreFull alert hook and of the DiskWatchLoop logs; the
// counter/metric is T-080.
type StoreFullEvent struct {
	// Space — a snapshot of the space at the moment of the event (for an
	// ENOSPC event — best-effort after the failed write).
	Space SpaceStat
	// Source — who detected it: StoreFullPut / StoreFullENOSPC /
	// StoreFullWatch.
	Source string
	// Recovered — the space returned above the threshold, the pause is lifted
	// (StoreFullWatch only: the "full → free" transition).
	Recovered bool
}

// OnStoreFullFunc — the T-080 alert hook: called for every Put
// stopped by the threshold (Source = put — an event counter), for ENOSPC,
// and on transitions of the periodic check. The hook must be fast and
// must not panic: it is called under Put.
type OnStoreFullFunc func(StoreFullEvent)

// spaceChecker — the store's statfs source (nil — the real OS; the package
// tests stub the stat field before first use).
func (s *PackStore) spaceChecker() SpaceChecker {
	if s.stat != nil {
		return s.stat
	}
	return OSStatfs{}
}

// diskFull — a snapshot of the content directory's space and the "free space
// below the threshold" verdict. A shared source for the Put pause (T-046) and the
// DiskWatchLoop periodic check: one formula — non-rejecting semantics. Total = 0 is
// an exotic FS without capacity: a share-based pause is undefinable, we report
// "space available" (a false pause would stop writing to a working directory).
func (s *PackStore) diskFull() (sp SpaceStat, full bool, err error) {
	if s.opts.DiskFreePercent <= 0 {
		return SpaceStat{}, false, nil // the control is disabled (the config always sets it)
	}
	sp, err = s.spaceChecker().Space(s.opts.Dir)
	if err != nil {
		return SpaceStat{}, false, fmt.Errorf("content: free space %s: %w", s.opts.Dir, err)
	}
	if sp.Total == 0 {
		return sp, false, nil
	}
	full = 100*float64(sp.Avail)/float64(sp.Total) < s.opts.DiskFreePercent
	return sp, full, nil
}

// pauseIfDiskFull — the content write pause (spec 8): space below the
// threshold — ErrStoreFull and an event to the alert hook. Put calls it only
// before appending a new block: dedup references and byte reads consume nothing
// and keep working.
func (s *PackStore) pauseIfDiskFull() error {
	sp, full, err := s.diskFull()
	if err != nil {
		return err
	}
	if !full {
		return nil
	}
	s.fireStoreFull(StoreFullEvent{Space: sp, Source: StoreFullPut})
	return fmt.Errorf("%w: %s free of %s (%.1f%%) — below the %.1f%% threshold (content.disk_free_percent)",
		ErrStoreFull, humanBytes(sp.Avail), humanBytes(sp.Total),
		100*float64(sp.Avail)/float64(sp.Total), s.opts.DiskFreePercent)
}

// fireStoreFull sends the event to the alert hook if it is set
// (nil — no hook set, T-056 will wire it up at the service boundary).
func (s *PackStore) fireStoreFull(ev StoreFullEvent) {
	if s.opts.OnStoreFull != nil {
		s.opts.OnStoreFull(ev)
	}
}

// enospcEvent — the event after an ENOSPC: a best-effort space snapshot (the
// write itself just failed, statfs may not yet reflect
// the reclamation — the event is informative without it).
func (s *PackStore) enospcEvent() StoreFullEvent {
	ev := StoreFullEvent{Source: StoreFullENOSPC}
	if sp, err := s.spaceChecker().Space(s.opts.Dir); err == nil {
		ev.Space = sp
	}
	return ev
}

// DiskWatchOptions — DiskWatchLoop parameters; a zero Interval means
// DefaultDiskWatchInterval, a nil Log suppresses records.
type DiskWatchOptions struct {
	Interval time.Duration
	Log      *slog.Logger
}

// DiskWatchLoop — the periodic free-space check of the content
// directory (spec 8): a Warn in the log and a StoreFullEvent event when entering
// the pause (even if there are no writes — the alert is visible to the operator),
// an Info and a Recovered event on recovery. The signal is edge-triggered (on
// transitions), not on every tick: a week-long Warn storm of a full disk is
// not information. A statfs error is a Warn without a state change: the next
// tick retries. Returning nil on ctx cancellation is the OnRun contract of the
// framework (the CompactLoop style).
func DiskWatchLoop(ctx context.Context, st *PackStore, opts DiskWatchOptions) error {
	if st == nil {
		return errors.New("content: disk check: the store is not set")
	}
	interval := opts.Interval
	if interval <= 0 {
		interval = DefaultDiskWatchInterval
	}
	log := opts.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	full := false
	check := func() {
		sp, isFull, err := st.diskFull()
		switch {
		case err != nil:
			if ctx.Err() == nil {
				log.Warn("content directory free space check", "err", err)
			}
			return
		case isFull && !full:
			full = true
			st.fireStoreFull(StoreFullEvent{Space: sp, Source: StoreFullWatch})
			log.Warn("content writing paused: free space below the threshold",
				"avail", humanBytes(sp.Avail), "total", humanBytes(sp.Total),
				"threshold_percent", st.opts.DiskFreePercent)
		case !isFull && full:
			full = false
			st.fireStoreFull(StoreFullEvent{Space: sp, Source: StoreFullWatch, Recovered: true})
			log.Info("content writing resumed: free space is back above the threshold",
				"avail", humanBytes(sp.Avail), "total", humanBytes(sp.Total))
		}
	}

	check() // the first pass right away: drain what accumulated during idle
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			check()
		}
	}
}

// humanBytes — a human-readable size for error messages and logs
// (a local analog of config.ByteSize.String: the content package does not
// pull in the configuration just for formatting).
func humanBytes(n uint64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1fGiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fKiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

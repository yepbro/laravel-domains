# T-275: block_trackers — switching the probes to async XHR (a consequence of the moli upstream fixes ①/④)

## Context

The `TestEnginesLiveCDPSlice/*/block_trackers` test
(`internal/render/block_live_test.go`, `blockLiveTrackerDomains`) checks
the T-214 domain-level tracker blocking with page probes. All probes (the
clarity.ms/mc.yandex.ru controls, the path-trap
`/js/google-analytics.com.js`) run via **synchronous XHR**
(`x.open('GET', url, false)` — the probe helper, block_live_test.go:~130–146).

The upstream moli changed the synchronous XHR behavior under interception:

- ① PR [lexmount/moli#2704](https://github.com/lexmount/moli/pull/2704) —
  interception is scoped by a set of classes (heterogeneous typed patterns no
  longer crash sync XHR);
- ④ issue
  [lexmount/moli#2728](https://github.com/lexmount/moli/issues/2728) + PR
  [lexmount/moli#2729](https://github.com/lexmount/moli/pull/2729) — sync XHR
  under interception goes **the regular network path**: pausing is
  architecturally impossible (an owner-loop deadlock), the failure factory
  was removed. As a consequence, **deliberate sync-XHR blocking is
  unavailable on moli** (a documented divergence from Chromium); async XHR
  pauses and blocks fully.

Before the fixes (v1.1.21/1.1.23) the test failed earlier on moli — the
control sync probes died from the “interception is not supported” ban
(t.Fatalf on a control). With the fixes the control passes, the path-trap
passes (the request goes to the network → 200), but the “trackers were cut
off” asserts (0 hits, the `error` attribute) become false: the tracker
sync probes go to the network and get 200 — the engine cannot block them.

## Root cause

The test probes are synchronous, while real web analytics (clarity.ms,
mc.yandex.ru, google-analytics) use async XHR/`fetch()`/Image/beacon. The
test checked an engine-specific behavior (the sync-XHR ban) rather than
the T-214 contract.

## Requirements

1. Switch the `blockLiveTrackerDomains` probes (the clarity/mc controls, the
   path-trap, the tracker checks) to **async XHR** with awaiting the result
   (page attributes, in the style of the existing probe mechanism). The
   synchronous probe variant may be kept as an explicitly engine-branched
   case: on moli — “went to the network, 200, not blocked” (the documented
   divergence #2728), on chromium — “blocked”; the expectations branch by
   engine, as in the moli smoke contracts.
2. The test invariants after the rework:
   - the control probes (no blocking) get through on both engines;
   - real tracker domains (async) are blocked on both engines
     (0 hits, `error`);
   - the path-trap (a tracker domain in the path, a foreign host) passes on
     both;
   - the sync-XHR probe: chromium — blocked, moli — goes to the network
     (the #2728 divergence, a comment in the test with a reference).
3. A run against the integration build of moli (①+④; ② already in main)
   and against chromium — both green (with the engine branching for the sync
   probe).

## Acceptance criteria

- `TestEnginesLiveCDPSlice/moli/block_trackers` green on the moli
  integration build (the #2704+#2729 branches) and on the `chromium` line
  of the slice.
- The unit invariants of the probe mechanism are not weakened; `make
  test`/`make lint` green (only `internal/render` affected).
- A comment in the test records the sync-XHR divergence (#2728) and the
  reason for switching the probes to async.

## References

- moli-analysis: `/home/brooks/projects/opensources/moli-analysis/04-path-trap-continue-request.md`
- Upstream: #2728 (issue), #2729 (PR ④), #2704 (PR ①), #2696 (②, merged).
- Integration run 2026-11-11 (the T-272→④ session): the slice 5/6 on ①+②,
  block_trackers — the only red one, the “sync probes to the network”
  profile.

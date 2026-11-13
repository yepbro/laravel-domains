# T-276: block_trackers — the sync probe on the exact-URL semantics of the moli #2729 merge

## Context

T-275 switched the `blockLiveTrackerDomains` probes to async XHR and left
the sync probe (`probeSync('sync-xhr', 'http://c.clarity.ms:…/clarity-sync-probe')`)
engine-branched under the semantics of OUR version of PR
[lexmount/moli#2729](https://github.com/lexmount/moli/pull/2729):
“moli: sync-XHR goes out to the network bypassing the pause (#2728),
200 + counter ≥ 1” (block_live_test.go:~495–500).

The maintainer `ldm0` reworked ④ at the head of the PR branch (commits
`f3b01c908`, `8ff18ca02`, `53ff25d05`, 12.11.2026) — the semantics changed:

- the protocol computes `synchronous_xhr_interception_patterns()` — the exact
  list of URL patterns relevant to sync XHR (matches the XHR class or a
  type-less request; CDP-auth ⇒ `["*"]`) — and passes it down with the
  `SetFetchSubresourceInterception` command;
- the renderer (`prepare_synchronous_xhr_request`) checks the URL of the sync
  request **and every redirect target** against these patterns: a match →
  a rejection (“Synchronous XMLHttpRequest interception is not supported”),
  no match → the regular network path.

Bottom line: **deliberate blocking of a sync XHR with a matching URL now
works on moli** — parity with Chromium for the tracker probes. Our
engine-branched case (“moli: 200 to the network”) is false under the new
semantics: the c.clarity.ms sync probe matches the tracker pattern → it will
be blocked on both engines. The divergence from Chromium remains only for “a
matching URL that the client would have continued” (path-trap-like) — that is
inherent while pausing is impossible.

## Requirements

1. After #2729 is merged into moli main: verify the final semantics against
   the actual commits (the maintainer may keep polishing; the analysis base
   is `8ff18ca02`).
2. Collapse the sync probe's engine branching: both engines expect the block
   (the `error` attribute, the `claritySync == 0` counter); the control (no
   block) — `loaded 200` on both.
3. Update the test comments: the #2728 divergence evolved from “sync XHR
   always goes to the network” to “exact-URL enforcement” (references to the
   #2729 commits); describe the path-trap asymmetry for sync explicitly.
4. Run: an integration build of moli with #2729 merged (or a release) — the
   moli row of the snapshot is green; the chromium row is green unchanged.

## Acceptance criteria

- `TestEnginesLiveCDPSlice/{moli,chromium}/block_trackers` green with the
  unified sync-probe expectations; `make test`/`make lint` green.
- The comments in the test match the final upstream semantics.

## References

- PR #2729 + the maintainer's commits: `f3b01c908` (response-pauses),
  `8ff18ca02` (exact-URL enforcement), `53ff25d05` (an offline test by the
  request identity).
- T-275 (the previous iteration of the probes), moli-analysis/04.

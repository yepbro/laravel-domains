# T-281: Fix crawlerctl domain on production (“domain_info: empty response”)

- **Priority:** P2
- **Status:** [new]

## Context and symptoms

- On production `crawlerctl domain <name> [--json]` answers
  “domain_info: empty response” for any domain — including the known-alive
  ya.ru. Discovered 13.11.2026 during the temp_error investigation (the
  spot checks of the domains had to go through psql + curl).
- The other crawlerctl read commands work on production (`status`,
  `zones`) — it is exactly the domain_info path that breaks: the candidate
  causes — the RPC handle/socket/response format/permissions, or a call to
  a service that does not exist on production (unit/port).

## Root cause

Not established (in the task: reproduce and localize).

## Requirements

1. Reproduce on production and on the dev bench; localize the failure
   point (the client ↔ the service, which service owns domain_info).
2. Fix it; cover both sides with a test (the client's handling of an empty
   response does not mask the error).
3. Run the other crawlerctl read commands on production (subdomains,
   zones, the reanalyze dry modes, if any) — make sure the defect class is
   a single one and is not smeared across the commands.

## Acceptance criteria

- `crawlerctl domain ya.ru --json` on production returns data; for a
  non-existent domain — a meaningful response (not “empty”).
- `make test` + `make lint` green.

## References

- Spec §5.11 (crawlerctl domain)
- The temp_error investigation of 13.11.2026 (the discovery site)

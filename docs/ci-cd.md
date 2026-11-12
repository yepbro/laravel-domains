# CI/CD: production deployment via GitHub Actions

Continuous deployment to the production server (a supplement to T-135): after
green CI on `production`, the same commit is built into static binaries and
installed on the server by the systemd deployment ([`deploy.md`](deploy.md)).
`master` is the development branch (ci without deploy); only what is merged
into `production` goes to production. The pipeline does not touch the server
configuration (`/etc/crawler`, units, nginx, certificates) — only
`/opt/crawler/bin`.

## Pipeline

```
push to production ──▶ ci (tests, lint, integration)
                       │ success
                       ▼
                   deploy (workflow_run)
                       │ build (setup-go + make web-css + go build)
                       │ upload (scp with the deploy key)
                       │ install (deploy/deploy-install.sh: backup → replace → restart)
                       ▼
                   healthcheck (deploy/deploy-healthcheck.sh)
                       │ install failure → rollback to /opt/crawler/bin.prev (trap)
                       │ healthcheck failure → rollback (deploy/deploy-rollback.sh)
                       ▼
                   production (10 units, healthz 9100…9108)
```

| File | Purpose |
|---|---|
| [`.github/workflows/deploy.yml`](../.github/workflows/deploy.yml) | workflow: runs after `ci` succeeds on `production` (or manually), build, deploy, rollback on healthcheck failure |
| [`deploy/deploy-install.sh`](../deploy/deploy-install.sh) | server side: atomic replacement of `/opt/crawler/bin` with rollback |
| [`deploy/deploy-healthcheck.sh`](../deploy/deploy-healthcheck.sh) | server side: 10 units active + healthz + crawlerctl |
| [`deploy/deploy-rollback.sh`](../deploy/deploy-rollback.sh) | server side: restoring `bin.prev` and restarting the set (T-137) |
| [`deploy/telegram-notify.sh`](../deploy/telegram-notify.sh) | Telegram notifications about pipeline events (§10.4) |

Principles:

- **exactly the SHA that was tested gets deployed** — `workflow_run` passes
  the `head_sha` of the passed ci run, and the checkout uses it;
- **binaries are deployed by the pipeline, configuration by the operator**:
  `/etc/crawler/*`, `/etc/systemd/system/crawler-*`, nginx and certificates
  are changed manually per [`deploy.md`](deploy.md) (rare operations);
- **rollback is automatic at any step after the set is stopped** (spec
  §10.4, T-137): the previous binaries remain in `/opt/crawler/bin.prev`; an
  install error after a successful stop restores them and restarts the set
  (trap in `deploy-install.sh`; if stopping the set itself fails — the
  binaries are untouched, but the unit state needs checking — the
  notification is "unknown"; a missing archive — the server is untouched; a
  backup preparation failure — the binaries are untouched, the set was
  stopped and restarted, and on a start failure the notification is
  "unknown"); a healthcheck failure — a separate pipeline rollback step
  (`deploy/deploy-rollback.sh`: copy the backup → stop → atomic replacement
  → start with a start-limit reset → a light unit check); before
  diagnostics — restoring a working version matters more than collecting
  logs (the new version's logs remain in journalctl). Edge cases: no
  `bin.prev` (the first deploy) — there is nothing to roll back to, the set
  is untouched and stays on the new version; the rollback itself failed or
  the set did not fully come up — the notification states this explicitly
  and calls the operator; ssh connection loss — the state is unknown.
  Rollback restores only the binaries: the DB schema stays at the new
  release's version, so a release that applied a migration is not restored
  by automatic rollback — the N-1 set will fail at startup and the
  notification will call the operator (a down-migration is manual and
  deliberate, [`operations.md`](operations.md) §4.1);
- **Chromium is not reinstalled**: the version is pinned
  (`CHROMEDP_HEADLESS_IMAGE` in `.env.example`); updating is a manual step
  in [`deploy.md` §4.3](deploy.md).

## Repository secrets (Settings → Secrets → Actions)

| Secret | Contents |
|---|---|
| `DEPLOY_SSH_KEY` | the private ed25519 deploy key (the public half — in the server's `/root/.ssh/authorized_keys`) |
| `DEPLOY_SSH_KNOWN_HOSTS` | the `ssh-keyscan -t ed25519 <host>` output — MITM protection in CI |
| `TELEGRAM_BOT_TOKEN` | the Telegram bot token for pipeline notifications (§10.4) |
| `TELEGRAM_CHAT_ID` | the id of the dedicated group the bot sends notifications to |

The server address is given in the workflow openly (not a secret). Secret
values are not printed to run logs and do not get into notifications.

## Telegram notifications (spec §10.4)

On pipeline events the GitHub Actions bot sends notifications to a dedicated
group — one call to [`deploy/telegram-notify.sh`](../deploy/telegram-notify.sh)
as the last step of the workflow (`ci.yml` — on failure, `deploy.yml` — on
any outcome). Service runtime alerts go to the same group via the spec §5.9
mechanism (the monitor alert loop; since T-162 their delivery to the
operator is handled by this script too — in the `--message "🚨 …"` mode,
invoked via exec from monitor, secrets from the unit's EnvironmentFile;
setup and semantics — [`operations.md`](operations.md) §4.3); the CI/CD
calls did not change.

Events and sending conditions:

| Event | Condition |
|---|---|
| 🟥 ❌ Code error | a lint or build (images/binaries) step failed in `ci` |
| 🛑 Test failure | unit tests, rules tests, integration tests or the race detector (`-race`, monitor) failed in `ci` |
| ✅ Deploy succeeded | the server install and the healthcheck passed |
| ⛔️ Deploy failed | any `deploy` step failed, including the healthcheck; a cancelled run (including by timeout) counts too |
| ⚠️ CI pipeline error | fallback: the other ci steps failed (diagnostics, environment preparation) |

The message format is plain text: an event marker at the start (the map
above — the customer's choice based on live Telegram rendering), the event
type, the workflow and step, the deploy version (`git describe` from the
build step) and the rollback fact for deploys, the offending commit (short
SHA + the first subject line; in PR runs — the head commit of the PR
branch), and a link to the run. The rollback fact reflects the actual step
outcomes: an install failure before the set is stopped — no rollback line
(the archive is missing — the server is untouched; a backup preparation
failure — the binaries are untouched, the set was restarted) or "unknown,
check the server" (the set did not stop; the stopped set did not come up);
a failure inside the install — by the trap rollback result: "completed to
the previous version (bin.prev)", "completed, but not all units are active
— operator required" or "failed — the server state requires operator
review"; a healthcheck failure → the rollback step result
(`deploy-rollback.sh`): "completed", "completed, but not all units are
active — operator required", "not performed — there is no previous version
(bin.prev is missing, the first deploy); the new version remains on the
server" or "failed — the server state requires operator review"; an
interrupted server session (ssh connection loss, run cancellation) →
"unknown, check the server"; a failure before the install begins (build,
scp) → no rollback line.

Principles:

- **one event — one message**, no intermediate statuses: the notification
  step runs once when the run finishes;
- **a send failure does not redden the run**: the step runs with
  `continue-on-error`; the script's own failure is recorded only in the log;
- **auxiliary and manual runs stay silent**: `mutation.yml` sends no
  notifications, and neither do manual `ci` runs (`workflow_dispatch`); a
  manual `deploy` run is a regular deploy and is notified;
- **skipped when secrets are absent** (a PR from a fork, a repository
  without configured secrets): the script prints the reason to the log and
  exits 0; PRs from the repository's own branches do see the secrets — the
  bot token is trusted to collaborators with push access (the same merge
  condition);
- an exception to the §10.4 table: a failure of the other ci steps
  (diagnostics, environment preparation) is sent with the fallback type
  "CI pipeline error" — staying silent about a red run is worse.

## Manual runs and operations

```bash
# run a deploy of the current production without waiting for ci (GitHub → Actions → deploy → Run workflow)
gh workflow run deploy

# on the server: the previous binaries (the last successful deploy)
ls /opt/crawler/bin.prev

# manual rollback — the same script the pipeline runs on a healthcheck
# failure; the script is not stored on the server — pipe it via stdin from
# a repository checkout (or copy it to the server first)
ssh root@155.212.174.88 'bash -s' -- < deploy/deploy-rollback.sh
```

The first deploy of a new server — per [`operations.md` §3](operations.md)
(manual); CI/CD is hooked up after the secrets and the key are set up.

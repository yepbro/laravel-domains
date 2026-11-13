# Domain Crawler

[![CI](https://github.com/yepbro/laravel-domains/actions/workflows/ci.yml/badge.svg)](https://github.com/yepbro/laravel-domains/actions/workflows/ci.yml)
[![Go](https://img.shields.io/github/go-mod/go-version/yepbro/laravel-domains)](https://github.com/yepbro/laravel-domains/blob/master/go.mod)

A service for bulk domain checking and discovery of Laravel sites.
Specifications: [`tasks/TZ_domain_crawler.md`](tasks/TZ_domain_crawler.md),
[`tasks/TZ_docker_dev_environment.md`](tasks/TZ_docker_dev_environment.md).

## Quick start (dev)

Requires Linux, Docker Engine ≥ 24 and Docker Compose v2. Go is not needed on the host.

```bash
cp .env.example .env
make up          # infrastructure + testbed + apps (first start 5–15 min)
make ps          # wait for healthy
make seed        # test domain lists → var/incoming/
make test        # unit tests
make test-integration  # environment check against the testbed
```

The public API of the web service (app profile) after startup —
http://127.0.0.1:8081 (the
`WEB_PORT` port in `.env`, published to loopback only; the landing pages
are static now — the separate ld-landing repo, they do not run here). The
pipeline operator
dashboard — http://127.0.0.1:8090 (the `DASHBOARD_PORT` port; login admin/admin
— dev creds, see [`docs/dev-environment.md`](docs/dev-environment.md)).

A live check of a single domain from the host (DNS → HTTP/render → Laravel
analysis, read-only): `make check DOMAIN=example.com` — works for both real
domains and the testbed (`*.crawl.test`): the primary resolver is the testbed
unbound, and on SERVFAIL/REFUSED the checker escalates to the public fallback
resolvers (`internal/checker/resolve.go`). The domain check API
(`POST /api/check-domain` on the web service) checks real domains the
same way — the web service
in dev gets the public `CRAWLER_DNS_FALLBACK_RESOLVERS` (see
[`docs/dev-environment.md`](docs/dev-environment.md) §6).

## Full local testing with real domains

By default the dev environment is hermetic: unbound runs in testbed mode and
answers SERVFAIL for everything outside the test zones. To run the full
pipeline cycle (ingest → scheduler → DNS → HTTP → analysis) over a list of
real domains with real DNS:

1. **Allow internet egress** — in `.env`:
   ```bash
   ALLOW_INTERNET=1
   ```
   The crawler User-Agent must carry the dev marker (the `.env` default is
   `CRAWLER_USER_AGENT=DomainCrawler/dev …`, do not remove it).

2. **Enable real DNS recursion and restart the stack**:
   ```bash
   make up-internet
   ```
   Unbound is recreated in internet mode — recursion from the root servers
   (spec 5.4); the other services are not recreated.

3. **Drop in a domain list** — the simplest option is a zone-less file
   (zones are mixed together, the zone is derived from the name; the format
   is free-form: one full name per line, `#` comments, registrar export
   lines — the domain is extracted automatically). The contract is
   [`docs/incoming-files.md`](docs/incoming-files.md):
   ```bash
   mkdir -p var/incoming/add
   printf 'laravel.ru\nlaravel.com\nexample.co.uk\n' > var/incoming/add/my-list.txt
   ```
   Per-zone directories (`var/incoming/<zone>/add/…`) remain for
   single-zone batches and testbed domains (`*.crawl.test`). Ingest picks up
   the file itself and moves it to `var/incoming/processed/` (invalid ones
   go to `failed/` with a report).

4. **Watch the pipeline**:
   ```bash
   make status                        # zones, queues, throughput
   make domain NAME=laravel.ru        # result: subdomains, redirects, score
   ```
   Operator dashboard — http://127.0.0.1:8090. For `q:dead` triage and
   re-analysis — [`docs/runbooks.md`](docs/runbooks.md) §2 and `make reanalyze`.

5. **Return to isolated mode**:
   ```bash
   make up
   ```
   In internet mode the test zones (`*.test`) do not resolve — running
   `make test-integration` and the testbed scenarios requires switching back
   to testbed.

**Start testing from scratch** — a full environment reset (all volumes, the
content packs in `var/content`, ingest reports) and an immediate start in
real-DNS mode:
```bash
make reset-internet
```
Equivalent to `make clean FORCE=1` + clearing host data + `make up-internet`
(checks `ALLOW_INTERNET=1` before the reset). Clearing only the queues,
without resetting the DB, is simpler — Redis in dev runs without persistence:
restart the container (`make down && make up` or `restart redis`).

A single-domain check without enqueueing it into the pipeline —
`make check DOMAIN=…` (see above); a live check via the API —
`POST http://127.0.0.1:8081/api/check-domain` with a JSON body
`{"domain":"example.com"}`.

## Documentation

The documentation map (the same table with "when to read" notes — in
[`docs/operations.md`](docs/operations.md) §1):

| Document | Contents |
|---|---|
| [`docs/operations.md`](docs/operations.md) | operations: startup scenarios (dev/production), updates and migrations, logs, metrics/alerts, the operator checklist |
| [`docs/dev-environment.md`](docs/dev-environment.md) | dev environment: compose profiles, testbed, the `crawlerctl` CLI, debugging, diagnostics |
| [`docs/docker.md`](docs/docker.md) | Docker cheat sheet: start/stop, commands inside containers, volumes, diagnostics |
| [`docs/configuration.md`](docs/configuration.md) | configuration: a single YAML + environment variables, priorities, validation (example: `config.example.yaml`) |
| [`docs/incoming-files.md`](docs/incoming-files.md) | incoming files contract `incoming/<zone>/add\|delete/*.txt`: validation, reports, delivery dedup (undo with the same list) |
| [`docs/services.md`](docs/services.md) | Go service framework: lifecycle, logging, metrics and alerts |
| [`docs/migrations.md`](docs/migrations.md) | PostgreSQL migrations (applied at service startup, creating new ones) |
| [`docs/schema.md`](docs/schema.md) | PostgreSQL schema (`zones`, `subdomains`, `domains`, `subdomain_results`, `contents`, `laravel_*`, `ingest_files`): partitioning, fillfactor, indexes, HOT updates, `domain_info` |
| [`docs/db.md`](docs/db.md) | DB layer: the `pgx` pool (budget 20–50 connections), streaming `COPY` in batches, batched `UPDATE … FROM unnest(...)`; production PostgreSQL tuning |
| [`docs/redis.md`](docs/redis.md) | Redis as the queue transport: dev/production, memory budget, `noeviction`, no persistence |
| [`docs/content.md`](docs/content.md) | content storage: pack files, the zstd dictionary, deduplication, compaction, non-find retention, disk threshold |
| [`docs/render.md`](docs/render.md) | rendering: Chromium, isolated contexts, waiting for readiness, DOM/cookie capture |
| [`docs/analyzer.md`](docs/analyzer.md) | the Laravel rules engine: targets (cookies/headers/HTML/meta), match evidence, re-analysis |
| [`docs/runbooks.md`](docs/runbooks.md) | procedural guides: zstd dictionary retraining, q:dead triage, re-analysis |
| [`docs/mutation-testing.md`](docs/mutation-testing.md) | go-mutesting mutation testing: coverage and thresholds of `make mutate`, reports, suppression of false mutants, a weekly CI run (three parallel jobs) |
| [`docs/deploy.md`](docs/deploy.md) | production systemd deployment: units, memory budget, installation |
| [`docs/ci-cd.md`](docs/ci-cd.md) | continuous deployment: GitHub Actions after green CI, rollback, secrets, Telegram notifications |
| [`docs/disks.md`](docs/disks.md) | production server disks: PostgreSQL and content volumes, SMART/TBW (spec §10.3) |
| [`docs/os-tuning.md`](docs/os-tuning.md) | production server OS tuning: sysctl — ephemeral ports, TIME_WAIT, nofile, conntrack, DNS UDP buffers (spec §10.2) |

Command list: `make help`.

## CI

On every push to `master` and in every pull request, GitHub Actions
([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs the checks —
the same make targets via Docker Compose as locally; Go is not needed on the
CI host:

image builds (`make build`, layers cached in the GitHub Actions cache via
`deploy/docker/compose.ci.yaml`) → `make lint` → `make test` →
`make test-race` (race detector for monitor) → `make test-rules`
→ `make test-integration` (testbed) → `make build-runtime`.

The status is the badge at the top; on failure the logs of all services are
uploaded to a run artifact. GitHub Actions and Go dependency updates are
raised by Dependabot ([`.github/dependabot.yml`](.github/dependabot.yml)).

Pipeline event notifications (build/lint and test failures in ci, deploy
results) are sent by a Telegram bot to the project group — spec §10.4,
details in [`docs/ci-cd.md`](docs/ci-cd.md).

Mutation testing is not part of the main CI (a heavy check): a separate
workflow, [`.github/workflows/mutation.yml`](.github/workflows/mutation.yml),
runs it weekly (Sunday 20:00 MSK) on `master` and on manual triggers
(`make mutate` in three parallel jobs with a score threshold, reports in
artifacts; [`docs/mutation-testing.md`](docs/mutation-testing.md)).

## Production server

Production deployment (T-135) — systemd units, details in
[`docs/deploy.md`](docs/deploy.md); updates — CI/CD
([`docs/ci-cd.md`](docs/ci-cd.md)).

| Parameter | Value |
|---|---|
| IP | `155.212.174.88` (VDS 18561438, IXcellerate — Moscow, `pulpirzvk.fvds.ru`) |
| OS | Ubuntu 24.04.5 LTS (kernel 6.8) |
| CPU / RAM / disk | 4 vCPU / 8 GB / NVMe 80 GB |
| Deployment | native systemd: 9 Go services + crawlerctl + Chromium, PostgreSQL 16 / Redis 7 / Unbound — OS packages |

Public entry points (TLS — Let's Encrypt, nginx):

- Public API of the crawler: <https://api9.laravel.city> (`/api/check-domain`,
  `/api/v1/landing/stats`; only `/api/` and `/healthz` are proxied, the rest — 404)
- EN landing page: <https://sites.laravel.city> — static, the separate
  [ld-landing](https://github.com/yepbro/ld-landing) repo (the primary/www
  names redirect here)
- RU landing page: <https://sites.laracity.ru> — static, the same repo
- Operator dashboard: <https://dashboard.laracity.ru> (HTTP Basic; credentials — held by the operator, in the server configuration)

## Structure

Matches Appendix A of the main spec; the `internal/` packages are filled in
as the stages are implemented (task links — in `tasks/tasks.md`, completed
ones — in the archive [`tasks/tasks_done.md`](tasks/tasks_done.md)).

| Directory | Purpose |
|---|---|
| `cmd/` | Go services (ingest, scheduler, dns-worker, http-worker, render-worker, analyzer, monitor, web — the public API, dashboard — the operator dashboard, crawlerctl) and testbed utilities (mockserver, testbedgen) |
| `internal/` | Shared code: the service framework (`service`, [docs/services.md](docs/services.md)), configuration (`config`), PostgreSQL access — pool and bulk writes (`db`), the domain model — statuses, flags, subdomain masks (`model`), name normalization — punycode/UTS-46, name zone — TLD (`idn`), Prometheus metrics (`metrics`), migrations at startup (`migrate`), file ingest — watching and the ingest pipeline (`watch`, `ingest`), the scheduler — selection/enqueue/lease/rechecks (`scheduler`), Redis Streams queues (`queue`), the DNS client with limits (`dnsclient`) and the q:dns loop (`dnsworker`), the HTTP transport with limits and redirects (`httpfetch`) and the q:http loop (`httpworker`), content storage (`content`, [docs/content.md](docs/content.md)), render (`render`, [docs/render.md](docs/render.md)) and the q:render loop (`renderworker`), the Laravel rules engine and the q:analyze loop (`rules`, `analyzer`, [docs/analyzer.md](docs/analyzer.md)), reconciliation of the pipeline's incremental counters and alerts (`monitor`), the pipeline status summary — queries and calculations for the CLI and dashboard (`status`), the public API — the check form endpoint, the landing stats endpoint (`web`), the operator dashboard — authentication and the state snapshot builder (`dashboard`, [docs/services.md](docs/services.md)) |
| `deploy/docker/` | The Compose environment, Dockerfiles, configs, testbed |
| `deploy/systemd/` | Production deployment on the target server: service systemd units, OS-package budget drop-ins, environment ([docs/deploy.md](docs/deploy.md)) |
| `deploy/nginx/` | A sample public entry point: a TLS reverse proxy in front of the `web` public API ([docs/deploy.md](docs/deploy.md) §6) |
| `deploy/sysctl/` | Production server kernel tuning (spec §10.2): `99-crawler.conf` for `/etc/sysctl.d` + application instructions; calculations — [docs/os-tuning.md](docs/os-tuning.md) |
| `deploy/smartd/` | NVMe SMART/TBW monitoring (spec §10.3): the smartd config, a warning handler for syslog/journald, a udev rule and a drop-in granting monitor alerts access to `/dev/nvme*`; calculations — [docs/disks.md](docs/disks.md) |
| `migrations/` | PostgreSQL migrations (golang-migrate): embedded into the binaries and applied by the services at startup |
| `rules/` | Laravel detection rules (reloaded on SIGHUP) |
| `testdata/laravel-samples/` | A corpus of page samples (positive/negative) |
| `test/integration/` | Integration tests of the dev environment against the testbed |
| `var/` | Working data: `incoming/` (domain files and the ingest archive: `processed/`, `failed/`), `content/` |

## Dependencies

Libraries — per Appendix B of the main spec; all are wired up in `go.mod`.

| Library | Purpose | Where used |
|---|---|---|
| `github.com/jackc/pgx/v5` | PostgreSQL: pool, COPY, batched UPDATE | `internal/migrate` (the migration runner), `internal/db` (the `pgxpool` pool, `COPY`, `UPDATE … FROM unnest`; T-007) |
| `github.com/golang-migrate/migrate/v4` | up/down schema migrations, advisory lock | `internal/migrate`, `migrations/embed.go` |
| `github.com/redis/go-redis/v9` | Redis Streams — task queues | `internal/queue` |
| `github.com/miekg/dns` | DNS client | the DNS stage (T-034+) |
| `golang.org/x/net` (`idna`) | IDN normalization (the Lookup profile, UTS-46) | `internal/idn` (T-009): punycode, name zone — TLD, is_idn; the DNS/HTTP stages |
| `github.com/chromedp/chromedp` | Chromium control via CDP (render) | `internal/render` |
| `github.com/klauspost/compress/zstd` | content compression | `internal/content` |
| `github.com/fsnotify/fsnotify` | watching the `incoming/` directory | `internal/watch` |
| `github.com/prometheus/client_golang` | Prometheus metrics | `internal/metrics` → `/metrics` of each service |
| `gopkg.in/yaml.v3` | configuration and Laravel YAML rules | `internal/config`, `internal/rules` |

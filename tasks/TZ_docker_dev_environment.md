# Technical Specification: Docker Development Configuration for the Domain Crawler Project

Version: 1.1 (the decisions on the assumptions and open questions are confirmed by the customer)
Related document: `TZ_domain_crawler.md` (the main service spec)

---

## 1. Purpose

Create a reproducible development environment based on Docker Compose that:

1. Brings up the entire project infrastructure with one command: PostgreSQL, Redis, a local DNS resolver (Unbound), Chromium for rendering, monitoring.
2. Contains an **isolated testbed** (fake DNS and web servers) so that development and tests do not depend on the real internet and do not load third-party sites.
3. Allows running the project's Go services with hot code reload and a debugger.
4. Is close to the production constraints (8 GB RAM, one IP, memory limits per service) so that memory and concurrency problems surface at the development stage.
5. Provides unified commands (`make`) for migrations, tests, loading test data, inspecting queues and logs.

---

## 2. Scope of work and deliverable

The deliverable is a set of files in the project repository:

```
/deploy/docker/
  compose.yaml                  # the main compose file
  compose.override.example.yaml # a local overrides template
  .env.example                  # environment variables
  Dockerfile.go                 # multi-stage: dev / build / runtime
  Dockerfile.render             # the render image (Chromium)
  postgres/
    postgresql.dev.conf
    init/                       # init scripts (extensions, roles, DB)
  redis/redis.dev.conf
  unbound/unbound.conf          # the dev resolver
  testbed/
    coredns/Corefile            # fake zones
    coredns/zones/*.zone
    web/                        # a Caddy web server with virtual hosts
    web/sites/<scenario>/       # static fixture pages
    mockserver/                 # a Go application for "evil" scenarios
  prometheus/prometheus.yml
/Makefile
/.air.toml                      # hot reload config (or an equivalent)
/docs/dev-environment.md        # a brief startup guide
/.github/workflows/ci.yml       # CI (GitHub Actions)
/testdata/laravel-samples/      # real Laravel samples from the customer + expected results
```

---

## 3. Host environment requirements

- Docker Engine ≥ 24, Docker Compose v2 (the `docker compose` command).
- Linux (Ubuntu LTS) — the only supported development OS; macOS and Windows are not supported.
- At least 8 GB of host RAM (16 GB recommended) and ≥ 20 GB of free disk space for the development volumes.
- Installing Go on the host is **optional**: building, tests and running happen in containers; if desired, a developer may run the services from the host.

---

## 4. The Compose service composition

### 4.1. Infrastructure services (always)

| Service | Image / basis | Purpose | Memory limit (dev) |
|---|---|---|---|
| `postgres` | `postgres:16` (the major version is pinned in `.env`) | The project DB | 1.5 GB |
| `redis` | `redis:7-alpine` | Queues (Streams) | 256 MB |
| `unbound` | an image based on Alpine/Debian with Unbound | Local recursive resolver | 256 MB |

Requirements:

- **postgres**: a persistent named volume; the `postgresql.dev.conf` config (parameters reduced for dev but keeping the key settings of the production schema: `synchronous_commit = off`, `random_page_cost = 1.1`, `effective_io_concurrency = 200`); the init scripts create the DB, the application role and the required extensions (if needed, e.g. `pg_stat_statements`); a `healthcheck` via `pg_isready`.
- **redis**: `maxmemory` and the `noeviction` policy; `appendonly` disabled or `everysec` (data loss is acceptable in dev); a `healthcheck` via `redis-cli ping`.
- **unbound**: a configuration in which:
  - in `testbed` mode (the default) the test-zone domains are forwarded to `coredns` (via `forward-zone`/`stub-zone`), and everything else **is not resolved outward** (`REFUSED`/`NXDOMAIN` is returned), so as not to accidentally reach the internet;
  - in `internet` mode (a profile, see 5) normal recursion is enabled.

### 4.2. Project applications

Each Go service from the main spec is a separate Compose service:

| Service | Run command (dev) | Profile | Ports |
|---|---|---|---|
| `ingest` | `air -c .air.toml` + `./cmd/ingest` | `app` | metrics `:9101` |
| `scheduler` | `air` + `./cmd/scheduler` | `app` | `:9102` |
| `dns-worker` | `air` + `./cmd/dns-worker` | `app` | `:9103` |
| `http-worker` | `air` + `./cmd/http-worker` | `app` | `:9104` |
| `render-worker` | `air` + `./cmd/render-worker` | `app` | `:9105` |
| `analyzer` | `air` + `./cmd/analyzer` | `app` | `:9106` |
| `monitor` | `air` + `./cmd/monitor` | `app` | `:9100` |
| `tools` | a container with `crawlerctl`, `migrate` and helper utilities (on demand, `docker compose run`) | `tools` | — |

Requirements:

- One shared `Dockerfile.go` image with the `dev` target (the Go toolchain, `air`, `delve`, `golangci-lint`, `migrate`), the source code is mounted via bind-mount; the module cache (`GOMODCACHE`) and the build cache (`GOCACHE`) live in named volumes for fast rebuilds.
- Each service gets its own memory limit (`mem_limit`) close to the production budget (see the spec, item 3.2) and `restart: unless-stopped`.
- The services use `unbound` as DNS (`dns: [<unbound ip>]`), not the Docker system resolver.
- The `incoming/` directory for `ingest` is a bind-mount from the host (`./var/incoming`) so that the developer can simply drop files in; the `processed/` and `failed/` directories sit alongside.
- The content storage directory is a named volume (or a `./var/content` bind-mount).
- The Laravel rules directory (`/rules`) is mounted from the host; rule changes are picked up on `SIGHUP` (the `make rules-reload` command).
- Debugging capability: port `2345` (delve) for the selected service via an override (`compose.override`), debugging started by `make debug SERVICE=dns-worker`.
- A `healthcheck` for each service: the `/healthz` HTTP endpoint on the metrics port.

### 4.3. Render (Chromium)

- The `Dockerfile.render` image: a base with headless-Chromium (e.g. `chromedp/headless-shell`) + the `render-worker` binary.
- Container parameters: `shm_size: 1gb` (or `--disable-dev-shm-usage` in the Chromium flags), `mem_limit` ≈ 2 GB (as in the production budget), non-root run, `init: true` (correct zombie process cleanup).
- Chromium uses the same DNS (`unbound`) and an isolated network; in `testbed` mode it has no internet access.
- Browser restart by page count and on memory overage is enabled (the behavior from the main spec).

### 4.4. The testbed

Purpose: a fully local "mini-internet" with known answers for development and automated tests.

| Service | Purpose |
|---|---|
| `coredns` | Authoritative DNS for the fake zones (`*.test` zones, IDN zones) |
| `testweb` | A **Caddy** web server with virtual hosts and self-signed certificates (HTTP and HTTPS) |
| `mockserver` | A Go application for scenarios that cannot be expressed statically: a tarpit, infinite and circular redirects, slow responses, huge bodies, dropped connections, wrong encodings, a zip bomb |

**Test zones** (a minimal set, extendable):

- `crawl.test`, `crawl2.test` etc. — ordinary zones;
- an IDN zone and IDN domains (e.g. `xn--…` equivalents of a Cyrillic and a hieroglyphic zone), to check punycode normalization and DNS/HTTP for IDN names;
- a composite zone (`co.test`-like public suffix) — for checking the zone/domain split (in the test suffix list).

**Required domain scenarios** (each is a separate domain/subdomain with a descriptive name, e.g. `nxdomain-case`, `wildcard-case`):

| Scenario | What it checks |
|---|---|
| NXDOMAIN at the apex | the `not_delegated` status, subdomain skipping |
| NS present, no A/AAAA | the `delegated_no_address` status |
| SERVFAIL / DNS timeout | the `temp_error` status, retries |
| Wildcard DNS | wildcard detection and skipping extra subdomains |
| CNAME chains (incl. a looped one) | chain handling and loop protection |
| Address present, ports closed | the `no_http` status |
| http only | one variant, `scheme = http` |
| https only | one variant, `scheme = https` |
| http and https lead to one site (an http → https redirect) | a single record |
| http and https — **different** sites | two records (`variant` 0 and 1) |
| `bare → www` and `www → bare` redirects | `alias_kind = 2`, a single content copy |
| `www` and without `www` — the same content without a redirect | `alias_kind = 1` |
| A redirect chain, `meta refresh`, a JS redirect | finding the final page |
| An infinite and a circular redirect | the limit triggering |
| A tarpit (slow read/serving) | minimum speed and timeouts |
| A huge body, a zip bomb | size and decompression limits |
| An invalid certificate, an expired one, a mismatching SAN | result recording, loading with `InsecureSkipVerify` |
| Different encodings (UTF-8, windows-1251, a wrong charset) | conversion to UTF-8 |
| An SPA shell (`#root`/`#app`, `<noscript>`) | render start, getting the DOM and the cookies set by JS |
| Cookies and headers | correct recording |
| Parking/the same pages on different domains | hash deduplication |
| Domains on a shared IP | the per-IP connection cap |
| **Laravel** pages — real samples provided by the customer (see 4.4.2); supplemented with synthetic ones for edge cases if needed | positive analyzer hits and version ranges |
| **Non-Laravel** pages with similar traits (e.g. `XSRF-TOKEN` from another framework, a `csrf-token` meta without Laravel) | no false hits / a low score |

The fixtures live in `testbed/web/sites/<scenario>/`; the zones — in `testbed/coredns/zones/`. Generating the zones from the scenario list is automated by a script (`make testbed-gen`) so that dozens of records are not edited by hand.

### 4.4.1. The testbed network

- A separate `crawler_net` Docker network (bridge) with static IPs for `coredns`, `unbound`, `testweb`, `mockserver`, so that the DNS zones reference stable addresses.
- To check "several domains on one IP" — all domains pointing to `testweb` share its address.
- To check the per-IP connection cap, several extra IPs (aliases) on `testweb` are allowed.

### 4.4.2. Laravel samples from the customer

Real Laravel page samples are provided by the customer and are used for two purposes at once: for developing the analyzer rules and as testbed fixtures.

The recommended format is a folder per sample in `testdata/laravel-samples/<id>/`:

```
index.html      # the final HTML/DOM of the page as the service will save it
headers.json    # the response headers
cookies.json    # cookies (name, value, attributes)
meta.yaml       # is_laravel: true|false, the expected version range (if known),
                # a note, the source and the receipt date
```

Usage:

1. **Corpus tests of the analyzer** (`make test-rules`): the rules run directly over the samples, without the network, and the result is checked against `meta.yaml` (the expected `is_laravel`, the version range).
2. **Testbed fixtures** (`make testbed-gen`): for each sample a test domain of the form `<id>.crawl.test` is generated, which serves `index.html` with the recorded headers and cookies via Caddy. This makes it possible to check the whole pipeline (DNS → HTTP → content → analyzer), not only the rules.
3. **Non-Laravel** samples (including those on other frameworks with similar traits, e.g. `XSRF-TOKEN` or `csrf-token`) are placed in the same directory with `is_laravel: false` and are used to guard against false hits.

Before entering the repository the samples are cleaned of personal data and secrets (e-mails, real tokens, keys); token and cookie values are replaced with structurally similar ones (the same length and format, e.g. `eyJpdiI6…`) so that the traits are preserved.

### 4.5. Monitoring and service tools (by profiles)

| Service | Profile | Purpose |
|---|---|---|
| `prometheus` | `monitoring` | Collecting the metrics of all services (viewed via the Prometheus UI and the `crawlerctl status` CLI; Grafana is not used in dev) |
| `adminer` or `pgadmin` | `tools` | DB browsing |
| `redisinsight` or `redis-commander` | `tools` | Redis stream browsing |

---

## 5. Compose profiles

Profiles are used to enable only what is needed:

| Profile | What it enables |
|---|---|
| (default) | `postgres`, `redis`, `unbound` |
| `testbed` | `coredns`, `testweb`, `mockserver` |
| `app` | the project's Go services and `render-worker` |
| `monitoring` | `prometheus` |
| `tools` | `adminer`/`pgadmin`, a Redis UI, the `tools` container |
| `internet` | Unbound with real recursion and outbound access (**only on explicit enablement**) |

The expected standard developer scenario: `make up` (infrastructure + testbed + app).

The `internet` profile must require explicit confirmation (the `ALLOW_INTERNET=1` variable in `.env`) to rule out accidental mass requests to the external network. In this mode the same rate limits apply as in the production configuration, and the `User-Agent` carries a `dev` marker.

---

## 6. The Dockerfile and the build

### 6.1. `Dockerfile.go` (multi-stage)

- The `base` stage: a Go image (the version is pinned in `.env`), setting `GOFLAGS=-mod=readonly`.
- The `dev` stage: `air`, `delve`, `golangci-lint`, `migrate` (golang-migrate), the `crawlerctl` dependencies; does not run as root; the working directory `/src`.
- The `build` stage: building the static binaries of all `cmd/*` (`CGO_ENABLED=0`, `-trimpath`, `-ldflags "-s -w"`).
- The `runtime` stage: a minimal image (distroless/alpine), only the binaries and an unprivileged user. Used to verify the "production" image (`make build-runtime`) and in CI.

### 6.2. Requirements

- Layers are cached: first `go.mod`/`go.sum` and the module download, then the code.
- The versions of the base images and tools are pinned (not `latest`).
- Go packages are not re-downloaded on every run (a cache in a volume).

---

## 7. Network, resources and proximity to production conditions

### 7.1. Resource limits

- Every service has a `mem_limit` (and `cpus` where applicable); the total limits do not exceed 8 GB — this lets us catch in advance the problems that will appear on the production server.
- Limit overrides — via `.env` or `compose.override.yaml` (for weaker developer machines).

### 7.2. DNS

- All application services use `unbound` as the resolver (an explicit `dns:`), the Docker system DNS is not used for them.
- For `render-worker` the Chromium DNS settings also point to `unbound`.

### 7.3. File limits and network parameters

- The compose file sets `ulimits` (`nofile`) for the workers.
- For the services that create many connections, the container-level `sysctls` (in particular, `net.ipv4.ip_local_port_range`) are documented; host-level settings (conntrack etc.) are described in `docs/dev-environment.md`.

### 7.4. Permissions and users

- Bind-mounting the directories is done with correct permissions: the developer's `UID/GID` are passed via `.env` and the `user:` parameter.

---

## 8. Data and migrations

- The migrations live in `/migrations` (the `golang-migrate` format); applied by `make migrate-up` (and `migrate-down`, `migrate-new NAME=…`).
- On the first environment start the DB is initialized automatically (the init scripts + optional auto-migration by the `migrate` service with a `service_completed_successfully` dependency).
- `make seed` — loading test data: domain lists for the test zones are generated into `var/incoming/<zone>/add/*.txt` (including IDN names and "bad" lines to check rejection), plus the initial `subdomains` set (`''`, `www`, `admin`, `crm`, `api`, `app`).
- `make seed-bulk N=1000000` — generating a large synthetic domain set for performance measurements and partitioning checks (no network access).
- `make db-reset` — a full recreation of the project DB and volumes (with confirmation).
- `make psql` — open a PostgreSQL console in a container.

---

## 9. Developer commands (the `Makefile`)

The required set of targets:

| Command | Action |
|---|---|
| `make up` | Bringing up the infrastructure, the testbed and the applications (the `testbed` + `app` profiles) |
| `make up-infra` | Postgres, Redis, Unbound only |
| `make down` | Stopping (the volumes are kept) |
| `make clean` | Stopping with volume removal (with confirmation) |
| `make logs SERVICE=…` | Service logs |
| `make ps` | Service state |
| `make build` / `make build-runtime` | Building the dev and production images |
| `make test` | Go unit tests in a container |
| `make test-integration` | Integration tests on the testbed (brings up everything needed, runs, returns the exit code) |
| `make lint` | `golangci-lint` |
| `make migrate-up` / `migrate-down` / `migrate-new` | Migrations |
| `make seed` / `make seed-bulk N=…` | Test data |
| `make testbed-gen` | Regenerating the testbed zones and fixtures |
| `make rules-reload` | `SIGHUP` to the analyzer to reload the Laravel rules |
| `make reanalyze` | Starting a re-analysis (via `crawlerctl`) |
| `make debug SERVICE=…` | Running a service under `delve` |
| `make status` | Calling `crawlerctl status` (zone progress, queues) |
| `make domain NAME=…` | Searching a domain by name: status and Laravel traits (`crawlerctl domain`) |
| `make test-rules` | Running the Laravel rules over the `testdata/laravel-samples` sample corpus without the network |
| `make psql` / `make redis-cli` | The DB and Redis consoles |
| `make shell SERVICE=…` | A shell in a container |

---

## 10. Configuration and secrets

- All configurable parameters — via `.env` (a `.env.example` template in the repository; `.env` itself is in `.gitignore`).
- The contents of `.env.example`: image versions, dev DB passwords, ports, memory limits, `UID/GID`, `ALLOW_INTERNET`, the data directory paths.
- The application configuration file (the YAML from the main spec) for dev lives in `deploy/docker/config.dev.yaml`; the values (the DSN, the Redis address, the `unbound` resolver, the paths) are substituted from the environment.
- Development secrets (passwords) are not used in other environments; real secrets never enter the repository.
- The service ports are published to `127.0.0.1` (not to all interfaces).

---

## 11. Environment testing and CI

- The repository has a **GitHub Actions** workflow (`.github/workflows/ci.yml`) that runs on push and pull request, builds the images (with layer caching) and runs `make lint`, `make test`, `make test-rules`, `make test-integration`. The CI environment is brought up with the same commands and compose files as locally.
- The integration tests run against the testbed and check the scenario table from item 4.4 (the expected statuses, variants, aliases, content present in the storage, analyzer results).
- An idempotency check: a repeated `make up` and `make seed` produces no errors.
- A cold start check: on a clean machine (after `make clean`) the environment is brought up by `make up` with no manual actions except copying `.env.example` to `.env`.

---

## 12. Documentation

`docs/dev-environment.md` must contain:

1. The host requirements and a quick start (5–7 steps).
2. A description of the profiles and typical work scenarios.
3. The directory structure (`var/incoming`, `var/content`, the volumes).
4. How to add a new scenario to the testbed (a domain, a zone, a fixture, the expected result in a test).
5. How to change the Laravel rules and apply them (`rules-reload`, `reanalyze`).
6. Debugging (delve), viewing the metrics, the DB, the queues.
7. Troubleshooting of common problems (memory shortage, bind-mount permissions, port conflicts, DNS resolution).
8. Recommendations on host kernel/network tuning for load tests.

---

## 13. Acceptance criteria

1. After `cp .env.example .env && make up` on a clean machine, `postgres`, `redis`, `unbound`, the testbed and all the Go services come up; all `healthcheck`s are green within a reasonable time (≤ 3 minutes with warm images).
2. A Go code change leads to an automatic rebuild and restart of the corresponding service (hot reload) without rebuilding the image.
3. A file with domains dropped into `var/incoming/<zone>/add/` is processed and appears in `processed/`; the domains enter the DB, for IDN zones and domains — in punycode form.
4. The end-to-end check of the test domains gives results matching the scenario table (item 4.4), including: the statuses, the http/https variants, the www/bare aliases, the Laravel rules hitting on the customer's real samples with the expected versions and not hitting on the negatives; `make domain NAME=<domain>` shows the status and the traits.
5. The application services **have no internet access** in `testbed` mode (checked by an automated test: a request to an external domain does not resolve/connect).
6. The `internet` mode is enabled only explicitly (`ALLOW_INTERNET=1` + the profile).
7. `make test` and `make test-integration` pass in containers without installing Go on the host; the integration tests finish with an exit code usable in CI.
8. The total memory limits of the containers do not exceed 8 GB; when a service exceeds its limit, its container restarts without stopping the others.
9. `make clean && make up` reproduces the environment from scratch with no manual steps.
10. The documentation (`docs/dev-environment.md`) lets a new developer bring up the environment without additional explanations.

---

## 14. Implementation stages

| Stage | Content |
|---|---|
| 1 | `compose.yaml` with `postgres`, `redis`, `unbound`; `.env.example`; healthchecks; the Makefile (the basic targets) |
| 2 | `Dockerfile.go` (dev/build/runtime), hot reload, the shared module cache, starting the stub services |
| 3 | Migrations, DB init scripts, `seed`, the `incoming/`/`content` directories |
| 4 | The testbed: `coredns`, `testweb`, `mockserver`; zone generation; the DNS/HTTP scenarios |
| 5 | The render image and service (Chromium), SPA fixtures |
| 6 | Loading the customer's real Laravel samples (`testdata/laravel-samples`), negative samples, connecting the analyzer and the rules (`rules-reload`), `make test-rules` |
| 7 | Profiles, monitoring (Prometheus), tools (Adminer, Redis UI) |
| 8 | Integration tests on the testbed, the GitHub Actions workflow |
| 9 | Debugging (delve), the load profile (`seed-bulk`), documentation, the final acceptance |

---

## 15. Accepted decisions and open questions

### 15.1. Decisions confirmed by the customer

- The development environment is Linux only.
- CI is GitHub Actions.
- The production server runs under systemd; no separate compose for production is created.
- The Laravel samples for the fixtures are real, provided by the customer (section 4.4.2).
- Monitoring in dev is Prometheus and the CLI (`crawlerctl status`); Grafana is not used.
- The testbed tools are CoreDNS and Caddy.
- The queue is Redis Streams (as in the main spec).

### 15.2. Left to the developers' discretion

- Hot reload via `air` (replacing it with an equivalent is allowed, keeping the behavior).
- The specific memory limit values and sample parameters are refined by measurements.

### 15.3. Open questions

- The timing and format of handing over the real Laravel samples (the recommended format is described in 4.4.2) and the availability of non-Laravel page samples for false-positive checks.

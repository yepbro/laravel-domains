# Docker Guide

A practical cheat sheet: starting and stopping the environment, running
commands inside containers, working with volumes, and diagnostics. For the
environment architecture — [`dev-environment.md`](dev-environment.md); for
the full list of make commands — `make help`.

## 1. Pre-check

```bash
docker version --format '{{.Server.Version}}'   # Docker Engine ≥ 24
docker compose version                          # Compose v2 (a plugin, not docker-compose)
cp .env.example .env                            # once; DEV_UID/DEV_GID = your id -u / id -g
```

Everything (builds, tests, lint, migrations) runs in containers — Go is not
needed on the host.

## 2. Starting and stopping

### Standard scenarios (make)

```bash
make up          # infrastructure + testbed + apps; waits for all healthchecks
make up-infra    # postgres + redis + unbound only (the services apply the schema)
make down        # stop everything; volumes and data are kept
make clean       # stop AND remove all volumes (DB, caches, content); asks for confirmation
make clean FORCE=1   # the same without asking
make ps          # status of all containers
make logs SERVICE=analyzer   # one service's logs (tail + follow)
```

- After Go code edits, `make up` rebuilds only what changed (hot reload then
  keeps working on its own, via air).
- `make up` is idempotent: rerunning it breaks nothing.
- `make clean && make up` brings the environment up from scratch with no
  manual steps.

### What the healthchecks check (T-110)

- Infrastructure — the images' own checks: `pg_isready` (postgres),
  `redis-cli ping` (redis), resolution of a test name (unbound).
- The app services (the app profile) — **liveness** `/livez` on the metrics
  port: the container is healthy as long as the process is alive and answers
  HTTP. The readiness `/healthz` (a PostgreSQL pool ping, 503 on failure) is
  deliberately not used in the healthcheck: workers pull tasks from the
  Redis queues themselves, and with the DB down a wall of unhealthy workers
  would only duplicate the postgres status itself. Check readiness manually
  (the metrics ports are not published outward — from inside the service
  container or via tools):
  `dc exec scheduler wget -qO- http://127.0.0.1:9102/healthz`
  (the ports — the "Metrics" table in [`services.md`](services.md)).
- `tools` — the healthcheck is disabled: the container lives as
  `sleep infinity` without an HTTP server, and the check was permanently red
  (noise in `dc ps`, a hanging `up --wait` with the tools profile).
- `render` (the runtime profile) — no healthcheck: the headless-shell image
  has no HTTP client (wget/curl); process liveness is maintained by
  `restart: unless-stopped` on the exit code. The probes are reachable from
  the crawler_net network on the host:
  `wget -qO- http://172.28.100.15:9105/healthz`.
- `lightpanda` / `moli` (the render-engines profile, stage 35) — the CDP
  endpoint `/json/version` (the same one the worker checks against
  chromium). curl is placed into the moli image on purpose, and the probe
  goes to the service's static address `172.28.100.17` (moli binds to it —
  see compose); the official lightpanda image is debian-slim without an
  HTTP client, so its probe against `127.0.0.1` is plain bash (`/dev/tcp` +
  a `grep` for "Browser" in the response body).

### Direct docker compose commands

The Makefile is a wrapper over compose. The canonical invocation from the
repository root:

```bash
docker compose --project-directory . -f deploy/docker/compose.yaml --env-file .env <command>
```

It is convenient to make an alias (add it to ~/.bashrc):

```bash
alias dc='docker compose --project-directory . -f deploy/docker/compose.yaml --env-file .env'
```

The examples below use `dc`.

```bash
dc ps -a                                  # all containers, including stopped ones
dc logs -f --tail=100 unbound             # logs
dc restart coredns testweb                # restart services
dc stop http-worker                       # a graceful stop of one service
dc up -d --no-deps http-worker            # bring up one service without dependencies
dc --profile monitoring up -d             # enable a profile (same for tools, app, testbed)
dc --profile '*' down --remove-orphans    # stop everything under all profiles
dc --profile '*' down -v                  # + remove volumes (= make clean FORCE=1)
```

Important: do **not** run `docker compose` from the `deploy/docker` directory
without `--project-directory .` — relative paths (volumes, build) resolve
against the project root, and without the flag the configuration will not
assemble.

### Restarting a single service after edits

Go services rebuild themselves (air watches the files). The other services
re-read their config only on recreation:

```bash
dc restart unbound                 # the unbound config changed
make testbed-gen                   # regenerates zones/fixtures and restarts coredns/testweb/unbound
dc up -d --force-recreate testweb  # if you edited the service's compose parameters
```

## 3. Commands inside containers

Two ways:

- `exec` — a command **in an already running** container (the service must
  be up);
- `run --rm` — a **one-off container** on top of the service image (does not
  bring up dependencies; useful for utilities).

```bash
# interactive shell
make shell                       # the tools container (Go toolchain, internet access for modules)
make shell SERVICE=analyzer      # a shell inside a running app service

# the same directly
dc exec analyzer sh              # a running service
dc run --rm tools sh             # one-off tools

# one-off commands
dc exec postgres psql -U admin -d crawler -c '\dt'     # SQL in a single call
dc exec redis redis-cli XLEN q:dns                     # stream length
dc exec unbound nslookup same-site.crawl.test 127.0.0.1   # check the resolver
dc exec testweb wget -qO- http://127.0.0.1:2019/config/   # the Caddy admin API
dc run --rm tools go test ./internal/... -count=1       # tests without make
```

Via make:

```bash
make psql                # the interactive PostgreSQL console
make redis-cli           # the interactive Redis console
make test                # unit tests (in tools)
make lint                # golangci-lint
make migrate-up          # apply migrations manually (usually not needed: the services do it themselves)
make migrate-new NAME=my_migration
make debug SERVICE=dns-worker   # the service under delve, the debugger on 127.0.0.1:2345
```

The user in the app containers is your UID/GID from `.env` (files created in
bind mounts belong to you). HOME inside is `/tmp`.

## 4. Testbed: how to inspect the scenarios by hand

The testweb static sites from the host — via `curl --resolve` (the
`127.0.0.1:8088`/`8443` ports from `.env`):

```bash
curl --resolve same-site.crawl.test:8088:127.0.0.1 http://same-site.crawl.test:8088/
curl -k --resolve spa-shell.crawl.test:8443:127.0.0.1 https://spa-shell.crawl.test:8443/
```

DNS queries to the test zones — via the running unbound (from any container
on the network; port 53 is not published on the host):

```bash
dc exec unbound nslookup parked-1.crawl.test 127.0.0.1
dc exec unbound nslookup example.com 127.0.0.1    # → an error: external names do not resolve in the testbed
```

The mockserver "evil" endpoints (from inside the network — directly by IP):

```bash
dc run --rm --no-deps tools wget -qO- http://172.28.100.13:8080/healthz
```

The scenario domains (`tarpit.crawl.test`, `huge-body.crawl.test`,
`zip-bomb.crawl.test`, `infinite-redirect`/`cycle-redirect`, `truncated`,
`garbage-content`, `bad-charset-*`, `cookies`, `slow-headers`, `tls-*`)
resolve to the mockserver and are served as virtual hosts: a `/` request
with a scenario's Host gets that scenario's "evil" response (the route map
is in `cmd/mockserver`, matching `mock_path` from `scenarios.yaml`). The TLS
scenarios get a certificate with their own defect over SNI (self-signed /
expired / a wrong SAN). Edits to the mockserver source are picked up by
`make test-integration` (the image is rebuilt).

The full scenario list: `deploy/docker/testbed/scenarios.yaml`.

## 5. Volumes and data

| Volume | Contents | When cleared |
|---|---|---|
| `crawler_pg_data` | the PostgreSQL database | `make clean` |
| `crawler_gomodcache`, `crawler_gocache` | Go caches (fast rebuilds) | `make clean` |
| `crawler_caddy_data`, `crawler_caddy_config` | the testweb TLS certificates | `make clean` |
| `crawler_prom_data` | Prometheus metrics (7 days) | `make clean` |

Data outside volumes: `var/incoming` (domain files), `var/content` (content)
— bind mounts from the repository, never committed to git.

```bash
docker volume ls | grep crawler
docker volume rm crawler_pg_data     # a targeted reset of the DB only
```

## 6. Building images

```bash
make build            # dev images (crawler/go-dev, unbound, mockserver) — cached
make build-runtime    # production check images: crawler/runtime:check, crawler/render:check
dc build tools        # build one service
dc build --no-cache unbound   # rebuild without the cache (if something looks off)
```

A regular `make up` also builds images when needed (`--build`). After
changing the versions in `.env` (`GO_VERSION`, `ALPINE_VERSION`, …) the
images are rebuilt automatically on the next `make up`/`make build`.

### The production render container (the runtime profile)

`make build-runtime` only checks that the images build. The full production
render container is brought up by the `runtime` profile — the `render`
service (`crawler/render:local`): render-worker + its own Chromium in one
container (`Dockerfile.render`; the image has `USER nobody`, in compose the
launch is overridden to `user: ${DEV_UID:-1000}:${DEV_GID:-1000}` — the
worker writes pack content to the host bind `./var/content` owned by
`DEV_UID`, without a manual `chown`; the unprivileged non-root run is
preserved — spec §9/§10.1), with the fixed resource requirements of spec
§3.2/§9/§10.1 (T-065):

```bash
dc --profile runtime up -d render   # also brings up the dependencies (postgres, redis)
docker inspect crawler-render-1 --format '{{.HostConfig.Memory}}'   # 2147483648 ≈ 2 GB (spec 3.2)
dc --profile runtime stop render    # stop it
```

- `mem_limit: ${MEM_RENDER:-2048m}` — the Chromium budget ≈ 2 GB (spec 3.2);
  `mem_limit` is the docker analog of the cgroup `MemoryMax` from spec 10.1:
  the OOM-killer does not touch PostgreSQL or the other services.
- `shm_size: 1gb`, `init: true` — as for the dev chromium service (spec 4.3).
- `cap_drop: ALL`, `no-new-privileges`, an unprivileged user (`nobody` in
  the image, `DEV_UID`/`DEV_GID` in compose, matching the writes to the
  `var/content` host bind) — rendering untrusted pages without root
  (spec §9).
- Chromium restarts every N pages and on the RSS threshold — from inside the
  worker (`render.restart_every_pages`, `render.restart_memory_bytes`;
  config — `deploy/docker/config.render.yaml`, `remote_url` empty —
  production mode); the hard cap is `mem_limit`; details —
  [`render.md`](render.md), the "Browser restart and memory threshold"
  section.
- systemd units with `MemoryMax`/`User=` (deployment outside Docker) —
  [`deploy/systemd/`](../deploy/systemd/), instructions in
  [`deploy.md`](deploy.md) (stage 9, spec 10.1 — the native production
  deployment: OS packages, static binaries; docker-runtime remains an
  option for Docker hosts).

The service consumes `q:render` with a full work cycle (T-067: render in
isolated contexts, replacing the raw HTML with the final DOM in storage,
publishing `q:analyze`) — the pipeline is described in
[`render.md`](render.md), "Queue cycle".

## 7. Docker diagnostics

```bash
dc ps -a                          # unhealthy/exited — the first look
dc logs --tail=200 <service>      # why it is crashing
dc top analyzer                   # processes inside the container
docker stats --noheader           # CPU/RAM usage of all containers
docker inspect crawler-analyzer-1 --format '{{.HostConfig.Memory}}'  # the limit
dc events                         # the event stream (restart, oom, etc.)
```

Typical problems:

- **`pool overlaps`** when creating a network — the compose subnet is taken
  by another project. Check `docker network ls` +
  `docker network inspect <name>`; if necessary, replace the
  `172.28.100.0/24` subnet in `deploy/docker/compose.yaml`,
  `deploy/docker/unbound/*.conf`, `deploy/docker/testbed/scenarios.yaml`,
  `test/integration/env_test.go`, `deploy/docker/config.dev.yaml` and run
  `make testbed-gen`.
- **An app container in a restart loop** — `dc logs <service>`: most often
  it is air with non-compiling code; the error is visible in the log, and
  after the fix everything rebuilds on its own.
- **Permission denied in `var/`** — the directory was created by root (for
  example, by Docker when auto-creating a bind mount). Fix:
  `sudo chown -R $(id -u):$(id -g) var`.
- **A port is taken on the host** — change `PG_PORT`/`REDIS_PORT`/… in `.env`.
- **"Address already in use" at service start** — a live one-off run
  container (`make test`/`lint`/`debug` from another tree) took the address
  of a stopped static service — a residual risk of the address plan (§8):
  wait for the run to finish or remove the stuck `tools-run-…` (the "Hung
  containers" item below), then start the service again.
- **Out of disk space** — `docker system df`; clean the build caches with
  `docker builder prune` (safe, rebuilds take longer); the project volumes —
  only via `make clean`.
- **"Hung" one-off tools containers** — `dc rm -f tools-run-…` or
  `docker rm $(docker ps -aq --filter label=com.docker.compose.project=crawler --filter status=exited)`.

## 8. Network, addresses and ports

The `crawler_crawler_net` network (`172.28.100.0/24`). All **long-lived**
services got static addresses (T-180): Docker's dynamic allocator does not
reserve the addresses of stopped containers, and a starting dynamic service
would take someone else's address — the owner failed with
"Address already in use" and would not come up without manual fixing
(incidents 2026-10-23/24 — stage 28 in `tasks/tasks.md`). The address plan
by class:

| Service | Address | Profile |
|---|---|---|
| postgres | `172.28.100.2` | default |
| redis | `172.28.100.3` | default |
| unbound | `172.28.100.10` | default (the DNS of all app services) |
| coredns | `172.28.100.11` | testbed |
| testweb | `172.28.100.12` | testbed (all fixture domains; testbedgen bakes the address into the zones) |
| mockserver | `172.28.100.13` | testbed |
| chromium | `172.28.100.14` | app (CDP `ws://…:9222`) |
| render | `172.28.100.15` | runtime (the production render container) |
| render-fallback | `172.28.100.18` | runtime (worker B — the fallback render, T-240) |
| lightpanda | `172.28.100.16` | render-engines (CDP `ws://…:9222`, T-241) |
| moli | `172.28.100.17` | render-engines (CDP `ws://…:9222`, T-241) |
| monitor | `172.28.100.20` | app |
| ingest | `172.28.100.21` | app |
| scheduler | `172.28.100.22` | app |
| dns-worker | `172.28.100.23` | app |
| http-worker | `172.28.100.24` | app |
| render-worker | `172.28.100.25` | app |
| analyzer | `172.28.100.26` | app |
| web | `172.28.100.27` | app |
| dashboard | `172.28.100.28` | app |
| prometheus | `172.28.100.30` | monitoring |
| adminer | `172.28.100.31` | tools |
| redis-insight | `172.28.100.32` | tools |
| tools | — (dynamic) | tools — the only exception to the plan (see below) |
| — | `172.28.100.250` | the "black hole": an address with no listener, the closed-ports scenario — claimed by no service |

The app service addresses follow a mnemonic: the address `.2k` — the
metrics port `910k` (monitor `9100` → `.20`, … dashboard `9108` → `.28`);
`.29` is reserved for a new app service, and monitoring/tools are free from
`.33` on. The render engines (stage 35, T-241) sit next to chromium/render —
`.16`/`.17`: they have no metrics ports (browsers, not Go services), so the
`.2k` mnemonic does not apply. Worker B (render-fallback, T-240) is a Go
service of the runtime class: `.18`, right after the render engines; the
`.2k` mnemonic does not apply — the runtime class lives in separate
containers with its own metrics ports (`:9105` in each, each with its own
network namespace — no conflict); in the systemd deployment worker B has a
fixed loopback metrics port 9120 (deploy.md §2.3), moved out of the
`render-worker@` instance range. The infrastructure/testbed/chromium/render
addresses are historical and do not change — they are pinned at their
consumers (the testbedgen DNS zones `172.28.100.12`,
`ws://172.28.100.14:9222` in the render live tests, the DSN `172.28.100.2`
in `config.dev.yaml` and the migration tests); the lightpanda address is
also baked into its `--advertise-host` (advertising the CDP address in
`/json/version` — compose.yaml), and the moli address — into
`command --host` and the healthcheck address (moli binds to its static
address and probes `/json/version` on it): changing any engine's address
under the plan means editing all the places at once (ipv4_address + flags).

**The exception — `tools`: no address is assigned.** Its runs (`make test`,
`make lint`, `docker compose run …`) are one-off run containers launched in
parallel from the main tree and from worktrees with
`COMPOSE_PROJECT_NAME=crawler`, i.e. onto the same `crawler_net` network: a
shared static address would conflict between them.

The residual risk (accepted deliberately): if the owner of a static address
is **stopped**, its address becomes the lowest free one (the allocator
hands out free addresses from the lowest), and the next one-off run
container will take it — then the owner will not start, with
"Address already in use". The window is bounded by the run container's
lifetime and self-heals (`--rm`): wait for the run to finish (or remove the
stuck one with `dc rm -f tools-run-…`, see §7) and start the owner again.
While all static services are running, the run containers take free low
addresses (.4–.9, .18–.19, …) and bother no one.
For the same reason, run `make debug SERVICE=<app service>` (a run container
of an app service) after stopping the service's regular container:
`dc stop <service>` — otherwise the run container will not get its static
address.

A one-time transition note (the update to T-180): containers started before
the plan was introduced keep their old dynamic addresses and may conflict
with the new static ones (for example, scheduler could be holding the
render's .15) — the first application of the plan requires a full
recreation via `make down && make up`; after it, the profile start order
does not matter.

Host ports (`127.0.0.1` only): `5432` PostgreSQL, `6379` Redis, `9090`
Prometheus, `8080` Adminer, `5540` Redis Insight, `8088`/`8443` testweb
HTTP/HTTPS, `8081` the web public API (`WEB_PORT` in `.env`), `8090` the
dashboard service's operator dashboard (`DASHBOARD_PORT`; password login —
dev creds in config.dev.yaml, see [`dev-environment.md`](dev-environment.md)).
Service metrics: `9100`–`9108` (inside the network only).

Service names (`postgres`, `redis`, …) resolve via Docker DNS from any
container on the network; for the app services all the remaining DNS goes
through unbound (isolation from the internet), while `tools` gets regular
DNS (proxy.golang.org is needed: new dependencies are downloaded with
`make mod-download`).

## 9. Unbound: modes, memory and fallback resolvers

The configs are in `deploy/docker/unbound/`; `Dockerfile.unbound` runs both
through `unbound-checkconf` when building the image. The mode is set by the
`UNBOUND_MODE` variable and is switched only by `make up` /
`make up-internet` (see [`dev-environment.md`](dev-environment.md)).

| Mode | Config | Behavior |
|---|---|---|
| `testbed` (default) | `unbound.testbed.conf` | all traffic is forwarded to coredns (172.28.100.11): the test zones answer authoritatively, everything else gets REFUSED, nothing leaves for the outside (integration tests of the DNS stage) |
| `internet` (`make up-internet`) | `unbound.internet.conf` | full recursion from the root servers, prefetch, the cache within the spec §3.2 budget |

Parameters of the `internet` mode (spec §5.4, §3.2):

- **Recursion** — no `forward-zone`: unbound resolves on its own from the
  root servers (built-in root hints). **Prefetch** (`prefetch: yes`): a
  record requested while its remaining TTL is < 10% is refreshed before it
  expires — popular records do not fall out of the cache (the spec §5.4
  wording).
- **Cache** ≈ 542 MB — inside the spec §5.4 window (512–768 MB):
  `msg-cache-size 128m` + `rrset-cache-size 384m` (≈3× msg — the NLnet Labs
  recommendation of 2–4×) + `neg-cache-size 16m` (the crawler gets NXDOMAIN
  en masse) + `key-cache-size 4m` (the DNSSEC validator is disabled, a pure
  `iterator`) + `infra-cache-numhosts 100000` (~10 MB of NS RTT/timeout
  statistics).
- **Container memory** — `mem_limit: ${MEM_UNBOUND:-700m}`: the spec §3.2
  budget "Unbound (cache) ≈ 0.7 GB"; `mem_limit` is the docker analog of
  `MemoryMax` from §10.1. The cache tables grow lazily as they fill (the
  startup RSS ≈ 20 MB), but the limit must accommodate a fully filled
  cache: ~542 MB + ~160 MB of headroom for the process, the thread buffers
  (`num-threads: 2` for the `dns.concurrency`/`dns.max_qps` load) and
  allocator fragmentation. The `testbed` mode uses ~150 MB and fits any
  limit — if the dev machine is short on memory, `MEM_UNBOUND=256m` can be
  restored in `.env` for the testbed. The production `MemoryMax` was raised
  to 800M (T-160: daily cache growth to ≈47 million names reaching
  733.9 MiB at the 700M cap — load-measurements §12); the dev limit of 700m
  was left deliberately (like the http-worker mem_limit after T-143): the
  growth is an effect of a multi-day run, dev sessions are shorter, and the
  cap does not reserve memory.

### Why the public fallback resolvers are not in the unbound config

Spec §5.4: "Public resolvers — only as a fallback (the list in the config)".
The unbound semantics do not allow implementing this inside the resolver:

- a `forward-zone` with `name: "."` intercepts **all** queries — the
  forwarders become primary for all traffic;
- `forward-first: yes` only changes the direction of failure: on SERVFAIL
  from a forwarder, unbound retries the query with its own recursion. The
  reverse direction "recursion → forwarder on SERVFAIL" does not exist in
  unbound — such a fallback is implemented in the query client.

Routing the crawl (hundreds of millions of domains) through
`1.1.1.1`/`9.9.9.9` as the first hop contradicts the letter of the spec
("only as a fallback"), risks hitting the rate limits of the public
services, and devalues the local cache with prefetch.

So the list lives in the application config and is consumed by the
**dns-worker client**: on SERVFAIL/timeout of the local unbound
(`dns.resolvers`) the query is retried directly against
`dns.fallback_resolvers` (`internal/dnsclient`, T-034). The field and the
`CRAWLER_DNS_FALLBACK_RESOLVERS` env override exist since T-006; the
defaults per spec §5.10 are `1.1.1.1:53`, `9.9.9.9:53` (public anycast
resolvers of two independent operators — Cloudflare and Quad9). In
dev/testbed the list is empty (`config.dev.yaml`) — isolation from the
internet is preserved; with `make up-internet` it is enabled when needed
via the same variable. Details — [`configuration.md`](configuration.md),
the `dns` section.

## 10. Scenario cheat sheet

```bash
# everyday work
make up && make logs SERVICE=analyzer

# "it fell over, bring everything back up with a clean DB"
make db-reset

# look at the state of a specific domain in a test zone
make domain NAME=laravel-11-vite.crawl.test

# run everything CI runs
make lint && make test && make test-rules && make test-integration && make build-runtime

# end the day
make down
```

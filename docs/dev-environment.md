# The Domain Crawler dev environment

A reproducible development environment on Docker Compose: PostgreSQL, Redis,
a local DNS resolver (unbound), an isolated "mini-internet" testbed
(CoreDNS + Caddy + mockserver), headless Chromium, monitoring, and Go services
with hot code reload.

Corresponds to `tasks/TZ_docker_dev_environment.md`.

Hands-on Docker work (starting/stopping, commands inside containers,
volumes, diagnostics) is split into a separate cheat sheet: [`docker.md`](docker.md).
Operations in general (starting, updating, metrics, procedures) —
[`operations.md`](operations.md); the input file contract —
[`incoming-files.md`](incoming-files.md).

## 1. Requirements and quick start

Host requirements:

- Linux (Ubuntu LTS), Docker Engine ≥ 24, Docker Compose v2 (`docker compose`);
- ≥ 8 GB of RAM (16 recommended), ≥ 20 GB of free space;
- Go on the host is **not needed** — everything is built and tested in containers.

Quick start:

```bash
# 1. Clone the repository and enter it
cd laravel-domains

# 2. Create .env (UID/GID are substituted automatically if desired)
cp .env.example .env

# 3. Bring up infrastructure + testbed + apps (first run: 5–15 min)
make up

# 4. Check the state (all healthchecks must turn healthy)
make ps

# 5. Load the test domain lists into var/incoming/
make seed

# 6. Run the environment tests
make test-integration
```

Management:

```bash
make logs SERVICE=analyzer   # a specific service's logs
make psql                    # the PostgreSQL console
make redis-cli               # the Redis console
make shell                   # a shell in the tools container (the Go toolchain)
make down                    # stop (the volumes are kept)
make clean                   # stop and remove the volumes (asks for confirmation)
make reset-internet          # a full reset (volumes, var/content, ingest
                             # reports) + start with real DNS
```

## 2. Profiles and typical scenarios

The compose profiles (`deploy/docker/compose.yaml`):

| Profile | What it includes |
|---|---|
| (default) | `postgres`, `redis`, `unbound` |
| `testbed` | `coredns`, `testweb` (Caddy), `mockserver` |
| `app` | the Go services (`ingest` … `monitor`, `render-worker`) + the public API `web` (http://127.0.0.1:8081) + the operator dashboard `dashboard` (http://127.0.0.1:8090, dev creds — admin/admin) + `chromium` |
| `render-engines` | `lightpanda`, `moli` — alternative render engines (stage 35): on demand, when the render-worker is switched off chromium (see "Render engines" below) |
| `monitoring` | `prometheus` (the UI at http://127.0.0.1:9090) |
| `tools` | `adminer` (http://127.0.0.1:8080), `redis-insight` (http://127.0.0.1:5540), the `tools` container |

PostgreSQL migrations have no separate service: every Go service applies
them itself at startup (see [`migrations.md`](migrations.md)).

Typical scenarios:

```bash
make up-infra        # DB/Redis/DNS only (work on the schema or migrations)
make up              # infrastructure + testbed + apps (a standard day)
make test            # unit tests (in a container, no Go needed on the host)
make lint            # golangci-lint
make test-race       # the -race race detector for internal/monitor (CGO is enabled automatically)
make test-rules      # the Laravel rules over the testdata/laravel-samples corpus
make test-integration# the environment integration tests on the testbed
make mutate          # mutation testing (heavy; mutation-testing.md)
```

The `-race` rule (T-190): keep the packages with goroutine loops (currently
`internal/monitor` — slowquery/load/zonestats) clean under
the race detector — locally `make test-race`, the same step is in CI.
The regular `make test` runs without `-race` (CGO_ENABLED=0), so races in
tests are caught only by this target; synchronize fakes that the loop's goroutine
reads with a mutex (the style of `zonestatsFakePool`,
`fakeSlowSource` in slowqueries_test.go).

Monitoring and the tools are brought up on demand:

```bash
docker compose --project-directory . -f deploy/docker/compose.yaml --env-file .env \
  --profile monitoring --profile tools up -d
```

### Dashboard: dev credentials and hash regeneration

The operator dashboard (the compose service `dashboard` of the app profile, the dashboard spec)
is published at http://127.0.0.1:8090 (`DASHBOARD_PORT` in `.env`) and
requires login: the dev credentials are **admin / admin** (the dev environment only;
in production — `dashboard.username`/`password_hash`, docs/deploy.md §4.5).
The hash lives in the `dashboard` section of the `deploy/docker/config.dev.yaml` file;
regenerate it when changing the dev password (stdin — the password does not stay in
the shell history):

```bash
printf 'admin' | docker compose --project-directory . \
    -f deploy/docker/compose.yaml --env-file .env \
    run --rm -T tools go run ./cmd/crawlerctl dashboard-secret
```

Put the resulting `pbkdf2-sha256$…` string into `password_hash` (and
update the comment with the password); after a service restart
(`docker compose … restart dashboard`) the new credentials take effect, and all
cookie sessions are invalidated (the HMAC key is a KDF-derived key, the dashboard spec §7.5).

### Internet mode (caution)

By default the environment is **isolated from the internet**: unbound forwards all
queries to coredns, the test zones answer authoritatively, everything else —
REFUSED. Real recursion is enabled explicitly:

```bash
# 1) in .env: ALLOW_INTERNET=1
make up-internet     # recreates unbound in recursion mode
```

The rate limits from the production configuration apply here too; the `User-Agent`
carries a `dev` mark (the `CRAWLER_USER_AGENT` variable in `.env`).
To return to the isolated mode — `make up`.

### Render engines: lightpanda and moli (stage 35)

Besides the standard `chromium` (the `app` profile, CDP at `ws://172.28.100.14:9222`)
the dev stack can bring up both alternative render engines — Lightpanda
([lightpanda-io/browser](https://github.com/lightpanda-io/browser),
the official image) and Moli ([lexmount/moli](https://github.com/lexmount/moli),
its own minimal image built from the release tarball — `deploy/docker/Dockerfile.moli`).
Both speak CDP on the same `/json/version` endpoint as Chromium and
are brought up by the `render-engines` profile — **on demand**, not together with
`make up`: a dev machine does not need three browsers at once.

Switching the engine for the render-worker is an edit of two configuration keys
(`engine` + `remote_url`), no code changes (FR-16 of the render-engine spec).
Here and below `dc` is an alias of the canonical compose invocation
([`docker.md`](docker.md) §2):

```bash
# 1) bring up the engine container (the first run builds/pulls the image)
dc --profile render-engines up -d lightpanda   # or: moli

# 2) switch the render-worker: in deploy/docker/config.dev.yaml replace
#    render.engine and render.remote_url (commented-out samples are right there):
#      lightpanda → engine: lightpanda + ws://172.28.100.16:9222
#      moli       → engine: moli + ws://172.28.100.17:9222

# 3) restart the worker — air does not re-read the config
dc restart render-worker
```

The `render.restart_memory_bytes` restart threshold is already `0` in the profile
(the browser in dev is always an external container, the RSS of a foreign process is unavailable —
validation requires 0 for moli/lightpanda), so the switch is exactly
two keys; the chromium address needs no extra configuration when switching back.

Switching back to chromium — restore `remote_url: "ws://172.28.100.14:9222"`
and comment `engine` back out (chromium is the default), restart
the worker; the engine containers can be stopped
(`dc --profile render-engines stop lightpanda moli`).

The addresses in `crawler_net`: `lightpanda` — `172.28.100.16`, `moli` —
`172.28.100.17` (the address plan — [`docker.md`](docker.md) §8). The versions
are pinned in `.env` (FR-17): `LIGHTPANDA_IMAGE`, the `MOLI_VERSION` +
`MOLI_SHA256` pair (the checksum of the release asset) and the image base
`DEBIAN_VERSION`; switching an engine version is a separate procedure with a
smoke run, not an accidental "latest" drift. Operational flags (FR-18):
Lightpanda's telemetry is mandatorily disabled by the
`LIGHTPANDA_DISABLE_TELEMETRY` variable (release builds have it on by default),
`--obey-robots` is not passed; `--advertise-host 172.28.100.16` is not
required by the worker (the lightpanda engine's remote allocator connects via
`render.remote_url` as is, T-234) and is kept as a guard for third-party
CDP clients that go by the advertised `webSocketDebuggerUrl`;
Moli is started with `serve` bound to its static address
`172.28.100.17` (the `/json/version` advertisement is built from the bind address; the worker
takes only the browser-session path from it since T-234, the connection address — from
`render.remote_url`), without `--layout` (geometry is not needed) and without `--resource`
(the resource families are blocked by the render stage itself, T-062). The healthcheck of both
engines is `GET /json/version` (inside the lightpanda container the probe goes to
`127.0.0.1:9222`, moli — to its static address; from the network it is more convenient
to check the engines as a whole:
`dc run --rm --no-deps tools wget -qO- http://172.28.100.16:9222/json/version`).

Per-engine live render tests (T-254): a full CDP slice of each engine
against the `*.crawl.test` zones — context isolation, DOM/cookie/header
capture, Fetch interception, the SSRF contour, UA override; the engine table, the addresses —
`CHROMIUM_WS_URL`/`MOLI_WS_URL`/`LIGHTPANDA_WS_URL` (the defaults — the
stack addresses; an unreachable engine at the default address — SKIP; in the
tools container `CHROMIUM_WS_URL` is set by compose — an unreachable chromium
gives a failure, not SKIP). How to run them and the current state of the pinned versions —
the doc comment of `internal/render/engines_live_test.go` and the section
["Switching the pinned engine version (FR-17)"](#switching-the-pinned-engine-version-fr-17-t-254)
below.

### Render fixtures and an isolated stage run (T-242/T-243/T-244)

The engine comparison bench (the render spec §6.1) does not drive domains through the whole
pipeline: the fixture is a dump of real q:render tasks (`subdomain_results` rows
with the final URLs after the SPA verdict), imported into a test DB and
published straight into q:render — the render stage is tested in isolation, without
DNS/HTTP. The full cycle (`dc` — an alias of the canonical compose invocation,
[`docker.md`](docker.md) §2; the dev DB DSN — modeled on `APP_PG_DSN` in
compose, the password `APP_DB_PASSWORD` from `.env`):

```bash
# 1) fixture export (reads the DB without writing; the ok/failed/heavy strata,
#    a smoke subsample; the dev DB is small — the quotas degrade honestly, the report
#    is printed and written into manifest.json)
dc run --rm -e CRAWLER_POSTGRES_DSN="$APP_PG_DSN" tools \
    go run ./cmd/crawlerctl fixtures export \
    --out /src/var/fixtures-render --ok 2000 --failed 500 --heavy 500

# 2) the test DB (separate from production, the spec §6.1 item 3). The first step is a dry-run:
#    without --force the command writes nothing, it prints the target (user@host:port/
#    dbname from the DSN, no password — verify this is the bench!) and the full
#    import plan: rows per table (insert/update by the existing PKs),
#    key duplicates, what will be swept out of contents
dc run --rm -e CRAWLER_POSTGRES_DSN="$APP_PG_DSN" tools \
    go run ./cmd/crawlerctl fixtures import \
    --dir /src/var/fixtures-render

#    The import — with an explicit --force (S-P3-5): the import overwrites zones/
#    domains/subdomain_results by PK and sweeps contents — protection against
#    a wrong DSN. The dev stack's migrations are applied by the services at startup
#    (a fresh make up) — the import goes into an empty DB; it is idempotent by PK
#    (a repeat does not duplicate, it restores the dump state),
#    the zones.id / domain_id_seq sequences are set above
#    the maximum, the content links are reset (contents was not exported —
#    the bench writes the DOM itself)
dc run --rm -e CRAWLER_POSTGRES_DSN="$APP_PG_DSN" tools \
    go run ./cmd/crawlerctl fixtures import \
    --dir /src/var/fixtures-render --force

# 3) publishing the tasks into q:render (--publish requires --force): all — the whole
#    dump (the full run §5.1), smoke — the smoke subsample domains (a pinned-version
#    switch, the spec §6.1 item 6); the import and the publish can be combined
#    in one invocation
dc run --rm -e CRAWLER_POSTGRES_DSN="$APP_PG_DSN" tools \
    go run ./cmd/crawlerctl fixtures import \
    --dir /src/var/fixtures-render --force --publish all
```

The run: the bench's render-worker reads the same Redis (`redis.addr`), the engine —
by the switch above (chromium/lightpanda/moli). The fallback bench (worker
B — the `render-fallback` service of the runtime profile): its exposure
`http://render-fallback:9105/metrics` is added as a line to
`dashboard.render_metrics_urls` (a sample is commented out in
`config.dev.yaml`), the dev stack's Prometheus scrapes it with the
`render-fallback` job. Re-running the same engine (variability control,
the spec §5.1 item 2): a repeated `fixtures import --force` (a reset of the
rows to the dump state — the worker run had changed rendered/content links/finalization markers;
the run's orphaned content is swept out at the same time) + a stream cleanup
(`DEL q:render q:render-fallback q:analyze q:dead` in the bench's Redis, with the
workers stopped — otherwise DEL races their XADD/XACK) +
`--publish` again — the two runs are compared from the same start.

### The engine comparison report (T-244)

A run leaves its results in the test DB rows, but a repeated fixture
import resets them to the dump state (and sweeps the run's content) —
therefore between runs the "after the run" state is captured as a snapshot,
and the report compares the snapshots. The bench comparison cycle (T-247):

```bash
# 1) snapshot the finished run — IMMEDIATELY upon completion (an empty q:render,
#    the fallback has finished) and BEFORE the repeated import of the next run;
#    --metrics — the bench render-worker's /metrics, --engine-rss-max —
#    the engine container's max RSS over the run (a periodic poll of
#    docker stats --no-stream during the run;
#    the dev stack has no engine memory exporter, without the flag the report's memory
#    threshold degrades to "no data")
dc run --rm -e CRAWLER_POSTGRES_DSN="$APP_PG_DSN" -e CRAWLER_REDIS_ADDR="$REDIS" tools \
    go run ./cmd/crawlerctl fixtures snapshot \
    --dir /src/var/fixtures-render --label chromium-1 --engine chromium \
    --metrics http://172.28.100.25:9105/metrics --engine-rss-max 734003200

# 2) the next run (reset+publish from the block above; the engine — by
#    the switch) → its snapshot: --label moli-1 --engine moli …

# 3) the comparison report against the §5.1 item 4 thresholds (detection <1%, success,
#    the rate, memory <⅓ of the chromium budget, 0 crashes) — over the snapshot files,
#    without infrastructure; --out saves the text into the T-247 protocol,
#    --fail-exit — exit code 1 on any "FAILED" (for scripts)
dc run --rm tools \
    go run ./cmd/crawlerctl fixtures report \
    --baseline /src/var/fixtures-render/chromium-1.snapshot.json \
    --candidate /src/var/fixtures-render/moli-1.snapshot.json \
    --out /src/var/fixtures-render/report-moli-1.txt
```

The snapshot captures: the fixture rows from the DB (successful renders — `rendered` +
a content link: after the import `content_hash = NULL`, the link is set
only by the DOM written by the run; the DOM length/hash; the nodes/scripts — by reading the
DOM from the content store, `--no-dom` skips it; the analyzer's
`laravel_findings` verdicts — the detection diff material), the q:analyze stream boundaries
by record IDs (the rate, rows/s; the streams are cleaned between runs — the boundaries
belong to one run) and — with `--metrics` — the stage counters: the
finalization reasons by the `taskFinalizeReason` dictionary (net::ERR_\*), the
page time (mean/p95 over the histogram), the browser error class. The report
prints the §5.1 item 3 metrics and the explicit verdict rows; a live DOM
is non-deterministic — the structural metrics are compared normalized
(the medians of the length/nodes/scripts ratios, the share of identical `content_hash`),
a byte-wise diff is not applied.

The degradations are honest, the report invents no values: a missing source
(the analyzer has not finished the tail, Redis/`--metrics` not captured, the engine RSS
not provided) yields a "NO DATA" row instead of a verdict. One operational
nuance: between runs restart the bench's render-worker —
the `/metrics` counters then equal exactly the run; a process shared by two
snapshots is recognized by the report via `process_start_time_seconds`, and it degrades
the counter-dependent verdicts with a warning (the success rate over the DB
rows and the rate over the streams remain valid). The detection diff compares
the verdicts of one rule set: diverging `rules_version` between
runs is also an honest degradation of the detection threshold; a missing
`laravel_findings` row is the normal "no findings" (the analyzer deletes it at
zero matches), not a gap — the loss of the last signal is caught as a
discrepancy. The comparable rows are those written by the run (content present) and
stamped by the analyzer (`subdomain_results.rules_version` not NULL:
the render clears the stamp, the analyzer sets it after its pass over the DOM) —
take the snapshot after the analyzer has finished, otherwise the tail is excluded from the diff.

Two more bench requirements: do not include http-worker in its composition —
the content retention cycle (T-166) selects rows by the fixture's
`checked_at` (production dates) and can cut the fresh run's content links
before the snapshot; `--engine-rss-max` is the engine container's RSS maximum,
captured during the run (a periodic poll of `docker stats --no-stream`;
an instant reading after the run understates the peak).

Bench isolation. The bench is a separate instance of the dev stack (its own `postgres.dsn`
and `redis.addr` in its configuration): the live pipeline services must not
run against its DB and Redis — scheduler+monitor (they would recount
`zones.domains_pending` and re-publish the fixture domains via `q:dns` →
an HTTP episode would overwrite the fixture rows), ingest, dns-worker, http-worker
(its T-166 content retention cycle would cut the `headers`/`cookies`/content
links of the rows "old" by `checked_at` between import and publish). The bench's
composition: postgres, redis, render-worker (the engine under test), analyzer
(the verdicts are needed for the detection diff, the spec §5.1 item 3), later — the fallback worker.
Do not publish the fixture into the live pipeline's Redis: the tasks would go to the production
render worker. A dedicated Redis DB (for example 15 — the integration tests') is no good here:
`fixtures import` takes the address from
`redis.addr` and does not select a DB index — a separate instance is needed
(point `CRAWLER_REDIS_ADDR` at it). The import is protected against a wrong DSN (S-P3-5):
without `--force` the command writes nothing — it prints the target (user@host:port/dbname
from the DSN, no password) and the plan; `--force` prints the target before the first write —
check it: `fixtures import` overwrites zones/domains/results
by PK and sweeps contents — it is a test-DB tool, not a production one.

### Switching the pinned engine version (FR-17, T-254)

Switching the pinned version of a render engine is a separate procedure, not a
silent drift to latest (FR-17 of the render-engine spec): the versions are pinned in `.env` — `LIGHTPANDA_IMAGE`,
the `MOLI_VERSION` + `MOLI_SHA256` pair (the checksum of the release asset),
`CHROMEDP_HEADLESS_IMAGE` (the latter also goes into the args of `Dockerfile.render` —
the runtime profile is rebuilt with the same pin). The procedure has two layers: first
the hermetic testbed slice (it needs no internet), then a real smoke over
the fixture's smoke subsample (the render-engine spec §6.1 item 6, ~300 domains). Any
red step — revert the variable (the image of the previous version stays local,
a re-up brings it back) and analyze the cause.

1. **Rebuild/re-up of the engine profile.** moli: change
   `MOLI_VERSION`/`MOLI_SHA256` → `dc --profile render-engines up -d moli`
   (the image tag carries the version — compose rebuilds it itself); lightpanda: change
   `LIGHTPANDA_IMAGE` → the same up; chromium: change `CHROMEDP_HEADLESS_IMAGE`
   → `dc up -d chromium` (+ rebuilding the render/render-fallback of the
   runtime profile if the bench also checks the production image). Health — the
   `/json/version` healthcheck in `dc ps` green.
2. **Layer 1 — the testbed slice of the new engine** (`TestEnginesLiveCDPSlice`,
   `internal/render/engines_live_test.go`; the engine table: the addresses
   `CHROMIUM_WS_URL`/`MOLI_WS_URL`/`LIGHTPANDA_WS_URL`, the defaults — the
   stack addresses, an unreachable engine — SKIP):

   ```bash
   dc run --use-aliases -e CRAWLER_RENDER_TEST=1 --rm tools \
       go test ./internal/render -run 'TestEnginesLiveCDPSlice/<engine>' -v -count=1
   ```

   The slice gate prints the `/json/version` response — cross-check the version in it against the new
   pin. A red slice is a stop signal: the new engine does not carry the stage's CDP invariants
   (the invariants — spec 5.6/FR-5: per-task context isolation, DOM/cookie/header
   capture, Fetch interception, the SSRF contour, UA override).
   The pinned-version precedents as of T-254/T-272 are in the file's doc comment:
   lightpanda 1.0.2 — one browser context per connection (a session
   is not created at all); moli — InvalidParams on `Fetch.enable` for the
   Media/Font types in v1.1.21/1.1.22 (every task failed at interception
   enablement); in v1.1.23 (the T-272 retest, 2026-11-10) the
   base set's patterns are accepted, but interception is still broken: with
   Fetch interception on, a synchronous XHR fails on the client with
   NetworkError, `failRequest` of the main navigation leaves the tab
   unresponsive without a load error; Stylesheet is not accepted in v1.1.23 either
   (the `BlockStylesheet` CSS mode fails the whole call) — until the
   upstream fix the moli slice must stay red (a retest per release,
   T-247).
3. **Real smoke test on the smoke subsample.** The bench is a separate instance
   of the dev stack in internet mode (`ALLOW_INTERNET=1`, `make up-internet` —
   real domains need outward resolution) with the composition and isolation from
   the section above. The fixture — a fresh dump from production (`fixtures export`, T-242,
   on a production copy) or the bench's existing dump. The order:

   ```bash
   # 0) baseline-chromium over the same smoke sample — once per dump
   #    (the engine switch — the "Render engines" section above; after
   #    draining q:render — a snapshot; refresh it when the dump/rules change)
   # 1) import and publish the smoke subsample (~300 domains)
   #    ($APP_PG_DSN — the bench test DB's DSN modeled on compose,
   #    $REDIS — the bench Redis address; as in the fixture block above)
   dc run --rm -e CRAWLER_POSTGRES_DSN="$APP_PG_DSN" tools \
       go run ./cmd/crawlerctl fixtures import \
       --dir /src/var/fixtures-render --force --publish smoke
   # 2) the bench's render-worker — into the profile of the engine under test
   #    (engine + remote_url in the bench config, dc restart render-worker),
   #    wait for the q:render drain (crawlerctl status / the dashboard)
   # 3) snapshot of the candidate's run (the metrics — the bench render-worker's /metrics)
   dc run --rm -e CRAWLER_POSTGRES_DSN="$APP_PG_DSN" -e CRAWLER_REDIS_ADDR="$REDIS" tools \
       go run ./cmd/crawlerctl fixtures snapshot \
       --dir /src/var/fixtures-render --label moli-v1.1.22 --engine moli \
       --metrics http://172.28.100.25:9105/metrics
   # 4) the report against baseline-chromium (the render-engine spec §5.1 item 4 thresholds)
   dc run --rm tools go run ./cmd/crawlerctl fixtures report \
       --baseline /src/var/fixtures-render/chromium-baseline.snapshot.json \
       --candidate /src/var/fixtures-render/moli-v1.1.22.snapshot.json
   ```

   On 300 domains the §5.1 thresholds are indicative (the full procedure — the T-247 bench
   comparison): the smoke criteria — success/rate without degradation
   against the baseline, 0 crashes of the engine core, memory in order; between
   the baseline and the candidate restart the render-worker (the counters = exactly
   the run) and repeat `import --force` + `--publish` (the row and
   stream reset — the "Render fixtures" section above).
4. **Decision and recording.** Green steps 1–3 — the pin updated (when changing the
   default — a `.env.example` commit), the log — `docs/operations.md`; a red
   step — revert the variable, re-up, for an engine-side cause — an issue in
   the engine's upstream (the precedents — the doc comment of `engines_live_test.go`).

The procedure's manual steps (there is no tool, the operator performs them): the engine RSS
measurement during the run — a periodic `docker stats --no-stream` and
`--engine-rss-max` on the snapshot (the dev stack has no engine memory exporter,
without the flag the report's memory threshold honestly degrades to "NO DATA"); the
queue drain control; refreshing baseline-chromium when the dump or the
rules change (the detection diff compares the verdicts of one `rules_version`). Layer 2 is
a procedure, not an automated test: the real internet and live domains never
participate in CI or the regular `make test`.

## 3. Directory structure

```
deploy/docker/           compose files, Dockerfiles, service configs
  testbed/               the testbed: scenarios, zones, fixtures, mockserver
migrations/              golang-migrate migrations: embedded into the binaries,
                        applied by the services at startup (make migrate-new)
rules/                   the Laravel rules (YAML), reload: make rules-reload
testdata/laravel-samples/  page samples (index.html, headers.json, cookies.json, meta.yaml)
var/incoming/            the ingest watch folder: <zone>/add|delete/*.txt and archives
                         (.zip/.tar/.tar.gz/.tgz/.gz — docs/incoming-files.md §9)
var/incoming/processed/  the archive of processed files (the zone/operation structure kept)
var/incoming/failed/     bad files with a *.report log report nearby
var/content/             the content store (dev)
tmp/                     hot-reload artifacts (air)
```

Docker volumes: `pg_data` (the DB), `gomodcache`/`gocache` (the Go caches — fast
rebuilds), `caddy_data` (the testweb TLS certificates), `prom_data`.

### The ingest data flow (spec 5.1)

The full input file contract (the format, encoding, rejections, the error-share
threshold, the reports) — [`incoming-files.md`](incoming-files.md).

The `ingest` service watches `var/incoming/` (fsnotify + a periodic walk,
size/mtime stability) and drives every ready file through the
pipeline: parsing the path `<zone>/add|delete/*.txt` or an archive (T-133 —
the unpacking is streaming, the members pass the same stages) → the zone (created
automatically on the first file) → one pass over the file (line-by-line
parsing and sha256 at the same time) → line validation (invalid lines go to the
`rejected` log with a reason, the `service=rejected` attribute on the service's stderr) →
loading into the staging table → applying (`add` — insert/revival from
deleted, `delete` — a move to deleted) → the report into the `ingest_files` table
→ moving the file to the archive:

| File outcome | Where | What remains |
|---|---|---|
| applied | `processed/<zone>/<operation>/…` | an `ingest_files` row (sha256, counters) |
| a duplicate — an exact repeated delivery (zone + action + sha256) | `processed/…` | no new report — the re-processing is not performed |
| bad (the path structure, the zone, the rejection share > `ingest.max_error_ratio`) | `failed/…` | a `<file>.report` log report nearby: the reason and the line counters |
| an infrastructure failure (the DB, reading) | stays in `incoming/` | reprocessed after the service restart (at-least-once) |

Only an exact repeated delivery is not applied again — the same zone,
action and content (T-113); the same content from `delete/` after
`add/` (an undo) or in another zone is processed again —
idempotency; empty files and files of comments only are allowed.
The service also runs a background cleanup of the deleted domains' results
(a pass once a minute in batches of up to 1000 domains, T-020).

## 4. How to add a testbed scenario

1. Describe the domain in `deploy/docker/testbed/scenarios.yaml`:
   the DNS records (`a: testweb|mockserver|blackhole`, `www`, `ns`, `wildcard`,
   `cname`, `cname_loop`, `a_many` — N distinct A records of 192.0.2.0/24:
   a large answer is truncated in UDP without EDNS0 (TC=1) and requires a TCP fallback,
   `special: servfail|timeout`) and the site
   (`site.template`, the http/https scheme, the redirect behavior).
   Fix the expectation in the `expect` field — the integration
   tests use it and it documents the scenario.
2. For static pages the template (`generic`, `parked`, `redirect-chain`,
   `meta-refresh`, `js-redirect`, `spa`) generates the fixture automatically.
   For dynamic scenarios use `mockserver` (see `cmd/mockserver`,
   add an endpoint and rebuild if needed: `make build`):
   a `site: {template: mock, mock_path: …}` scenario is served under its own
   domain as a virtual host — "/" with the scenario's Host is routed to
   `mock_path` (the host map in `cmd/mockserver` is edited together with the
   scenarios), TLS scenarios get a defective certificate by SNI.
3. `make testbed-gen` — regenerates the zones (`coredns/zones/`), `zones.conf`
   and the `Caddyfile` with the fixtures; the coredns/testweb services restart.
4. Add a check to `test/integration/env_test.go` if the scenario
   belongs to the acceptance criteria.

The bench zones are multi-level (`crawl.test`, `co.test`, …): the TLD semantics of
the intake directories (the tld-only-zones spec §5.1) allows the working directories
`incoming/<tld>/`, so all the scenario zones are listed in the
`ingest.allowed_zone_paths` allowlist of the dev environment config
(`deploy/docker/config.dev.yaml`) — without an entry the `make seed` files
go to `failed/`. A new scenario zone — add it to the allowlist
(the name — lowercase punycode, as in `zones.conf`); the consistency is
guarded by the `TestSeedZonesAllowedInDevConfig` unit test
(`cmd/crawlerctl/seed_test.go`). The production allowlist is empty.

Laravel samples are added as a directory into `testdata/laravel-samples/<id>/`
(`index.html`, `headers.json`, `cookies.json`, `meta.yaml`) — the
`<id>.crawl.test` domain is created automatically at the next `make testbed-gen`;
testweb serves the headers from `headers.json` (including `Set-Cookie` with the Laravel cookies)
together with the page, so cookie-based analysis works in e2e.

The generated artifacts (`coredns/zones/`, `zones.conf`, the `Caddyfile`, the `web/sites/` fixtures)
are committed to the repository: CI builds the zones from the commit. A desync with
`scenarios.yaml`/the samples is caught by the `TestGeneratedArtifactsInSync` unit test
(`cmd/testbedgen/main_test.go`) — after editing the scenarios or the samples
it fails until `make testbed-gen` is run. The bench composition is cross-checked there too
against the mandatory list of spec 11.2 (`TestScenariosCoverTZ`):

| Spec 11.2 | Scenario | Where implemented |
|---|---|---|
| NXDOMAIN | `nxdomain-case` | a zone without records (coredns, an authoritative NXDOMAIN) |
| wildcard | `wildcard` | `*.wildcard` A in the zone + an SPA-independent page (testweb) |
| tarpit | `tarpit` | mockserver `/tarpit`: a body at 512 B/s |
| infinite redirect | `infinite-redirect` (+`cycle-redirect`) | mockserver `/redirect/infinite`, `/redirect/cycle/a` |
| giant body | `huge-body` | mockserver `/huge`: Content-Length 1 GiB |
| an SPA page | `spa-shell` | testweb: `#root` + `<noscript>` + a JS cookie/content |
| Laravel pages of different versions | `<id>.crawl.test` for the 17 samples of `testdata/laravel-samples` | testweb: statics + `headers.json` (the `XSRF-TOKEN`/`*_session`/CMS-name `__Secure-october_session`/`winter_session` cookies, `Content-Type` etc.), versions 5.8–12, the mix/vite/inertia/livewire stacks, a stateless API/404 JSON/CMS cookies + 5 negatives |

Beyond the mandatory list the bench also emulates: a DNS SERVFAIL and timeout
(`servfail`, `dns-timeout`), NODATA for a delegated name (`delegated-no-address`),
a CNAME chain and loop, the TCP fallback (`tc-fallback`, 60 A records), closed
ports (`closed-ports`), the http/https variants and www aliases, slow headers,
a zip bomb, a connection drop, junk bytes, broken encodings, defective
TLS certificates (self-signed/expired/wrong SAN), parkings for
content deduplication, a shared IP for the connection limit.

## 5. Laravel rules

- The rule files: `rules/*.yaml` (the format, validation and `rules_version` —
  [`configuration.md`](configuration.md), the `analyzer` section; the reference —
  spec 5.8 of the main spec). The working set — `rules/laravel.yaml` (22 rules;
  the weights and the version boundaries are justified in the comment on each rule).
- Changing the rules: edit the YAML → `make rules-reload` (a SIGHUP to the analyzer,
  no container restart; air does not watch `rules/` specifically).
- A corpus check without the network: `make test-rules` — a run over
  `testdata/laravel-samples`, cross-checking `is_laravel`/the version range and the full
  composition of the fired rules against `meta.yaml` (the `rules` field; `rules: []` — not
  a single rule may fire). The same cross-check is performed by the unit test
  `internal/rules/laravel_test.go`.
- The `meta.yaml` format of a sample: `is_laravel`, `ver_min`/`ver_max` — the real
  version of the sample (the computed range must cover it), `rules` —
  the full composition of the fired rules; the exact engine expectations (T-075):
  `score` — the noisy-OR of the `rules` weights, rounded to 3 digits
  (a cross-check with a ±0.001 tolerance), `calc_ver_min`/`calc_ver_max` — the exact strings
  of the computed range's boundaries (`""` — no boundary, NULL in the DB),
  `conflict` — an empty intersection of the ranges; plus `note`/`source`/
  `obtained`. The end-to-end corpus with these expectations is driven by the unit test
  `internal/rules/corpus_test.go` (the full "headers+cookies+HTML →
  EvaluateCtx → ScoreRules → IsLaravel(0.6)" story, negative samples,
  the score math, range conflicts, the HTML limits).
- Re-analysis of the stored content: `make reanalyze` — finds the results
  with a stale `rules_version` and enqueues them into `q:analyze` (without
  re-downloading; details — [`analyzer.md`](analyzer.md), the "Re-analysis" section).
  The same pass is made by a background goroutine of the analyzer service: a rule change
  via `make rules-reload` starts the re-analysis without a restart. The step-by-step
  operator instruction (edit → check → apply → control) —
  [`runbooks.md`](runbooks.md) §3.

## 6. The crawlerctl CLI

`crawlerctl` is the crawler management CLI (spec 4.1). The command framework —
routing, flags, name validation (internal/idn) and `--help` for every
command (task T-011); the management and dev commands are implemented. Exit
codes: 0 — success, 1 — an execution error (including "not found"),
2 — a usage error (arguments/flags).

```
crawlerctl status                        # a summary over the zones/queues (T-079)
crawlerctl domain <name> [--json|--content]
                                         # the domain info (spec 5.11):
                                         # the status, the results, the Laravel
                                         # findings, the content; the name in any
                                         # form
crawlerctl subdomains list|add|disable|recheck
                                          # the subdomain catalog (T-083)
crawlerctl zones list|enable|disable|sync
                                          # zone management (T-084),
                                          # populating the catalog from the
                                          # IANA registry (T-125)
crawlerctl reanalyze [--zone NAME]        # re-analysis under the new rules
                                          # (T-074): enqueueing into q:analyze
                                          # without re-downloading
crawlerctl recheck <domain> [domain …]     # force-scheduling the domain
                                          # for a check (T-084)
crawlerctl dict train <file> [--samples PATH]…
                                          # training/retraining the zstd
                                          # compression dictionary (spec 5.7)
crawlerctl dashboard-secret [PASSWORD]     # the operator dashboard's password hash
                                           # (dashboard.password_hash,
                                           # the dashboard spec §7.2); the password is an argument
                                           # or stdin (the argument stays
                                           # in the shell history)
crawlerctl near [--zone NAME] [--min-score F]
                                          # Laravel finds in the gray zone
                                          # of the threshold — calibration material
                                          # for the rules (T-115); the default threshold —
                                          # analyzer.laravel_min_score
crawlerctl check <domain>                  # a live domain check by the pipeline
                                           # (T-119): DNS → HTTP (+an SPA
                                           # render) → analysis; read-only —
                                           # writes nothing to the DB and does not
                                           # publish to the queues (the only
                                           # read — the subdomain catalog),
                                           # the result — stdout only;
                                           # to put a domain on the real
                                           # cycle with a DB write — recheck
crawlerctl counters list|recount|verify  # the pipeline's incremental counters
                                          # (T-178): the values,
                                          # a manual reconciliation (--zone),
                                          # a no-write cross-check against the live aggregates
                                          # (exit code 0/1); the drift
                                          # analysis — docs/runbooks.md §7
crawlerctl seed / test-rules             # the dev commands (make seed, make test-rules)
crawlerctl fixtures export --out DIR [--ok N] [--failed N] [--heavy N]
                                          # [--smoke N] [--weeks N] [--seed N]
                                          # the render fixture export (T-242,
                                          # the render spec §6.1 items 1–2): a selection
                                          # of subdomain_results — the q:render tasks —
                                          # by the ok/failed/heavy strata
                                          # proportional to the zones + the smoke domains;
                                          # the content store is not exported.
                                          # The dump (zones/domains/
                                          # subdomain_results/smoke NDJSON +
                                          # manifest) — the T-243 import contract;
                                          # the v1 format — the doc comment of
                                          # cmd/crawlerctl/fixtures.go. Reads the
                                          # DB via postgres.dsn, writes
                                          # only the --out directory
  crawlerctl fixtures import --dir DIR [--force] [--publish all|smoke]
                                          # importing the fixture dump into a test DB
                                          # (T-243, the render spec §6.1 items 3–4). The import —
                                          # a write without a rollback (an overwrite by PK +
                                          # a contents cleanup): without --force — a dry-run
                                          # (the target from the DSN without the password and the full plan:
                                          # rows per table insert/update,
                                          # key duplicates, the content to be swept —
                                          # without a single write), --force — the import
                                          # (the target and the plan are printed before the first
                                          # write): idempotent by the PK from the dump keys
                                          # (a repeated import restores
                                          # the dump state), zones — bypassing
                                          # identity (OVERRIDING SYSTEM VALUE),
                                          # the zone/domain sequences —
                                          # setval above the maximum, the content
                                          # links are reset (contents was not
                                          # exported — the bench writes the DOM).
                                          # --publish (only with --force) —
                                          # publishing the fixture tasks straight
                                          # into q:render (no DNS/HTTP): all — the whole
                                          # dump, smoke — the smoke subsample domains;
                                          # key duplicates are skipped with a count.
                                          # Writes to the DB via postgres.dsn and (with
                                          # --publish) to Redis via redis.addr.
                                          # The bench run instruction —
                                          # §2 "Render fixtures"
  crawlerctl fixtures snapshot --dir DIR --label NAME --engine NAME
                                          # a snapshot of the run results
                                          # (T-244, the render spec §6.1 item 5):
                                          # the fixture rows from the DB (successful
                                          # renders, the DOM length/hash/nodes/scripts,
                                          # the analyzer verdicts),
                                          # the q:analyze boundaries from Redis
                                          # (the rate), with --metrics — the stage
                                          # counters (the finalization reasons,
                                          # the page time). Taken
                                          # after a run and before a repeated
                                          # import; between runs
                                          # restart the render-worker
                                          # (the counters = exactly the run)
  crawlerctl fixtures report --baseline FILE --candidate FILE
                                          # the engine comparison report
                                          # (T-244, the render spec §5.1 items 3–4)
                                          # over the snapshots: the comparison metrics
                                          # and the explicit verdict rows against
                                          # the canary admission thresholds;
                                          # --fail-exit — exit code 1 for scripts
                                          # on any "FAILED"
```

Training the content compression dictionary (spec 5.7): `--samples` is a file or a directory
with `*.html`/`*.htm` (the default `testdata/laravel-samples`; the flag can be
repeated, the samples are read up to the corpus limit of 64 MiB). The dictionary is written
atomically and wired in by the `content.zstd_dictionary` configuration; on
retraining the previous dictionary is archived into the registry of previous dictionaries
(`dict-<id>.zstd` next to the new file) — the content written with it stays
readable. An example (the dictionary — in the dev environment's data directory):

```
docker compose run --rm -v "$(pwd)/var/content:/out tools \
    go run ./cmd/crawlerctl dict train /out/dict.zst --samples testdata/laravel-samples
```

The full procedure (when to retrain, the wiring, the registry of previous
dictionaries) — [`runbooks.md`](runbooks.md) §1.

Through make: `make status`, `make domain NAME=example.crawl.test`,
`make check DOMAIN=example.com` (a live check of any domain from the host:
the primary resolver is the testbed unbound; on SERVFAIL/REFUSED (everything outside
the test zones — this is how the hermetic unbound answers) the checker escalates to
the public fallbacks `FALLBACK ?= 1.1.1.1:53,9.9.9.9:53`; therefore one
command checks both `*.crawl.test` and real domains — no `RESOLVER`
switching needed),
`make reanalyze`, `make seed`, `make test-rules`. The other commands —
`docker compose run --rm tools go run ./cmd/crawlerctl <command>`
(see `make shell`).

The domain check API (`POST /api/check-domain`, T-123) uses the same
pipeline and in dev checks real domains: compose gives the web service the public
`CRAWLER_DNS_FALLBACK_RESOLVERS`, and the checker escalates to them when unbound fails (the semantics — a "single manual
question", not a mass pipeline: dns-worker gets no fallbacks,
the crawl's hermeticity does not change).

`reanalyze` (T-074, spec 5.8): finds `subdomain_results` with a stale
`rules_version` (NULL — never analyzed) and enqueues them in batches
into `q:analyze` — the analysis will go over the stored content, the sites are not
downloaded again. `--zone` limits the zone (the name in any form,
punycode/unicode); without the flag — all zones. The rule set version is read
from `analyzer.rules_dir` at the moment of the run; the enqueue counter is printed
to stdout. The command runs on the dev environment stack (PostgreSQL/Redis).

`status` (T-079, spec 5.9): a summary for the operator — by zones (the
`domains_total`/`domains_checked` counters from the `zones` table, updated by the
monitor service, T-078, plus the live `domains` aggregate over `check_state`: pending not counting
`deleted` — the scheduler does not select deleted domains, queued, done),
the depths of the `q:dns`/`q:http`/`q:render`/`q:analyze`/`q:dead` queues (the consumer groups' backlog),
the rate and the ETA of the first cycle's completion: `(pending + queued) / rate`.
The domain rate — the average since the start of the day over the daily check counter
`crawler_counters['checks:YYYY-MM-DD']` (T-179: a live count over
`last_checked_at = current_date` was expensive — heap fetches of fresh pages;
a window shorter than 10 minutes is not used — right after
midnight the estimate jumps); the HTTP results and renders — the hourly rate buckets
`crawler_counters['results:'/'rendered:YYYY-MM-DD"T"HH24']`
(T-203: the previous and the current calendar hour are summed, a sliding
"last hour" cannot be assembled exactly from calendar buckets — the rate
is divided by the actual length of the covered 1–2 h window, the per-second estimate
is unbiased; before, live subqueries over `subdomain_results.checked_at`
degraded with heap fetches of fresh pages; a render has no timestamp of
its own — the share of `rendered` among the window's results is used). The DNS and analysis stage rates are not
measurable from the DB (no timestamps; the DNS+HTTP completion is what "domains/s" is) — their
metrics are in Prometheus (T-076/T-077). `--json` — machine-readable output ("n/a" —
null). If Redis is unavailable, the DB summary is still printed, and the command
exits with code 1.

`domain` (T-081, spec 5.11): a domain search by name — the status and the Laravel
findings in one call. The name — in any form (unicode/punycode, case,
a trailing dot, surrounding spaces): normalized by the command per spec 5.2
(`internal/idn`), punycode is stored in the DB. The data — the SQL function
`domain_info(text)` (migration 000006, T-082): the CLI and psql share one
source of truth. The human-readable output: the zone (id, unicode), the status and the state
of the domain's check, the date of the last check (`last_checked_at`), the live DNS
subdomains (the `dns_mask` decoding), the results table by subdomains
× variants (the scheme, the HTTP status, the final URL, the alias `≡`/`→`, the
DOM/HTML content, the error with its class, the check time) and the Laravel findings: the score
with the threshold, the version range, the subdomains, the rule version and the matches
with evidence fragments; weak matches (`is_laravel=false`,
the score below the threshold) — a separate section. Domains with only weak
matches have no row in `laravel_domains` and are not exposed by the
`domain_info` function (the T-082 contract). `--json` — the raw response
of the function (pretty-printed); `--content` — instead of the summary, print to stdout
the content bytes of the selected row as-is (the priority — the rendered DOM,
otherwise the first row with content in the subdomain×variant order; the summary
and the row note — to stderr, stdout is redirected to a file).
The store reader — the same PackStore with the zstd dictionary as the analyzer's
(T-071), read-only; the `var/content` directory is mounted into the
tools container too. `--json` and `--content` are incompatible. An unknown domain/zone —
a clear message saying what exactly was not found (the zone exists but
there is no domain with the label / the zone is not in the catalog), exit code 1; for `--json` —
`{"found":false,…}`; a malformed name — exit code 2 before touching the DB.
Examples: `make domain NAME=laravel-11-vite.crawl.test`,
`docker compose run --rm tools go run ./cmd/crawlerctl domain --json
example.crawl.test`, `... domain --content example.crawl.test > page.html`.

`subdomains` (T-083, spec 5.10, 13.1) — managing the catalog of checked
subdomains (the `subdomains` table; id = the `dns_mask` bit, at most 63 — bit 63 is
the sign bit, `''` — the apex with bit 0). `list` — the "bit / label / enabled" table;
`add <label>` — inserting a row with the free `max(id)+1` bit, enabled
right away, the label is a single DNS label (unicode → punycode); `disable
<label|bit>` — `enabled = false` without changing the bit or the data (the masks stay stable,
evolution without migrations; the argument is first looked up as a label, then as a bit
number; the apex cannot be disabled — it is the basis of the domain check, spec 5.4 item 1).
The DNS worker re-reads the catalog on every batch — no service restarts needed. `recheck <label> [--zone NAME]` — starting a check of the new subdomain
over the already checked domains: the zone's done domains (without `--zone` — of all zones,
`deleted` excluded) are moved in batches of 5000 from `check_state` to
`pending`, then the domains pass the regular pipeline (the scheduler's selection →
`q:dns` → `q:http`) and the DNS stage checks them with the new subdomain from
the catalog; the domains in pending/queued will be checked with it anyway, the rate
is set by the scheduler (the `q:dns` backpressure). An interrupted drain can be
repeated — the move is idempotent. The commands run on the dev environment stack
(PostgreSQL; `recheck` is interrupted by Ctrl-C with a partial outcome).

`zones` (T-084, spec 4.1, 5.3) — managing the check zones. `list` —
the "id / zone (punycode) / unicode / enabled / total / checked" table:
the `domains_total`/`domains_checked` counters are updated by the monitor service
(T-078). `enable|disable <zone>` — the `zones.is_active` flag: the scheduler
(T-027) selects domains of active zones only, so `disable`
pauses the zone's check (the domains stay pending), and `enable`
returns them into the selection without any datafix — the domains' state
was not changed. disable does not touch the domains themselves, their states and results
(spec 13.1), the zone's file intake (ingest) works as before. The zone name —
in any form, like the `incoming/<zone>` directory; a repeated enable/disable
is idempotent ("already enabled/disabled"), an unknown zone — exit code 1.

`zones sync` (T-125, stage 14) — the initial population and refresh of the
catalog from the machine-readable slice of the IANA root zone registry
(`tlds-alpha-by-domain.txt`, ~1460 delegated TLDs, IDNs — already
in punycode, like `zones.name`). New zones are added enabled
(`is_active = true` — like the ingest auto-creation on the first file,
spec 5.1: an empty zone is irrelevant to the scheduler), existing rows are
not changed at all (`is_active` is the operator's ownership, the counters — the monitor's),
a repeated run is idempotent (changes nothing). The source — `--url`
(the default is the canonical `https://data.iana.org/TLD/tlds-alpha-by-domain.txt`,
stdlib net/http with a timeout) or a pre-downloaded file `--file`
(the offline mode: production without outbound internet). `--deactivate-missing`
additionally disables the DB zones missing from the slice (retired TLDs) —
the disabled names are printed in the report; without the flag the DB zones are not touched.
The dev/testbed limitation: the scenario test zones (`crawl.test`,
`co.test`…) are not in the IANA slice — `--deactivate-missing` would disable them too
(the zone files are still accepted, but the scheduler does not select the domains
of a disabled zone). The way back — one zone per call: `crawlerctl zones
enable crawl.test` (and the other scenario zones; enable takes a single
zone) — or do not use the flag in dev; the production allowlist is empty,
there is no such problem on production records.
Parsing: `#` comment lines are skipped (the first one — the file version,
printed in the report), every line is validated by `internal/idn`
(junk — an error with the line number, no silent skips), duplicates in the file
converge. The report: the file version, read/added/already there. This is
a catalog of TLD names, not a source of domains: domains enter the system
only through ingest (`var/incoming`). An offline sync example:

```
curl -o /tmp/tlds.txt https://data.iana.org/TLD/tlds-alpha-by-domain.txt
docker compose run --rm -v /tmp/tlds.txt:/tlds.txt:ro tools \
    go run ./cmd/crawlerctl zones sync --file /tlds.txt --deactivate-missing
```

`recheck <domain> [domain …]` (T-084, spec 4.1) — force-scheduling
domains for a check ("manual tasks") without waiting for the recheck
interval: the names (in any form) are normalized per spec 5.2, the zone —
the longest suffix of the name from the zones catalog (like `domain_info`),
the found domains are moved `check_state → pending` (resetting `queued_at`)
with one batched UPDATE by the PK keys. Publishing to `q:dns` is done by the
scheduler — it owns the backpressure and the lease (T-028); a pending
domain is picked up by the T-027 selection round-robin. A domain in flight (`queued`)
is moved like a manual lease return — it will be checked again (the duplicated work
is bounded, the delivery is at-least-once, spec 3.4); an already waiting (`pending`)
domain stays in the queue. A deleted domain is not checked — a refusal
with a message (spec 13.1); the not-found names are listed in the refusal
(no zone in the catalog / no label in the zone), exit code 1 is returned if at least one
domain was not scheduled (the scheduled ones are moved — the partial
outcome is visible in the output). For a disabled zone the move is made with the note
"the domain awaits the zone being enabled". The move is idempotent, repeating the command
is safe; Ctrl-C interrupts the execution.

## 7. Debugging, metrics, the DB, the queues

Debugging (delve, port 2345 on 127.0.0.1):

```bash
make debug SERVICE=dns-worker
# then attach the IDE to 127.0.0.1:2345
```

`make debug` starts a one-off run container of the service with its static
address in crawler_net (the address plan — [`docker.md`](docker.md) §8):
stop the service's regular container first
(`docker compose … stop <service>`), otherwise the run container will not get the address.

To run a service under the debugger permanently, copy
`deploy/docker/compose.override.example.yaml` → `compose.override.yaml`.

Metrics: every service exposes `/metrics` on its port (9100–9108, web —
9107, dashboard — 9108), see `deploy/docker/prometheus/prometheus.yml`. The
Prometheus UI — after enabling the `monitoring` profile,
http://127.0.0.1:9090/targets.

The DB and the queues:

```bash
make psql         # PostgreSQL
make redis-cli    # Redis: XLEN q:dns, XINFO GROUPS q:dns etc.
# Adminer:        http://127.0.0.1:8080 (the tools profile)
# Redis Insight:  http://127.0.0.1:5540 (the tools profile)
```

The queue resilience integration tests (`test/integration/
resilience_test.go`, T-032) run in the dedicated Redis DB **15** (the dev config
allows 16 DBs; the services use db 0): the production `q:*` stream names are
created and cleaned up there by the tests, the running services do not interfere with them.

The end-to-end pipeline test (`test/integration/e2e_test.go`, T-091; spec 11.2,
criterion 11.4 item 5) runs the whole path "a file in `incoming` → ingest →
the scheduler/`q:dns` → the DNS stage → `q:http` → the HTTP stage → `subdomain_results` →
`q:analyze` → the analyzer → `laravel_findings`/`laravel_domains`" with library
calls of the components of all stages (without launching service processes) and cross-checks
the outcomes against the expectations of the `testdata/laravel-samples` samples (meta.yaml). PostgreSQL restart
resilience is checked mid-pipeline by emulation: the DB
container is shared by the stack, restarting the instance from the test is impossible — instead all of the
test's connections are cut (`pg_terminate_backend` by the `application_name` label in the DSN
of the pool): for the clients this is exactly a restart (the connections are closed, the data is alive),
the pool reconnects, the pipeline finishes the episodes, the outcomes match.

### Acceptance tests (T-094)

`test/integration/acceptance_test.go` — an explicit acceptance map over all the criteria of
the spec §11.4: one criterion — one `TestAcceptNN_*` case (the item's wording —
a comment above the case). The cases complement the neighboring files (e2e/http/resilience/
ingest/schema), not duplicate them; the coverage map:

| §11.4 | Criterion (essence) | Acceptance case |
|---|---|---|
| item 1 | add/delete + IDN zones, no unicode/punycode duplicates | `TestAccept01_IngestAddDeleteIDN` |
| item 2 | the subdomains are independent; the variants and aliases of §5.5 | `TestAccept02_SubdomainsVariantsAliases` |
| item 3 | SPA: DOM + cookies; HTML; dedup/compression; reading back | `TestAccept03_SPADomCookiesContent` |
| item 4 | analysis (versions/score) + reanalyze without re-downloading | `TestAccept04_ReanalyzeNoRefetch` |
| item 5 | selections from `laravel_domains`/`laravel_findings` | `TestAccept05_LaravelTablesQueries` |
| item 6 | zone progress updates automatically; metrics | `TestAccept06_ZoneProgressAutomatic` |
| item 7 | a daily run within the memory budget on the target server | outside the dev bench — task T-093, done ([load-measurements.md](load-measurements.md) §13) |
| item 8 | the recheck interval (6 months, config) | `TestAccept08_RecheckInterval` |
| item 9 | service restarts: no losses or "stuck" domains (lease) | `TestAccept09_RestartNoStuckDomains` |
| item 10 | a search by name in any form; an understandable "not found" | `TestAccept10_DomainSearchByName` |
| item 11 | real customer samples through the pipeline | `TestAccept11_CustomerSamplesPipeline` |

Specifics: the re-analysis (item 4) is verified with a copy of the rule set with
a never-firing probe — the set version changes, the verdicts do not;
the SPA render (item 3) goes through the real `renderworker` on the stack's Chromium,
the navigation is substituted with `http://` — the internal Caddy CA is not trusted by
the dev stack's Chromium (production renders public certificates), the live render
tests go the same way; the sample expectations (items 4, 5, 10, 11) are read
from the corpus's `meta.yaml` (`rules.LoadSamples`) — the acceptance suite keeps no
copies of the references.


## 8. Troubleshooting common problems

- **`make up` fails on a healthcheck** — `make logs SERVICE=<name>`; the first
  air build may take up to a minute (`start_period` accounts for this).
- **After `make clean` the first `make up` is slow** — the Go caches empty: `make up`
  first warms the shared build cache in tools (`make warm-cache`), then
  starts the services; if desired the modules can be pre-fetched with `make mod-download`.
- **The DB schema is in a dirty state** — a migration was interrupted halfway; the
  service fails with a clear error. Recovery: see [`migrations.md`](migrations.md).
- **Memory shortage** — the total limits are ≤ 8 GB; on weak machines lower the
  `MEM_*` values in `.env` (postgres, chromium) — compose will recreate the services.
- **bind-mount permissions** — `DEV_UID`/`DEV_GID` in `.env` must match your
  `id -u`/`id -g` (ingest writes into `var/` as you).
- **A port conflict** — the host ports are configured in `.env`
  (`PG_PORT`, `REDIS_PORT`, `PROMETHEUS_PORT`, ...), all bound to 127.0.0.1.
- **DNS does not resolve** — the apps use unbound (172.28.100.10);
  check with `docker compose exec unbound nslookup <domain> 127.0.0.1`.
  The `tools` container deliberately has the regular Docker DNS (proxy.golang.org is needed);
  new Go dependencies are downloaded via `make mod-download`.
- **A new module is not found during hot reload** — `GOFLAGS=-mod=readonly`:
  after editing `go.mod` run `make mod-download`, then the
  service rebuilds itself.

## 9. Host setup for load tests

The container parameters (set in compose): `ulimits.nofile=65535`,
`net.ipv4.ip_local_port_range=1024 65535` for the workers.

For load tests close to production conditions (one IP, thousands of connections),
apply the production server's sysctl set on the host — the calculations and rationale
are in [`os-tuning.md`](os-tuning.md):

```bash
sudo install -m 0644 deploy/sysctl/99-crawler.conf /etc/sysctl.d/
sudo sysctl --system
```

The file covers the ephemeral ports and TIME_WAIT, nofile/fs.file-max, conntrack
(the table + the hash) and the DNS UDP buffers. The instructions, verification and rollback —
[`deploy/sysctl/README.md`](../deploy/sysctl/README.md). The host kernel-level
settings affect all containers; they are not required in CI.

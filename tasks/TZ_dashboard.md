# Specification: operator web dashboard of the Domain Crawler pipeline

Version: 1.0 (a supplement to the main project spec
[`TZ_domain_crawler.md`](./TZ_domain_crawler.md) v1.1; decisions confirmed
by the customer on 2026-10-19)

---

## 1. Purpose and overview

An internal (operator-facing) web dashboard of the Domain Crawler pipeline:
a single page showing the data-processing status "at a glance" — progress,
throughput, counters, queues, finds, and service health — password-protected.

Operational problem: today such a summary can only be assembled by the
one-shot CLI command `crawlerctl status`, while the metric graphs live in
Prometheus (the monitoring profile) and require writing queries.

1. The dashboard is a project Go service (`cmd/dashboard` +
   `internal/dashboard`) that reads the pipeline state from the same
   sources as `crawlerctl status` (PostgreSQL, Redis) and probes the
   liveness of the pipeline services.
2. Access is password-only (HTTP Basic Auth; the password is stored as a
   hash, §7.2).
3. The dashboard is strictly read-only: no actions on the pipeline are
   available from it.

---

## 2. Terms

| Term | Meaning |
|---|---|
| Spec | The main project spec (`TZ_domain_crawler.md`), unless stated otherwise |
| Dashboard spec | This spec |
| Operator | The dashboard user (the crawler owner) |
| Snapshot | A single snapshot of the pipeline state: all data sections (§5) |
| Section | A part of the snapshot coming from one source (zones, queues, statuses, …) |
| Session | A signed cookie issued after successful password verification (§7.5) |

---

## 3. Constraints and scope

### 3.1. Scope (what is not included)

1. Historical graphs stay in Prometheus; the dashboard shows "now" plus
   a short history via sparklines built from its own polls (§6.4).
2. The public part (the landing page, the domain-check form) is stage 13
   of the plan (T-121…T-124); the dashboard does not overlap with it:
   a different audience, different data, a separate listener.
3. Pipeline management (enabling zones, recheck, content rotation) is
   outside the dashboard.
4. Multi-user support and roles — a single operator (§7.1).

### 3.2. Technical constraints

1. The stack is Go, stdlib, and already-present dependencies; no new
   dependencies (go.mod is unchanged). PBKDF2 is stdlib
   `crypto/pbkdf2` (added in Go 1.24; the project is on Go 1.26).
2. An offline environment: the front uses no external CDNs or fonts —
   only `go:embed` artifacts (§6.1).
3. DB load does not depend on the number of open tabs: a background loop
   builds the snapshot, the browser receives cached JSON (§4.4).
4. The memory budget is that of the other lightweight Go services:
   `mem_limit` 256m in compose (§11.1), `MemoryMax=128M` in systemd
   (§11.2); the reference point is spec §3.2 (the combined
   "Go services ≈ 1.3 GB" budget).

---

## 4. Architecture

### 4.1. Service

`cmd/dashboard` + `internal/dashboard`; started following the
`cmd/monitor` pattern (config.Load → `service.Run`, the DB pool via
OnDBPool). The working logic (OnRun) is the background snapshot-building
loop (§4.4) plus its own public `http.Server` with a graceful stop on
ctx cancellation.

### 4.2. Listeners (ports)

1. Public — `dashboard.listen` (§8.1): the dashboard pages and API. In
   the dev environment it is published on `127.0.0.1:8090`.
2. Metrics/probes — the `internal/service` scaffold on METRICS_ADDR
   `127.0.0.1:9108` (the next one after web:9107 from stage 13):
   `/livez`, `/healthz`, `/metrics`; not exposed externally (spec §9).

The precedent of "a public listener inside OnRun" is the same as the
one designed for `cmd/web` (T-121); whichever is implemented first sets
the pattern for the other.

### 4.3. Data sources (all read-only)

1. PostgreSQL: SQL aggregates identical to `crawlerctl status` (zones,
   throughput), the distribution of domain statuses, Laravel finds,
   ingest_files reports.
2. Redis: the depth of the `q:dns/q:http/q:render/q:analyze/q:dead`
   streams (XLEN; the same narrow `BackpressureGate.Len` API as monitor
   and crawlerctl).
3. `/livez` probes of the pipeline services from the `dashboard.probes`
   config (§8.1): in parallel, a 2 s timeout.

### 4.4. Snapshot building

A background loop with the `dashboard.refresh` period (§8.1): it builds
the snapshot in full and atomically replaces the previous one; the HTTP
handlers serve the latest ready snapshot. A section failure does not
take down the snapshot (§5.9).

### 4.5. Shared summary library

The summary SQL, types, and calculations identical to
`crawlerctl status` are moved into the `internal/status` package used by
both the CLI and the dashboard: a single source of truth for the meaning
of the numbers; duplicating SQL between the CLI and the dashboard is
forbidden.

---

## 5. Dashboard data

Number definitions are the same as in `crawlerctl status` and monitor
unless explicitly stated otherwise.

### 5.1. Summary (totals)

Total domains (the sum of `zones.domains_total`), checked
(`domains_checked`), the share %, in progress (pending + queued), the
first-cycle residual and the ETA of its completion, total Laravel finds.

### 5.2. Throughput

Domains/s — the average since the start of the day (`last_checked_at`;
a window ≥ 10 minutes, otherwise "n/a"); HTTP results/s and renders/s —
over the last hour (`subdomain_results.checked_at`) — since T-203 the
window is assembled from the hourly throughput buckets
`crawler_counters['results:'/'rendered:…']` (the sum of the previous
and the current calendar hour; the divisor is the actual length of the
covered window, 1–2 h); the "n/a" rules are the same as in
`crawlerctl status` (a short day window, a zero rate).

### 5.3. Zones

Per zone: the name (punycode; the unicode form is display-only), the
active flag, total/checked/%, pending/queued/done.

### 5.4. Queues

The depth of the five streams, including `q:dead`; highlighting when
the sum approaches `scheduler.queue_high_watermark` (the backpressure
threshold) and when `q:dead` is non-zero.

### 5.5. Domain statuses

The distribution of `domains` by `status`; the codes and names — per
§6.2 of the main spec (`model.Statuses`).

### 5.6. Laravel finds

Total; over the last 24 h (`laravel_domains.updated_at`); the last 10:
domain, zone, score, version range (ver_min–ver_max), updated_at.

### 5.7. Ingest

The last 10 `ingest_files` records: file, action (add/delete),
lines_total, added/existed/deleted/rejected, processed_at.

### 5.8. Service health

Per probe from `dashboard.probes`: name + up/down (unreachable or
timeout).

### 5.9. Degradation

A section error (DB, Redis, a probe) does not take down the others: the
section gets an error flag with the reason text, and the interface
shows "n/a". Until the first successful build, `/api/status` responds
503.

---

## 6. Web interface and API

### 6.1. Routes and static assets

| Route | Auth | Purpose |
|---|---|---|
| `GET /` | yes | the dashboard HTML page |
| `GET /assets/*` | yes | static assets (js/css) from `go:embed` |
| `GET /api/status` | yes | the snapshot JSON |
| `GET /healthz` | no | a PostgreSQL ping (503 on failure) |
| `GET /livez` | no | liveness |
| `GET /robots.txt` | no | `Disallow: /` |

### 6.2. `GET /api/status`

JSON: the zone/totals/throughput/rate fields — as in
`crawlerctl status --json` (via internal/status, §4.5), plus the
§5.5–5.8 sections and a map of section errors; null for unavailable
values. `Cache-Control: no-store`.

### 6.3. Methods and headers

Anything other than GET/HEAD — 405. Response headers:
`X-Content-Type-Options: nosniff`;
`Content-Security-Policy: default-src 'none'; script-src 'self';
style-src 'self'; connect-src 'self'; img-src 'self' data:`. Server
timeouts: ReadHeader 5 s, Read 10 s, Write 15 s, Idle 60 s.

### 6.4. Front-end behavior

Auto-refresh via `fetch /api/status` every ~5 s; sparklines (SVG
polyline) of the rates and queues from the poll history kept in client
memory (~120 points); 401 — a re-login prompt; "n/a" for degraded
sections.

### 6.5. Localization

The interface language is Russian (a single one).

---

## 7. Authorization and brute-force protection

### 7.1. Access scheme

A single operator: a username (`dashboard.username`) + a password. The
login is HTTP Basic (the browser stores and sends the credentials
itself); sending credentials outside TLS is allowed only in dev
(loopback, §9.1).

### 7.2. Password storage

Only a hash in `dashboard.password_hash` (§8.1); the string format:

```
pbkdf2-sha256$<iterations>$<salt base64>$<key base64>
```

- PBKDF2-HMAC-SHA256; a salt ≥ 16 bytes (`crypto/rand`); a 32-byte key.
- The iterations are read from the string; the recommended value at
  generation is 600,000 (OWASP for PBKDF2-SHA256).
- A plaintext password in the config or the environment is forbidden.

### 7.3. Secret generation

`crawlerctl dashboard-secret`: the password from an argument or stdin
(the help warns that the argument stays in the shell history); prints
the ready `password_hash` string.

### 7.4. Verification

The username and the derived key are compared with
`subtle.ConstantTimeCompare`. The KDF verification runs only when there
is no valid session (§7.5).

### 7.5. Cookie session

After successful Basic verification a cookie is issued:
`<expiry>.<HMAC-SHA256(expiry, KDF-derived key)>` — HttpOnly,
SameSite=Lax, a 24 h lifetime. The HMAC key is the KDF-derived key
itself: a password change invalidates all sessions, and no separate
server-side secret is needed. The fast cookie check takes the KDF cost
off the periodic polling.

### 7.6. Brute-force protection

Per-IP: after 5 failed verifications within a minute — refusal until
the end of the window, 429 + `Retry-After` (in-memory, following the
T-123 token-bucket pattern).

### 7.7. Mandatory credentials

Empty `username`/`password_hash` — the service does not start (a
startup error). The check is performed in `cmd/dashboard`, not in the
shared `config.Validate`: otherwise the requirement would affect every
service of the project.

---

## 8. Configuration

### 8.1. The `dashboard` section

| Field | Type | Default | Meaning |
|---|---|---|---|
| `listen` | string | `127.0.0.1:8090` | the public dashboard address |
| `refresh` | duration | `5s` | the snapshot build period (allowed range 1s–5m) |
| `username` | string | `""` | the operator login (required at startup, §7.7) |
| `password_hash` | string | `""` | the format per §7.2 (required at startup, §7.7) |
| `probes` | name→URL map | `{}` | base metric URLs of the services for `/livez` probes |

### 8.2. Environment variables

`CRAWLER_DASHBOARD_LISTEN`, `CRAWLER_DASHBOARD_REFRESH_INTERVAL`,
`CRAWLER_DASHBOARD_USERNAME`, `CRAWLER_DASHBOARD_PASSWORD_HASH`,
`CRAWLER_DASHBOARD_PROBES` (the format "name=url,name2=url2" —
following the `zone_weights` pattern).

### 8.3. Validation

`listen` — `[host]:port`; `refresh` — 1s..5m; `probes` — absolute
http(s) URLs. `username`/`password_hash` are not checked by the shared
`config.Validate` (§7.7).

---

## 9. Security

### 9.1. Network

Dev — the public port is bound to `127.0.0.1` only (compose).
Production mode — listen on loopback (or a unix socket) behind a
reverse proxy with TLS; the scheme is the same as the one fixed for the
landing page (T-124); the config sample will appear there.

### 9.2. Logs

The password, the hash, and the cookie value are never written to logs;
failed verifications — Warn with the IP address, without credentials.

### 9.3. Data and connections

The dashboard performs no writes to the DB or Redis. Outbound
connections — only PostgreSQL, Redis, and the configured probes (§4.3);
no active probes outward.

---

## 10. Own-service metrics

On the metrics port (§4.2), via the `internal/metrics` factory:

- `crawler_dashboard_snapshots_total{result=ok|error}` — a build
  counter;
- `crawler_dashboard_snapshot_seconds` — a histogram of the build
  duration.

---

## 11. Deployment

### 11.1. Compose (dev)

The `dashboard` service, the app profile: a clone of `go-common`
(SERVICE=dashboard, METRICS_PORT=9108, app-dns), the port
`127.0.0.1:${DASHBOARD_PORT:-8090}:8090`, depends_on postgres/redis
(healthy), `mem_limit` 256m; the healthcheck — `/livez` on METRICS_PORT
(the shared go-common anchor).

### 11.2. systemd (production)

`crawler-dashboard.service` following the `crawler-monitor.service`
pattern: User=crawler-dashboard, Group=crawler,
`CRAWLER_DASHBOARD_LISTEN=127.0.0.1:8090`, `MemoryMax=128M`, the
isolation of spec §9/§10.1 (NoNewPrivileges, ProtectSystem=strict,
etc.). User creation is documented alongside the others in
docs/deploy.md.

### 11.3. Prometheus

The `dashboard` job (dns_sd, port 9108) in
deploy/docker/prometheus/prometheus.yml.

---

## 12. Testing and acceptance

1. Unit tests: authorization (§7: verification, cookie, brute-force
   blocking), SQL contracts and aggregate semantics (§4.5), section
   degradation (§5.9), routes/methods/headers (§6), configuration (§8),
   secret generation (§7.3).
2. Integration test: a full snapshot on the testbed stack
   (skipUnlessIntegration).
3. Acceptance: `make test` + `make lint` green; `make up` →
   `127.0.0.1:8090` asks for a password; after login the numbers match
   `crawlerctl status`; stopping Redis — the queues show "n/a", the
   rest of the summary stays alive (§5.9).

---

## 13. Accepted decisions and rejected alternatives

1. A custom Go service instead of Grafana: zero new containers and
   dependencies, a Russian locale, full control of the composition;
   historical graphs remain with Prometheus. Rejected: Grafana would
   require a container and a DB-counter exporter, and its localization
   is limited.
2. Basic Auth + PBKDF2 (stdlib `crypto/pbkdf2`) instead of a login form
   and server-side sessions: minimal code, the browser stores the
   credentials itself; the cookie session (§7.5) takes the KDF cost off
   the periodic polling.
3. A separate `cmd/dashboard` service rather than an `/admin` section
   of the future `cmd/web`: no dependency on stage 13, a different
   audience and different data.
4. Snapshot caching by a background loop (§4.4): the DB load is fixed
   by the refresh frequency and does not depend on the number of
   clients.
5. Periodic polling instead of WebSocket/SSE: sufficient for an
   operational summary; the code and debugging are simpler.

# Technical Specification: Mass Domain Verification and Laravel Website Detection Service

Version: 1.2 (1.1 — decisions on assumptions and open questions confirmed by the customer; 1.2 — added §10.4 "Telegram notifications (CI/CD)")

---

## 1. Purpose and Overview

Build a service that:

1. Stores the list of zones and domains (about **350 million** domains) and keeps it up to date from text files arriving in a watched folder.
2. For each domain, independently checks a set of subdomains (default: empty prefix, `www`, `admin`, `crm`, `api`, `app`): DNS, HTTP/HTTPS availability, redirects.
3. Downloads and stores the content of the main page (the final DOM as displayed by the browser) and cookies.
4. Analyzes the stored content, cookies and headers and detects **Laravel** websites with a version estimate (range), a confidence score, and a list of triggered signals.
5. Runs on a **single server** (8 GB RAM, 1 external IP, NVMe with plenty of spare space) at the maximum achievable speed with minimal memory consumption.

Stack: **Go** (backend), **PostgreSQL** (metadata and results), **Redis** (queues), external file storage for content, Chromium (headless) for rendering.

---

## 2. Terms

| Term | Meaning |
|---|---|
| Zone | TLD or public suffix (`com`, `ru`, `co.uk`, `xn--p1ai`) |
| Domain | Registrable name within a zone (`example` in the `com` zone) |
| Subdomain (in service terms) | A prefix before the domain from a configurable list; empty prefix = the domain itself (apex) |
| Variant | Access scheme for the site: 0 = primary (https if it works), 1 = http if it leads to a different site than https |
| Alias | A subdomain identical to another subdomain of the same domain or redirecting to it |
| Render | Loading the page in a headless browser with JS execution and obtaining the final DOM |
| Pack | Append-only file of the external content storage |

---

## 3. Constraints and Non-Functional Requirements

### 3.1. Resources

- 1 server, **8 GB RAM**, 1 external IP, NVMe, plenty of free space.
- Everything runs on this server. Scaling to multiple servers is not required, but the interfaces between services must be network-based (queue, DB) so that scaling is not ruled out.

### 3.2. Memory budget (guideline, configurable)

| Component | Limit |
|---|---|
| PostgreSQL (`shared_buffers` ≈ 2 GB + backends) | ≈ 2.5 GB |
| Redis (`maxmemory`) | ≤ 0.5 GB |
| Unbound (cache) | ≈ 0.7 GB |
| Go services (total) | ≈ 1.3 GB |
| Chromium (render, cgroup `MemoryMax`) | ≈ 2 GB |
| Remainder — OS page cache | — |

Each service runs as a systemd unit with `MemoryMax` so that the OOM killer does not hit PostgreSQL.

### 3.3. Performance

- Throughput is not fixed: "maximum within the available resources".
- The initial pass over 350 million domains takes roughly weeks (DNS ≈ 1–2 days, HTTP ≈ 1–2 weeks). Actual figures are determined by measurements (see section 15).
- Minimum domain recheck cycle: **6 months** (configurable).

### 3.4. Reliability

- PostgreSQL is the source of truth. Losing Redis must not lead to data loss (tasks are recovered by the scheduler).
- Task delivery is at-least-once; all result writes are idempotent.
- A crash of any worker must not block the others.
- Graceful shutdown: workers finish their current tasks or return them to the queue.

### 3.5. Misc

- Minimize memory usage: streaming processing of files and responses, batched DB operations, bounded sizes for all buffers.
- Check history is **not stored**: only the latest result.

---

## 4. Architecture

```
 incoming/ ──► [ingest] ──► PostgreSQL ◄──────────────┐
                                │                       │
                         [scheduler] ──► Redis Streams  │
                                          │  │  │  │    │
                       ┌──────────────────┘  │  │  └────┤
                       ▼                     ▼  ▼       │
                 [dns-worker]        [http-worker] [render-worker]
                       │                     │          │
                       └──────► PostgreSQL ◄─┴──────────┘
                                    │
                              [analyzer] ──► laravel_* tables
                                    │
                              [monitor] ──► Prometheus / CLI

   Content (zstd, dedup) ──► pack files on NVMe + the contents table
   DNS: local Unbound
```

### 4.1. Services (separate Go processes, one repository)

| Service | Purpose |
|---|---|
| `ingest` | Ingesting files with domains (add/update/delete) |
| `scheduler` | Selecting domains for checking, enqueueing tasks, recovering "stuck" tasks, moving overdue domains back to `pending` |
| `dns-worker` | DNS check of the apex and subdomains |
| `http-worker` | HTTP fetching, redirects, alias detection, render decision |
| `render-worker` | Rendering in Chromium, obtaining DOM and cookies |
| `analyzer` | Laravel detection by rules |
| `monitor` | Aggregating per-zone progress, exporting metrics |
| `crawlerctl` | CLI: status, **domain lookup by name** (5.11), zone and subdomain management, re-analysis, manual tasks |

Each worker type scales by goroutine count and (when needed) by process count; independence and parallel processing of same-type tasks are mandatory.

### 4.2. Queue

**Redis Streams** with consumer groups (replacement with RabbitMQ behind the `Queue` interface is allowed):

| Stream | Task unit | Consumer |
|---|---|---|
| `q:dns` | a batch of domains (id + name) | dns-worker |
| `q:http` | one domain with the list of DNS-alive subdomains | http-worker |
| `q:render` | one result (domain, subdomain, variant) | render-worker |
| `q:analyze` | one result (domain, subdomain, variant) | analyzer |
| `q:dead` | tasks that exhausted their attempt count | manual triage |

Requirements:

- `MAXLEN` on each stream; the scheduler publishes new tasks only while the queue length is below the threshold (backpressure).
- Acknowledgement (`XACK`) only after the result is written to the DB; stuck messages are reclaimed via `XAUTOCLAIM`.
- Maximum attempts per task — configurable (default 3), after that `q:dead`.
- A message contains only identifiers and the minimally necessary data (the domain name).
- *Addendum 2026-10-23 (based on the T-092 measurements and the follow-up after T-139):*
  the acknowledged prefix of a stream must not accumulate in Redis memory —
  `XACK` releases the entry from the PEL but not from the stream, and the history up to `MAXLEN`
  (hundreds of MiB) overflows the Redis budget of §3.2. Mandatory
  requirement: release the acknowledged prefix (`XDEL` after `Ack`,
  or a periodic `XTRIM MINID` at the group's oldest unacknowledged entry —
  without losing unprocessed tasks). Backpressure thresholds,
  `MAXLEN`, and `q:dns` batch sizes must be aligned with the Redis budget
  based on actual entry sizes (measurement 2026-10-22: a `q:dns` entry ≈ 237 KiB
  ≈ 1000 domains; individual `q:http`/`q:analyze` entries ≈ 0.3–0.7 KiB).

---

## 5. Functional Requirements

### 5.1. Ingest (file ingestion)

**Source.** A watched folder (the path is in config). Structure:

```
incoming/
  <zone>/
    add/       *.txt   — add or update domains
    delete/    *.txt   — delete domains (registered earlier)
processed/     — successfully processed files
failed/        — files with an error (with a log report alongside)
```

- File format: UTF-8, one domain per line — the **full domain name** (`example.com`). Empty lines and comments (`#`) are allowed. Lines that are not a full domain name of the zone specified by the file path are rejected.
- The zone directory name may be in unicode or punycode; both forms are normalized to a single zone.
- A file is processed only after writing completes: upload under a temporary name (`*.tmp`, `*.part`) followed by renaming to `*.txt`, plus a check that size/mtime remain stable for N seconds. Watching modes: `fsnotify` + periodic polling as a fallback.
- Processing is streaming (line by line); memory usage does not depend on file size. Loading into the DB is done in batches (default 50,000 lines) via `COPY` into a staging table, then:
  - **add**: `INSERT … ON CONFLICT DO NOTHING`; an existing domain is not changed, except for the `deleted` status — it is returned to `new`/`pending`;
  - **delete**: the domain status is set to `deleted`; related results and content references are removed by a background task.
- Accounting: after a file is processed, a report is written (line count, added, already existed, deleted, rejected, rejection reasons).
- Rejected lines (invalid name, a domain outside the zone, a name too long) are written to the `rejected` log with the reason; the file is not considered failed because of isolated bad lines (the error share threshold is in config).
- Reprocessing the same file is safe (idempotency). The file hash is recorded in the `ingest_files` table to guard against duplicates.
- A new zone is created automatically on the first file (`is_active = true`).

### 5.2. Name normalization (punycode / IDN)

**Rule: the ASCII form (punycode, `xn--…`) is stored everywhere within the system; the unicode form is only a derivative for display.**

1. Convert to lowercase, strip whitespace and the trailing dot.
2. If there are non-ASCII characters — convert to punycode via `golang.org/x/net/idna` (the **Lookup** profile, UTS-46).
3. Labels starting with `xn--` are checked for correct decoding; invalid ones are rejected.
4. Length checks: label ≤ 63, full name ≤ 253 (in ASCII form).
5. The zone is split off from the domain by the public suffix list (`publicsuffix`), not by a simple split on dots.
6. The domain must belong to the zone specified in the file path (the zone is normalized the same way).
7. The `zones` table contains `name` (punycode) and `name_unicode` (display only). Looking up and outputting the unicode names of a domain is done by the `idna.ToUnicode` function on the fly, without a separate column in `domains`.
8. The ASCII form is always used for DNS queries, `Host` and SNI. Hosts from `Location` and `<meta refresh>` that contain unicode are converted to punycode before the request.
9. The `is_idn` bit is set in `domains.flags` if a label contains `xn--`.

### 5.3. Scheduler

- Selects domains with `check_state = pending` in batches (default 1,000–10,000), round-robin between zones (so that a large zone does not hold up the others; zone weights are optional in config).
- Moves the selected domains to `queued` with a timestamp (lease) and publishes them to `q:dns`.
- Domains in `queued` whose lease has expired (default 30 min) and that have no result are returned to `pending`.
- Once a day (the schedule is in config), moves `done` → `pending` for domains whose `last_checked_at` is older than `recheck_interval` (default 6 months; at least the configured minimum).
- Publishes new tasks only if the queue length is below the threshold.
- For the `temp_error` status — recheck after `temp_error_retry_after` (default 24 hours), no more than N times in a row; after that the domain receives its final status based on the latest check result.

### 5.4. DNS-worker

For each domain:

1. An A query (and NS if needed) for the apex.
   - **NXDOMAIN** → status `not_delegated`; subdomains are not checked (RFC 8020).
   - SERVFAIL/timeout after retries → status `temp_error`.
2. For each enabled subdomain from the `subdomains` table — A, AAAA, CNAME queries (with CNAME chain expansion).
3. **Wildcard DNS detection**: query a random label `<random>.<domain>`. If it resolves — subdomains whose addresses match the wildcard response are marked as wildcard and are no longer checked over HTTP (except the apex and `www`).
4. The result is written: `domains.dns_mask` (bit = `subdomains.id` for each resolving subdomain), `domains.status`; addresses and CNAME are passed to the `q:http` task.
5. Domain status:
   - NS/response present, but no A/AAAA for any subdomain → `delegated_no_address`;
   - addresses present → a task to `q:http`.

Implementation requirements:

- The `miekg/dns` library; an own pool of UDP sockets, TCP fallback on truncation (TC).
- The resolver is **local Unbound** (recursive, prefetch, cache ≈ 512–768 MB). Public resolvers are only a fallback (the list is in config).
- Concurrency and QPS limits are configurable; adaptive reduction as the timeout share grows.
- If the server has no IPv6 — AAAA queries are disabled by config.

### 5.5. HTTP-worker

For each DNS-alive subdomain (one task per whole domain; subdomains are fetched in parallel inside the worker):

1. Request `https://<host>/` and `http://<host>/` following redirects (up to `max_redirects`, default 8; loop detection).
2. Connect to the previously resolved IP; for redirects to other hosts — resolve via the same resolver with cache.
3. TLS: `InsecureSkipVerify = true` (the content is needed), but the certificate verification result, issuer, expiration and SAN are stored.
4. Only the main page (`/`) is requested; if there were redirects — the page at the end of the chain is taken (both HTTP redirects and `<meta http-equiv="refresh">` are honored).
5. **Variant logic (http/https):**
   - both protocols lead to the same site (the same final host and path, or the same content hash) → one record, `variant = 0`;
   - only one protocol works → one record with the corresponding `scheme`;
   - the protocols lead to **different** sites → two records: `variant = 0` (https) and `variant = 1` (http).
6. **Subdomain aliases** (`www` vs no `www`, as well as any other pairs): if the final URL of a subdomain matches the final URL of another subdomain of this domain, or the content hash matches — the record is marked with `alias_kind` (0 — independent, 1 — identical, 2 — redirect to another subdomain) and `alias_subdomain_id`; the content is stored once. Inside the worker, a domain task uses a shared "final URL → result" cache so that the same page is not downloaded twice.
7. Everything is stored: addresses, CNAME, the redirect chain, status, final URL, TLS info, response headers, cookies, response time, error code.
8. Render decision: if the page looks like an SPA shell (little visible text + a root container `#root`, `#app`, `#__next`, `#__nuxt` etc.; a `<noscript>` demanding JS; a JS redirect; `meta refresh`), a task is published to `q:render`. In all other cases the raw HTML is considered final and stored.
9. After the result is written, a task is published to `q:analyze`.

Limits and safeguards (all configurable; default values):

| Parameter | Value |
|---|---|
| connect timeout | 5 s |
| TLS handshake timeout | 5 s |
| header wait | 10 s |
| total request timeout | 20 s |
| max redirects | 8 |
| max readable body size | 2 MB |
| max stored HTML size | 1–2 MB (configurable) |
| tarpit protection | min speed 2 KB/s after the first byte |
| concurrent connections per IP | 3–4 |
| keep-alive | disabled or a very short idle |

- The response encoding is detected (header, `meta charset`, sniffing) and converted to UTF-8.
- `Accept-Encoding: gzip, br` is supported; decompression is size-limited (zip bomb protection).
- An honest `User-Agent` with contact information is used (in config).
- The minimum number of requests is made per site; `robots.txt` is not requested (main page only).
- Outbound load is limited by the global connection count and RPS; kernel tuning for a single IP (section 13.3).

### 5.6. Render-worker

- Chromium via `chromedp` (or `playwright-go`) — **one** browser, several isolated contexts (default 2–4 in parallel).
- Blocking the loading of images, fonts and media (resource saving).
- Waiting: the load event + a bounded wait for network "quiescence"; a total timeout (default 25 s).
- Result: the final DOM (`document.documentElement.outerHTML`) and **all cookies** (including JS-set ones) via CDP.
- Browser restart every N pages and when the memory threshold is exceeded.
- If the render queue is full, domains get the `render_pending` flag and are finished later; rendering must not delay the other stages.
- The final DOM replaces the raw HTML in storage; `rendered = true` is set on the result.

### 5.7. Content storage

Stored: the final HTML (as the DOM for rendered pages, raw HTML for the rest). Screenshots and external resources are **not stored**.

- Storage is **external** (not in PostgreSQL): pack files on NVMe, pack size 512 MB–1 GB, append-only.
- Compression is **zstd** with a trained dictionary (the dictionary is retrained on demand by a `crawlerctl` command).
- **Deduplication** by a 128-bit hash (e.g., based on SHA-256 of the normalized content) — for parking pages and hosting stubs.
- Metadata in PostgreSQL: `contents(hash, pack_id, offset, len, refcount)`.
- When a result is overwritten, the reference count of the old content is decremented; background **compaction** of packs when the share of live data < 50%.
- Access via the `ContentStore` interface (`Put`, `Get`, `Delete`) so that the implementation can be replaced later (e.g., with S3).
- Content is kept at least as long as analysis rules may need to be re-applied (condition: until the domain is rechecked or deleted).

### 5.8. Laravel analyzer

The analysis is **passive**: only over already downloaded content, cookies and headers. No additional requests to sites are made.

**Rules** live in YAML files (the directory is in config) and are reloaded on `SIGHUP` without a restart. A rule contains:

- `id`, a description;
- a target: a cookie name or value, a header, a regular expression over HTML, a `meta` tag;
- `weight` (0..1);
- an optional Laravel version range implied by the signal (`ver_min`, `ver_max`).

Example (illustrative; specific version boundaries are refined against the Laravel documentation when writing the rule set):

```yaml
rules:
  - id: cookie_xsrf_token
    target: cookie_name
    match: "XSRF-TOKEN"
    weight: 0.25
  - id: cookie_session_suffix
    target: cookie_name
    regex: "^[a-z0-9_\\-]+_session$"
    weight: 0.2
  - id: cookie_encrypted_payload
    target: cookie_value
    regex: "^eyJpdiI6"          # base64 of {"iv":
    weight: 0.45
  - id: meta_csrf_token
    target: html
    regex: '<meta\s+name="csrf-token"'
    weight: 0.2
  - id: vite_assets
    target: html
    regex: '/build/assets/[^"]+-[A-Za-z0-9_-]{8}\.(js|css)'
    weight: 0.15
    ver_min: "9.19"
  - id: mix_assets
    target: html
    regex: '\.(css|js)\?id=[0-9a-f]{20}'
    weight: 0.15
    ver_max: "9.x"
  - id: inertia
    target: html
    regex: 'data-page="'
    weight: 0.2
  - id: livewire
    target: html
    regex: 'wire:(id|model|click)|livewire(\.min)?\.js'
    weight: 0.3
```

**Scoring:**

- **Score** — "noisy-OR": `1 − Π(1 − wᵢ)` over the triggered rules (weak signals accumulate but never reach 100%).
- **Version** — the `[ver_min, ver_max]` range as the intersection of the ranges of the triggered rules; if the intersection is empty, a conflict is recorded and the range is marked as undetermined.
- The threshold for a domain to enter the `laravel_domains` table is in config (default `score ≥ 0.6`).
- `laravel_findings` stores **all hits** (any record where at least one rule triggered), regardless of the threshold; the `is_laravel` flag (`score ≥ threshold`) makes it possible to separate confident hits from weak ones. If no rule triggered, no row is written.
- Result: for each (domain, subdomain, variant) — score, version range, list of triggered rules and brief evidence (what exactly was found); at the domain level — an aggregate (max score, merged range, subdomain mask).

**Re-analysis:** results store `rules_version` (a hash of the rule set). A background task finds records with an outdated rules version and enqueues them into `q:analyze`; re-downloading is not required. The `crawlerctl reanalyze [--zone …]` command triggers re-analysis manually.

### 5.9. Monitoring

- Prometheus metrics (an HTTP endpoint on every service): throughput per stage, queue lengths, errors by type, render share, response time, backlog depth, worker memory usage.
- **Per-zone progress:** `monitor` updates `zones.domains_total` and `zones.domains_checked` every 30–60 seconds (in batches, without updating the counters per row).
- The CLI `crawlerctl status`: a summary of zones, queues, throughput, and the estimated time to complete the cycle.
- Visualization: the Prometheus UI and the CLI `crawlerctl status`; Grafana is not used (too resource-hungry).
- Alerts (minimum): growth of the DNS error share, queue overflow, stalled progress, disk space shortage, NVMe wear (SMART).

### 5.10. Configuration

A single YAML file + overrides via environment variables. Minimal set:

```yaml
postgres:
  dsn: "postgres://…"
  pool_size: 20
redis:
  addr: "127.0.0.1:6379"
ingest:
  dir: "/data/incoming"
  batch_size: 50000
  stable_seconds: 10
scheduler:
  recheck_interval: "4380h"        # ≈ 6 months
  batch_size: 5000
  queue_high_watermark: 100000
  lease: "30m"
  temp_error_retry_after: "24h"
dns:
  resolvers: ["127.0.0.1:53"]
  fallback_resolvers: ["1.1.1.1:53", "9.9.9.9:53"]
  concurrency: 2000
  max_qps: 10000
  ipv6: true
http:
  concurrency: 2000
  per_ip_limit: 4
  connect_timeout: "5s"
  header_timeout: "10s"
  total_timeout: "20s"
  max_redirects: 8
  max_body_bytes: 2097152
  user_agent: "…; contact: …"
render:
  contexts: 3
  timeout: "25s"
  restart_every_pages: 500
content:
  dir: "/data/content"
  pack_size: "512MB"
  max_store_bytes: 2097152
  zstd_dictionary: "/data/content/dict.zst"
analyzer:
  rules_dir: "/etc/crawler/rules"
  laravel_min_score: 0.6
subdomains:                          # initial content of the subdomains table
  - ""
  - "www"
  - "admin"
  - "crm"
  - "api"
  - "app"
monitor:
  listen: ":9100"
  zone_stats_interval: "60s"
```

The subdomain list is **configurable and extensible**: it is stored in the `subdomains` table (managed via `crawlerctl subdomains add|disable`). Adding a new subdomain (a bit in `dns_mask`) must not require a data migration; checking a new subdomain for already checked domains is started by a separate command per zone / for all domains. Maximum subdomains — 63 (an `int8` mask limitation).

### 5.11. Lookup by domain name

Purpose: given a domain name, quickly learn its status and Laravel signals.

- **Interface:** the CLI `crawlerctl domain <name>` (the `--json` flag for machine-readable output) and the SQL function `domain_info(text)` for direct queries from the DB. A separate HTTP API and web UI are not required (possible later).
- **Input:** a domain name in any form — unicode or punycode, any case, with or without a trailing dot; it is normalized per the rules of 5.2, and the zone is split off by the public suffix list.
- **Output:**
  - the zone, domain status, check state, last check date;
  - DNS-alive subdomains (`dns_mask` decoded);
  - per subdomain and variant: scheme, HTTP status, final URL, alias kind, render flag, error, check time;
  - Laravel signals: score, version range, list of triggered rules with supporting fragments, subdomains where Laravel was found (including weak hits below the threshold, marked with `is_laravel`);
  - with the `--content` flag — the stored HTML, cookies and headers (decompressed from storage).
- If the domain or zone is not found, a clear message is printed (stating exactly what was not found).
- The lookup goes by the unique key `(zone_id, label)`, without a full scan; the target response time is at most 100 ms with a warm DB.

---

## 6. Data Model (PostgreSQL)

Principles:

- All tables with `domain_id` also contain `zone_id`; partitioning by `zone_id` (LIST), large zones additionally by a hash of `id`, so that a partition does not exceed ≈ 30–50 million rows.
- A minimum of indexes; `last_checked_at` is not indexed (HOT updates, a reduced `fillfactor`).
- Field sizes are minimal (`smallint`, `int4`, `bytea` for hashes and addresses).
- Writes are batched (`COPY`, `UPDATE … FROM unnest(...)`), `synchronous_commit = off` for crawler data.

### 6.1. Main tables (DDL sketch)

```sql
CREATE TABLE zones (
    id              smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            text     NOT NULL UNIQUE,      -- punycode
    name_unicode    text,                          -- for display
    is_active       boolean  NOT NULL DEFAULT true,
    domains_total   bigint   NOT NULL DEFAULT 0,
    domains_checked bigint   NOT NULL DEFAULT 0,
    updated_at      timestamptz
);

CREATE TABLE subdomains (
    id       smallint PRIMARY KEY,                 -- the bit number in dns_mask (0..62)
    label    text     NOT NULL UNIQUE,             -- '' = apex
    enabled  boolean  NOT NULL DEFAULT true
);

CREATE SEQUENCE domain_id_seq;

CREATE TABLE domains (
    zone_id         smallint NOT NULL,
    id              bigint   NOT NULL DEFAULT nextval('domain_id_seq'),
    label           text     NOT NULL,             -- without the zone, punycode
    status          smallint NOT NULL DEFAULT 0,   -- see 6.2
    check_state     smallint NOT NULL DEFAULT 0,   -- 0 pending, 1 queued, 2 done
    flags           smallint NOT NULL DEFAULT 0,   -- bit0: is_idn, bit1: render_pending, …
    dns_mask        bigint   NOT NULL DEFAULT 0,   -- resolving subdomains
    queued_at       timestamptz,
    last_checked_at date,
    PRIMARY KEY (zone_id, id),
    UNIQUE (zone_id, label)
) PARTITION BY LIST (zone_id) WITH (fillfactor = 85);

CREATE INDEX domains_pending_idx
    ON domains (zone_id, id) WHERE check_state = 0;   -- partial index for the scheduler

CREATE TABLE subdomain_results (
    zone_id            smallint NOT NULL,
    domain_id          bigint   NOT NULL,
    subdomain_id       smallint NOT NULL,
    variant            smallint NOT NULL DEFAULT 0,   -- 0 primary, 1 http (if different)
    scheme             smallint NOT NULL,             -- 1 http, 2 https
    http_status        smallint,
    final_url          text,
    alias_kind         smallint NOT NULL DEFAULT 0,   -- 0 none, 1 identical, 2 redirect
    alias_subdomain_id smallint,
    addrs              bytea,                         -- packed A/AAAA
    cname              text,
    tls                jsonb,
    redirects          jsonb,
    headers            jsonb,
    cookies            jsonb,
    rendered           boolean  NOT NULL DEFAULT false,
    content_hash       bytea,                         -- 16 bytes, a reference to contents
    error              smallint,
    response_ms        integer,
    checked_at         timestamptz NOT NULL,
    rules_version      integer,
    PRIMARY KEY (zone_id, domain_id, subdomain_id, variant)
) PARTITION BY LIST (zone_id);

CREATE TABLE contents (
    hash     bytea    PRIMARY KEY,                    -- 16 bytes
    pack_id  integer  NOT NULL,
    off      bigint   NOT NULL,
    len      integer  NOT NULL,
    raw_len  integer  NOT NULL,
    refcount integer  NOT NULL DEFAULT 1
);

CREATE TABLE laravel_domains (
    zone_id        smallint NOT NULL,
    domain_id      bigint   NOT NULL,
    score          real     NOT NULL,
    ver_min        text,
    ver_max        text,
    found_on_mask  bigint   NOT NULL,                 -- subdomains where Laravel was found
    rules_version  integer  NOT NULL,
    updated_at     timestamptz NOT NULL,
    PRIMARY KEY (zone_id, domain_id)
) PARTITION BY LIST (zone_id);

CREATE TABLE laravel_findings (
    zone_id       smallint NOT NULL,
    domain_id     bigint   NOT NULL,
    subdomain_id  smallint NOT NULL,
    variant       smallint NOT NULL DEFAULT 0,
    score         real     NOT NULL,
    ver_min       text,
    ver_max       text,
    signals       jsonb    NOT NULL,                  -- [{rule, weight, evidence}, …]
    is_laravel    boolean  NOT NULL,                  -- score >= the config threshold
    PRIMARY KEY (zone_id, domain_id, subdomain_id, variant)
) PARTITION BY LIST (zone_id);

CREATE TABLE ingest_files (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    zone_id     smallint,
    action      smallint,                             -- 1 add, 2 delete
    path        text,
    sha256      bytea UNIQUE,
    lines_total bigint, added bigint, existed bigint, deleted bigint, rejected bigint,
    processed_at timestamptz
);
```

Schema refinements during implementation are allowed as long as the listed principles and fields are preserved.

### 6.2. Domain statuses (`domains.status`)

| Code | Status | Meaning |
|---|---|---|
| 0 | `new` | not checked yet |
| 1 | `not_delegated` | apex NXDOMAIN |
| 2 | `delegated_no_address` | delegated, but no subdomain has A/AAAA |
| 3 | `no_http` | addresses exist, but the web does not respond over http or https |
| 4 | `alive` | at least one subdomain responded over HTTP(S) |
| 5 | `temp_error` | a temporary error (SERVFAIL, timeouts), a retry is required |
| 6 | `deleted` | removed from the registered list (by a deletion file) |

Additional statuses are allowed (e.g., `parked` based on stub signals) as long as they do not break the listed ones.

### 6.3. PostgreSQL requirements

- Parameters for NVMe: `random_page_cost ≈ 1.1`, `effective_io_concurrency ≥ 200`.
- Configured autovacuum for the `domains` and `subdomain_results` partitions (frequent updates).
- Connections through the `pgx` pool (≤ 20–50 connections in total).
- Data backup is **not required** (the data is recoverable by re-checking). The DB schema and migrations are kept in the repository.

---

## 7. Domain Check Algorithm (summary)

```
domain pending
 └─ dns-worker
     ├─ apex NXDOMAIN → status=not_delegated, done
     ├─ SERVFAIL/timeout → status=temp_error, retry later
     ├─ wildcard detection
     ├─ A/AAAA/CNAME for subdomains → dns_mask
     ├─ no addresses → status=delegated_no_address, done
     └─ addresses present → q:http
 └─ http-worker (parallel over subdomains, a shared final URL cache)
     ├─ https and http, redirects, limits
     ├─ variant 0/1 (only if different sites)
     ├─ alias_kind (www ≡ no www, etc.)
     ├─ no response from anyone → status=no_http, done
     ├─ SPA shell → q:render
     ├─ content (zstd, dedup) and result stored
     └─ q:analyze
 └─ render-worker → DOM + cookies → result updated → q:analyze
 └─ analyzer → laravel_findings / laravel_domains
domain done, last_checked_at = today
```

The stages are independent: a rendering or analysis delay does not block DNS and HTTP; a domain counts as `done` after the DNS/HTTP stage completes, while render and analysis results are appended to it asynchronously.

---

## 8. Error Handling and Special Cases

- Tarpits, infinite redirects, huge responses, zip bombs — aborted per the limits of 5.5; the error code is recorded.
- Invalid certificates — do not prevent fetching; the certificate data is stored.
- Wildcard DNS — see 5.4.
- Parking pages and hosting stubs — natural deduplication by hash; optionally a flag by rules.
- A service crash in the middle of a batch — stuck tasks are recovered via lease/`XAUTOCLAIM`.
- A domain deleted by a file during its check: the result is not written (a status check at write time) or is removed by background cleanup.
- Disk overflow: services pause content writing and raise an alert about it; the threshold is configurable.

---

## 9. Security and Ethics

- All outgoing requests go out with an identifiable `User-Agent` and contact information.
- The load on a single host/IP is limited (section 5.5).
- The analyzer is passive; there is no active probing (paths like `/.env`, `/_ignition`, etc.).
- Rendering runs in an isolated Chromium profile with no state kept between pages, under a separate unprivileged user, with network and resource limits.
- Page content is treated as untrusted: all parsers have size and time limits.

---

## 10. Deployment and Tuning

### 10.1. Environment

- Ubuntu Server (LTS), a systemd unit per service (`MemoryMax`, `Restart=on-failure`, separate users).
- Postgres, Redis, Unbound — OS packages; Go services — static binaries; Chromium — a separate package/directory.

### 10.2. Network tuning (single IP)

- Increase the ephemeral port range (`net.ipv4.ip_local_port_range`), `tcp_tw_reuse`, the `nofile` limits.
- Configure `nf_conntrack` (enlarge the table or exclude the crawler traffic).
- Configure the UDP buffer sizes for DNS.

### 10.3. Disk

- Separate directories/volumes for PostgreSQL and content pack files.
- SMART and NVMe wear (TBW) monitoring, since the write load is high.

### 10.4. Telegram notifications (CI/CD)

For GitHub Actions pipeline events, the project sends notifications **via a Telegram bot to a dedicated group**. The pipeline itself sends them (a notification step in the workflow); no separate resident service or Go code changes are required. Notifications cover CI/CD only; the runtime alerts of the service remain the mechanism of §5.9 (Prometheus).

Events:

| Event | Send condition |
|---|---|
| Code error | the lint step (`golangci-lint`) or the build step (images/binaries) failed |
| Test error | unit tests, rule tests against the sample corpus, or integration tests failed in GitHub Actions |
| Successful deploy | the deploy workflow ran to completion: server installation and the healthcheck passed |
| Failed deploy | any deploy step failed, including the healthcheck with automatic rollback |

Message requirements:

- contents: the event type, the workflow and step, the offending commit (short SHA and the first line of its message), a link to the run; for deploys — additionally the version (`git describe`) and whether a rollback happened;
- a result marker at the start of the message (success/failure) so that the group can be skimmed;
- one event — one message, no duplicates or intermediate statuses;
- auxiliary and manual runs (e.g., mutation) do not produce notifications.

GitHub Actions repository secrets: `TELEGRAM_BOT_TOKEN` (the bot token), `TELEGRAM_CHAT_ID` (the group id). Secret values never appear in run logs or in notifications.

Reliability: a notification delivery failure does not affect the pipeline outcome — the notification step runs at the end of the run regardless of its status, and its own failure does not turn the run red and is only recorded in the log.

---

## 11. Testing and Acceptance

### 11.1. Unit tests

- Name normalization: mixed case, `ß`/`ς`, emoji domains, zero-width characters, already encoded `xn--`, invalid `xn--`, long labels, composite zones (`co.uk`), unicode names of zone directories.
- File parsing: empty lines, comments, CRLF, BOM, duplicates, lines outside the zone.
- The http/https variant logic and www/no-www aliases (including `bare → www`, `www → bare`, http → https redirects, and http leading to a different site).
- The rules engine: score, version range intersection, conflicts; a run over the corpus of the customer's real Laravel samples (`/testdata/laravel-samples`, with expected `is_laravel` and version ranges) and over negative samples.

### 11.2. Integration tests

- Testbed: local DNS and HTTP servers emulating NXDOMAIN, wildcard, a tarpit, an infinite redirect, a giant body, an SPA page, Laravel pages of different versions.
- An end-to-end run: a file in `incoming` → DB rows → results → `laravel_*`.
- Resilience: a worker stopped mid-batch, Redis loss, a Postgres restart.

### 11.3. Load tests and measurements

- Measuring real performance on the target server: DNS QPS, HTTP RPS, render speed, memory usage per service, DB and storage write speed.
- Based on the measurements, the default parameter values and the estimated duration of the first cycle are refined.

### 11.4. Acceptance criteria

1. add/delete files from the watched folder are applied correctly, including for IDN zones and domains (no duplicate zones in unicode/punycode forms).
2. The check of each subdomain runs independently; the results follow the variant and alias rules (section 5.5).
3. For SPA pages the final DOM and cookies are stored; for the rest — HTML; deduplication and compression work; the content can be read back.
4. The analyzer detects Laravel on a test set of pages of different versions with the expected version ranges; the list of triggered signals and the score are stored; changing the rules + `reanalyze` updates the results without re-downloading.
5. The `laravel_domains` / `laravel_findings` tables allow retrieving: the list of domains with Laravel, subdomains, the version (range), signals.
6. Per-zone progress (total / checked) is displayed and updated automatically; the metrics are available in Prometheus.
7. The service runs on the target server within the memory budget (section 3.2) without triggering the OOM killer during a daily run.
8. A domain recheck happens no more often than the configured interval (default 6 months); the value is changed via config.
9. Stopping/restarting any service does not lead to data loss or "stuck" domains (within the lease time).
10. The lookup by name (`crawlerctl domain`) for a domain in any written form (unicode/punycode) returns the status, per-subdomain results, and Laravel signals (if any); for an unknown domain a clear message is printed.
11. The analyzer on real Laravel samples provided by the customer gives the expected hits and version ranges; all hits go into `laravel_findings`, and only those with a score at or above the threshold go into `laravel_domains`.

---

## 12. Development Stages

| Stage | Content |
|---|---|
| 1. Foundation | Repository, config, DB schema and migrations, partitioning, name normalization (IDN), CLI skeleton |
| 2. Ingest | Folder watching, streaming parsing, `COPY` and add/delete, reports, IDN tests |
| 3. Queues and scheduler | Redis Streams, scheduler, lease/recovery, backpressure |
| 4. DNS stage | Unbound, `dns-worker`, wildcard detection, statuses, `dns_mask` |
| 5. HTTP stage | `http-worker`, redirects, variants, aliases, limits, result writing |
| 6. Content storage | Packs, the zstd dictionary, dedup, compaction, the `ContentStore` interface |
| 7. Render | `render-worker`, the SPA heuristic, cookies, Chromium memory management |
| 8. Analyzer | The rules engine, Laravel YAML rules based on the customer's real samples, `laravel_*`, re-analysis |
| 9. Monitoring and lookup | Metrics, `monitor`, per-zone counters, `crawlerctl status`, `crawlerctl domain` (lookup by name), alerts |
| 10. Tuning and acceptance | Load measurements, kernel/Postgres tuning, acceptance tests |

---

## 13. Accepted Decisions and Open Questions

### 13.1. Decisions confirmed by the customer

- Input files contain **full domain names**; the `incoming/<zone>/add|delete/*.txt` structure is confirmed.
- The set of domain statuses (7 items, section 6.2) is confirmed; there is no separate "parked" status.
- The fetch limits (section 5.5) are confirmed.
- A deleted domain stays in the DB with the `deleted` status; results and content are removed by a background task.
- For rendered pages only the final DOM is stored.
- Checking a new subdomain for already checked domains is started by a separate command.
- The queue is Redis Streams.
- Laravel: the `laravel_domains` threshold is `score ≥ 0.6`; `laravel_findings` stores **all** hits.
- Real Laravel page samples will be provided by the customer (used for writing rules and tests).
- Data access: lookup by domain name (status and signals), section 5.11; a search API and UI are not required.
- Backup is not required.
- CI/CD notifications (code and test errors in GitHub Actions, deploy outcomes) are sent by a Telegram bot to the project group (section 10.4).

### 13.2. Left to the development team

- The SPA shell detection heuristic, the pack file format and compression parameters.
- Specific default configuration values — refined by load measurements (section 11.3).

### 13.3. Open questions

- Format and timing for delivering the Laravel samples: the recommended format is a folder per sample with the final HTML, headers, cookies, and a file with the expected result (the version or range, if known); see `TZ_docker_dev_environment.md`, item 4.4.2.
- It is desirable to also receive **non-Laravel** page samples (including pages on other frameworks with similar signals) to check for false positives.

---

## Appendix A. Repository Structure (recommendation)

```
/cmd/
  ingest/  scheduler/  dns-worker/  http-worker/  render-worker/
  analyzer/  monitor/  crawlerctl/
/internal/
  config/  db/  queue/  idn/  dnsclient/  httpfetch/  render/
  content/  rules/  metrics/  model/
/migrations/
/rules/                 # Laravel YAML rules
/deploy/                # systemd units, Postgres/Redis/Unbound configs, sysctl
/testdata/laravel-samples/   # real Laravel page samples (provided by the customer)
/docs/
```

## Appendix B. Recommended Go Libraries

`github.com/jackc/pgx/v5`, `github.com/redis/go-redis/v9`, `github.com/miekg/dns`, `golang.org/x/net/idna`, `golang.org/x/net/publicsuffix`, `github.com/chromedp/chromedp`, `github.com/klauspost/compress/zstd`, `github.com/fsnotify/fsnotify`, `github.com/prometheus/client_golang`, `gopkg.in/yaml.v3`.

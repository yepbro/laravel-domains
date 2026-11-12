# Render: Chromium managed by render-worker (spec 5.6, §9)

The `internal/render` package (T-061) manages **one** Chromium per worker
and renders pages in parallel in isolated contexts. Resource blocking
(T-062), waiting for page readiness (T-063), and capturing the final
DOM/cookies/headers (T-064) are described below; browser restart by page
count and memory (T-065) — Manager; the queue cycle — T-066 (publishing to
q:render on SPA, the `render_pending` flag) and T-067 (consuming
q:render — content replacement, the next section); the consumer calls
`Manager.Run`.

## One browser, two modes

`render.StartBrowser(ctx, opts)` — one process/connection per worker
(spec 5.6 "one browser"):

| Mode | Condition | Allocator |
|---|---|---|
| external Chromium | `render.remote_url` set (`ws://host:9222`) | own allocator on top of `chromedp/remote` (remotealloc.go — deterministic socket closing, T-218) |
| own process | `render.remote_url` empty | `chromedp.NewExecAllocator` + the headless flag set and `--no-sandbox` |

The external mode is the dev stack: the `chromium` compose service
(`chromedp/headless-shell`, `mem_limit` 2 GB ≈ the spec 3.2 budget, CDP at
`172.28.100.14:9222`). The own-process mode is the production render image
(`deploy/docker/Dockerfile.render`: headless-shell + the worker binary,
`USER nobody` — a dedicated unprivileged user per spec §9). The browser
session profile is service-only (headless-shell without a user directory):
task pages are not rendered in it.

The `ctx` of the starting call is the worker lifecycle context: in chromedp
the browser lives as long as the first call's context; canceling ctx stops
both the connection/process and any unfinished initialization.

### Render engines: chromium / moli / lightpanda (stage 35)

The `render.engine` field (the render engines spec, FR-1) selects the engine:
`chromium` is the default and reproduces the behavior above pixel-for-pixel;
`moli` and `lightpanda` are external CDP servers — the engine container is
brought up by the infrastructure (dev stack — the `render-engines` profile,
see [`dev-environment.md`](dev-environment.md)), and the worker only
connects. Engine specifics are isolated at two boundaries (FR-4): config
assembly → `render.Options` (the `Engine` field + validation) and
the remote allocator; the package core — Session/Pool/Manager/extract/block/wait —
does not distinguish engines and has no branching on them.

Validation (of both the configuration and `Options`) checks the combination
"engine × `remote_url` × `restart_memory_bytes`" (FR-3): for
moli|lightpanda `remote_url` is required (the browser's own process —
chromium only), its scheme is `ws`/`wss` (the address goes straight into
the WebSocket dial), and `restart_memory_bytes` must be 0 — the RSS of a
foreign process is unavailable, same as today in the dev stack's remote mode.

The allocator address mode depends on the engine (behavior confirmed on the
pinned stack images: moli v1.1.21, lightpanda 1.0.2):

| Engine | Connection |
|---|---|
| chromium | resolve the host to an IP + refinement via `/json/version`, the advertised `webSocketDebuggerUrl` as is (as before stage 35) |
| lightpanda | the configuration address as is (an analog of `remote.NoModifyURL`): the engine's browser WS lives at the root path |
| moli | the path `/devtools/browser/<uuid>` — from `/json/version` (the uuid is regenerated on every process start and cannot be set in the config), the authority (host:port) — from the configuration address |

The authority substitution for moli is needed because the engine builds its
advertisement from the bind address (it has no advertise-host flag, unlike
lightpanda), and when bound to 0.0.0.0/127.0.0.1 the advertised address is
unreachable from outside the container. The worker health-check does not
change: all three engines support `/json/version`.

## Isolated contexts and no state persistence

`Browser.NewSession` — an isolated CDP context
(`Target.createBrowserContext`, the chromedp option `WithNewBrowserContext`):
an incognito profile with **its own** cookies, cache, and storage (localStorage,
IndexedDB, service workers) and a single `about:blank` tab.

A session lives for exactly one task: after the page the pool closes it
(`Target.disposeBrowserContext`), and the next task gets a fresh profile.
Recreation instead of selective cleanup: dispose discards all profile storage
atomically and without enumerating origins (cleanup would require
`Storage.clearDataForOrigin` per encountered origin and would not cover
the cache), while the cost — one `createBrowserContext` + `createTarget`,
is negligible against a page timeout measured in seconds. The spec §9
requirement of "no state persistence between pages" is satisfied by
construction; isolation is confirmed by a live test (a cookie placed in one
session's profile is not visible in the next one).

## Pool

`render.NewPool(browser)` — parallelism of `render.contexts` (the spec 5.6
recommendation: 2–4): at most N tasks at a time, each in a fresh
isolated profile. `Pool.Run(ctx, task)`:

- waits for a free slot (or ctx cancellation / pool closure);
- one attempt = a new session → the task → profile closure on any outcome;
- on a `context`-class error — one retry on a new context;
- a successful page increments `Browser.Pages()` — the counter for the
  "every N pages" restart (`render.restart_every_pages`, spec 5.6);
  generation switching — Manager (T-065, below).

`Pool.Close` — graceful shutdown per spec 3.4: new tasks are rejected
(`ErrClosed` → redelivery at the queue level), the current ones finish
(each bounded by `render.timeout`), then the browser is stopped.

The task and the result — `RenderTask{URL}` and `RenderOutput`: the final URL
(including JS redirects the HTTP stage does not see), `Title`,
the final `DOM`, `Cookies`, and `ResponseHeaders` (T-064, below). The task
URL is validated before navigation (`validateTaskURL`, R-09 — http/https
only); when `http.block_internal_addrs` is enabled, tasks with internal
hosts are rejected at the start of `Run` (the SSRF perimeter of T-219, the
task class: IP literals of any form — including IPv4-in-IPv6 and the numeric
`127.1`/`2130706433`, which Chromium canonicalizes — and loopback names;
`renderHostInternal`). The same check powers the Chromium request
interception (`blockInternal`): when the flag is enabled, `Fetch.enable`
receives a catch-all pattern, and any session request — navigations over
HTTP/JS redirects (Document) and subresources (Script/XHR/WebSocket) — with
an internal host is rejected before it leaves for the network; the cost is
a CDP round per request, so the pattern is installed only by the flag.
Since T-230 the interception also resolves arbitrary hostnames (the
`Options.LookupHost` seam, the default — the system resolver, like Chromium
itself; a per-session cache with a 30 s TTL and a cap of 3 s per exchange —
`dnsCache`, block.go): a name that is public at the HTTP stage and internal
at render time (DNS rebinding over time) is rejected by failRequest; a
resolution error — the request passes (the NXDOMAIN of advertising
subresources is page-normal), a resolved internal address — rejection.
The residual vector: an attacker controlling a zone can tell the
interception's resolve from Chromium's own resolve (two separate DNS
exchanges) and answer them with different addresses — "public" to the
interception, "internal" to the browser. Therefore for Internet-facing
operation the Chromium container should be kept in a network egress policy
without access to internal subnets — a layered defense that the
interception complements (closing the address change between stages) but
does not replace; task hostnames are filtered by the HTTP stage with the
same funnel — docs/configuration.md, `block_internal_addrs`). The User-Agent
of pages — the single identifiable crawler UA `http.user_agent` (spec §9; the
render section of spec 5.10 has no separate field): applied to the tab via
`Emulation.setUserAgentOverride`.

## Browser restart and the memory threshold (T-065)

`render.NewManager(ctx, opts)` — the worker's Chromium lifecycle (spec 5.6:
"restart the browser every N pages and on exceeding the memory threshold").
`Manager.Run(ctx, task)` — the task entry point instead of direct `Pool.Run`
(the queue cycle — T-067): before each task a live generation
"browser + pool" is guaranteed; the generation switch triggers:

| Trigger | Condition | Spec |
|---|---|---|
| page threshold | `Browser.Pages() ≥ render.restart_every_pages` (500) | 5.6 |
| memory threshold | RSS of the Chromium process tree ≥ `render.restart_memory_bytes` (1536 MiB) | 5.6, 3.2 |
| browser death | `!Browser.Alive()`: lost connection/process | §3.4 |

**The generation FSM (T-263).** The liveness of the "browser + pool"
generation is a single predicate `Manager.genStateLocked` with six states:
absent | starting | active | draining | dead | closed. The specification and
transition table with the switch reasons and side effects (restart, drain,
logs) — `internal/render/genstate.go` (TZ-fsm spec §3 FR-9); the decisions
are made by `swapReasonLocked` and `Alive` through the predicate. The
page/memory thresholds from the table above are not states but reasons for
a scheduled replacement of a live generation. Formalization without a
behavior change: before T-263 the decision was assembled from five
independent checks inside `swapReasonLocked`.

**The generation switch does not wait for tasks.** The old pool is taken out
of service synchronously (`retire`: new tasks get `ErrClosed`), finishes its
pages (each bounded by `render.timeout`, spec 3.4) and closes its browser
from a background goroutine; the new browser starts immediately — otherwise
a memory-triggered restart would wait for the "heaviest" pages that ate the
budget. A task that never started in a closing pool (`ErrClosed` — it never
ran) and a browser that died mid-task (`ClassBrowser`) — a generation switch
and a task retry, at most two per task: the Chromium crash loop must not
multiply executions of a single page; beyond that — redelivery at the queue
level.

**RSS — from /proc, the whole process tree.** Chromium is not one process:
the main one, renderers, GPU; each counts toward the spec 3.2 budget. The
PID of the main chromedp process is exposed explicitly (`Browser.Process()`,
the method is documented exactly for collecting process metrics); from it
the RSS of descendants is summed via `/proc/<pid>/stat` (the `rss` field,
traversal by PPid down to the leaves — `internal/render/proc.go`). The check
runs before every task, but the `/proc` traversal (listing all container
processes) is cached for 10 seconds (`rssCacheTTL`, T-229): a full pass
happens at most once per window, and the error is safe — the memory restart
is soft prophylaxis, the hard cap is held by the cgroup. The cache stores
only positive measurements and is keyed by the tree root: a generation
switch (a new Browser) reads fresh. In remote mode (dev stack) the Chromium
process lives in another container — RSS is unknown (`Browser.RSS() = -1`),
the memory threshold never fires: the hard cap there is held by the
`mem_limit` of the chromium compose service (2 GB — the spec 3.2 budget).

**Start and reconnect.** `Manager.Start()` brings up the first generation
immediately: a start error is a service startup error (exit code 1, systemd
`Restart=on-failure`), not an enumeration of delivery attempts.
`Manager.Alive()` — the worker's consumption probe (`renderworker.Options.Alive`):
the probe replaces a dead or missing generation right in the call, so after
a lost connection or a Chromium restart (the `crawler-chromium.service`
unit, a docker restart of the container) the consumption pause does not
hang forever — the next probe connects to the new generation.

**CDP disconnect: a half-closed connection (T-218, incident 02.11).** After
a Chromium recycle (the T-208 timer) workers accumulated WebSocket
connections to :9222 (up to 284 over ~6 h), `Target.createBrowserContext`
hung until `SessionStartTimeout` (11,520 WARN "render context died" in
6 h), the rate fell for ~5.5 h and by 07:25 rendering stopped; the fresh
Chromium was healthy — the workers' state was broken, the cure was
restarting the workers. The cause is in the chromedp transport: the
WebSocket read (`remote.Conn.Read`) reacts neither to the context nor to
timeouts — the library learns of a connection's death only from socket
errors, and a half-closed/stuck connection (no FIN/RST) looks alive
forever; the write side (`remote.Conn.Write`) also has no deadline — the
chromedp pumps hang in a write until TCP retransmissions run out, and the
upstream allocator does not close the socket until the pumps finish.
Three countermeasures:

- **the connection probe** `Browser.probeConn` — a live browser-level
  command (`Target.getTargets`, 5 s) before creating each session, in the
  classification of context-creation timeouts, and on page timeout:
  a failure means the browser class; the browser is marked lost (sticky —
  a half-closed socket never comes back) from the second consecutive probe
  failure (T-226: a single timeout does not distinguish a black hole from
  an overloaded Chromium, and a false death on a loaded worker costs more
  than an extra probe) or immediately — on proven transport death
  (`context.Canceled`: the browser context canceled, the socket closed via
  LostConnection); the Manager switches the generation and retries the task
  on the new connection; a hanging command with a live probe stays in the
  context class (a retry on a new context), and a page timeout with a live
  probe — in the task class (the tarpit policy of T-148). The probe is
  cheap on a live browser (milliseconds even with a slow page); recovery
  after a Chromium restart takes one task (two probes + connecting), with
  no worker restart;
- **the own remote-mode allocator** (`remotealloc.go`): after the generation
  context is canceled — chromedp's graceful tab closing (bounded by the
  library's timeouts, ≤ ~5.5 s), then a forced `Close()` of the socket
  regardless of the pump state — old connections are released within
  chromedp timeout bounds, not TCP retransmissions;
- **the initialization cap** `initBrowserSession`: chromedp bootstrap
  commands (the service tab, domains) are not bounded by the library itself —
  on a stuck but accepting Chromium the start would hang forever holding
  the manager mutex; the attempt runs with a cap of
  `DialTimeout + SessionStartTimeout`.

Reproduction — the unit test `TestManagerRecoversAfterBrowserDeath`
(`internal/render/reconnect_test.go`): a CDP "black hole" (reads hang,
writes are accepted) in place of the dead browser — assertions: no contexts
are created on the dead socket, the generation switches on its own, the
task completes on the new connection within ≤ a minute, connections do not
grow (the old socket is closed forcibly).

**Why the 1536 MiB threshold at a 2 GB limit.** The restart is soft leak
prophylaxis before the cgroup-OOM fires (`mem_limit`/`MemoryMax`,
spec 3.2/10.1). The 25 % headroom covers memory growth between checks
(a check per task, the RSS snapshot no older than the 10 s cache window,
T-229) and the tmpfs `/dev/shm`, which counts toward cgroup memory but not
process RSS. With the default 8 contexts (T-169) the headroom is not
narrowed by raising the threshold: in-flight pages keep growing RSS even
after the last check; RSS systematically sitting at the threshold is a
saturation signal of a single Chromium (the answer — a second Chromium at
:9223 and a second worker against it, deploy.md §2.2), the threshold is not
raised. Configure — `render.restart_memory_bytes` (0 — disable).

**The unprivileged user and container memory** (spec §9, §10.1):
the production image `deploy/docker/Dockerfile.render` runs under
`USER nobody`, Chromium is started by the worker with `--no-sandbox`
(mandatory in an unprivileged container; the isolation boundary is the
container itself). Resource requirements are fixed in the `runtime` compose
profile (the `render` service, `deploy/docker/compose.yaml`):
`mem_limit` ≈ 2 GB, `shm_size`, `init`, `cap_drop: ALL`,
`no-new-privileges`. systemd units with `MemoryMax`/`User=` — stage 9
(spec 10.1).

## Capturing the result (T-064)

The final stage of `Session.Run` after navigation (`extract.go`; spec 5.6
"Result: the final DOM (`document.documentElement.outerHTML`) and all
cookies (including JS-set ones) via CDP", §11.4 item 3):

| What | How | Why |
|---|---|---|
| the final DOM | `chromedp.Evaluate` of the expression `document.documentElement.outerHTML` | serialization of the live tree after JS finished painting, by the same Chrome HTML serializer that "save page" uses: the format is identical to the raw HTML — straight into storage (T-067) and into the HTML/meta rules (T-068) without transformations; `DOM.getFlattenedDocument` (a node tree with nodeIds, requiring `DOM.getOuterHTML` midway) was rejected |
| all cookies | CDP `Network.getCookies` with the final page URL | reads the cookie store of the isolated context, not `document.cookie` — httpOnly and JS-set cookies are visible. `Network.getAllCookies` was removed from the current protocol (cdproto v0.157.8 does not have it); `Network.getCookies` over the tab's session is the same store, and the explicit URL yields the "this page's cookies" set without subframe noise |
| response headers | `chromedp.NavigateResponse` (the main navigation's response from `Network.responseReceived`) | spec 5.8 — header rules: for a rendered page the final document need not match what the HTTP stage captured. Chromedp itself tracks the loader/request chain across redirects — no event subscriptions needed. The `http.Header` format — a single contract with the HTTP stage (T-058) |

`Cookies` — `[]ExtractCookie`: the record shape mirrors the HTTP stage's
cookie (`subdomain_results.cookies`, T-058: `name`/`value` — the targets of
the spec 5.8 rules, `path`/`domain`/`expires` RFC 3339 UTC/`secure`/
`http_only`/`same_site` lowercase `lax`/`strict`/`none`), the order is
deterministic (name, domain, path). `max_age` is not filled: the browser
store keeps only the absolute expiry. Cookies are captured by the final
URL; tasks are http/https only (`validateTaskURL`, R-09); a non-http final
URL is a defensive case (the cookie step is skipped), the main navigation's
response is the real network one.

## Blocking images, fonts, and media (T-062)

The report on all render pipeline optimization methods verified in
production with A/B measurements (adopted and rejected,
29.10–01.11.2026) —
[`optimizations.md`](optimizations.md).

Spec 5.6 requires not loading images, fonts, and media (resource savings).
The mechanics — intercepting the tab's requests with the CDP `Fetch` domain
(the protocol no longer has `Network.setRequestInterception`; chromedp v0.21
exposes the `fetch.RequestPaused` events). `Session.Run` enables
interception **before** navigation, in the order "subscribe to events →
`Fetch.enable` → navigate" — an event arriving before the subscription
would go unanswered and would hold the request until the page timeout.

`Fetch.enable` receives patterns for the blockable types — `Image`,
`Media`, `Font`, stage `Request`: only what will be rejected is paused
(`Fetch.failRequest` with `BlockedByClient`, the request never leaves the
browser). The other types (`Document`, `Stylesheet`, `Script`, `XHR`,
`WebSocket`, …) the browser does not pause at all: scripts and XHR must
load (SPA shells finish painting — rendering exists for the sake of JS),
and a listener failure cannot hang a page (fail-open: blocking is a
saving, not a correctness condition). A favicon requested by the page
(`link rel=icon`) arrives as `Image` and is blocked; the browser's own
`/favicon.ico` arrives as `Other` and passes (headless-shell makes no such
requests, a URL heuristic for one file is not justified). The analytics
domain patterns (T-214, the next section) are additional and obey the
fail-open rules the same way: the listener decides the fate of every
paused request.

The listener is a goroutine on the task context: it answers every
`requestPaused` and terminates on `stop` (returning from `Run`) or session
closure; `stop` waits for the goroutine to exit (no leaks, verified with
`-race`).

### CSS blocking — the `render.block_stylesheet` option (T-213)

`Stylesheet` joins the blockable types by the same mechanism (`blockTypes`
in `internal/render/block.go`) when the `render.block_stylesheet` option
(`CRAWLER_RENDER_BLOCK_STYLESHEET`) is enabled. CSS is the remaining 5–10%
of page weight bytes and a noticeable share of style/layout CPU:
per the 31.10 research, blocking CSS+images gives another −one-third of
the load time.

The default `false` is the risk: on rare sites JS reads computed styles and
media queries (`getComputedStyle`, `matchMedia`) — on unapplied styles
their logic and the final DOM could differ, and with it the Laravel
detection verdicts. Therefore the decision to enable is made by an A/B test
in production: a diff of Laravel signatures before/after on the same sample
(the criterion — the share of detection disagreements < 1%) and a rate
check (no worse than current); observation after enabling — the same stage
metrics (`/metrics` of render-worker, port 9105). The option does not touch
scripts and XHR: SPA shells still finish painting — rendering exists for
the sake of JS (spec 5.6).

### Blocking tracker domains — the `render.block_tracker_domains` option (T-214)

Web analytics (GA, GTM, Yandex Metrica, Hotjar, …) arrives as
Script/XHR/Ping requests — it cannot be cut off by type (the site's SPA
scripts must load, spec 5.6), and the losses are direct: 5–15 requests and
100–500 KB per page (source measurements, the 31.10 research) plus the CPU
of tracker timers that prolong the network "quiet" phase (T-063). Therefore
with the `render.block_tracker_domains` option
(`CRAWLER_RENDER_BLOCK_TRACKER_DOMAINS`) enabled, `Fetch.enable` receives
**additional** urlPattern patterns — one per list domain, without
`resourceType` (catches a request of any type).

The domain list is the package constant `blockTrackerDomains`
(`internal/render/block.go`), maintained: google-analytics.com,
googletagmanager.com, doubleclick.net, googleadservices.com,
googlesyndication.com, connect.facebook.net, hotjar.com, mc.yandex.ru,
mc.yandex.com, matomo.cloud, clarity.ms, mixpanel.com, fullstory.com,
amplitude.com, sentry.io. Selection principle: only domains whose sole
purpose is analytics/telemetry/ad bidding; NOT general-purpose CDNs and
not site infrastructure that useful code also loads from (deliberately
excluded: yastatic.net, googleapis.com, jsdelivr — shared statics;
www.facebook.com — share buttons and redirects on par with the pixel, only
the SDK domain connect.facebook.net was taken from facebook;
segment.io/segment.com — a customer-data platform, for a site that is
infrastructure). Self-hosted Matomo/Sentry on the site's own domain are not
caught by the list on principle — the site's own domain must not be
blocked.

Precision — the pair "broad pattern → exact verdict". A CDP urlPattern is
a glob with full matching (`base::MatchPattern`: `*` — any sequence), so
the domain is embedded into the pattern with stars:
`*google-analytics.com*` equals a URL substring and catches requests of
any type with this domain anywhere in the address. The substring is broader
than needed (a site path `/js/google-analytics.com.js` is paused too) —
therefore the fate of every paused request is decided by the exact host
check `blockTrackerURL`: the host equal to a list domain or its subdomain —
`failRequest`; otherwise (a substring in the path, userinfo, a foreign
suffix like `google-analytics.com.evil.example`) — `continueRequest`. The
fail-open invariant of T-062 holds: the unblockable is never paused at all,
and the single deliberate exception (a substring pause of a foreign
request) lives for one listener cycle, whose death is covered by the
fail-open `Fetch.disable` of the pump.

The default `true` — unlike `block_stylesheet` (T-213): trackers do not
participate in Laravel detection (the rules look for framework signatures
in the site's own content and scripts, not in analytics), the residual
risk is exotics assembling visible content via tag-manager tags; the
benefit is direct, no A/B is required. The post-enablement check — the
usual stage metrics: the render rate and success no worse than current.
The live check against the dev stack — `TestBlockLiveTrackerDomains` (run
with `--use-aliases` — for the test, tracker domains resolve via DNS
aliases to the run container, see the doc comment of `block_live_test.go`).

## Waiting for page readiness (T-063, spec 5.6)

After navigation `Session.Run` waits for readiness in two phases (the
strategy — `wait.go`, the plan `DefaultWaitPlan(render.timeout)`):

```
NavigateResponse (navigation; waits for load itself — T-064)
  → the Page.loadEventFired load event, cap = 1/2 of the timeout
  → network "quiet": a 2 s window with no new Network.requestWillBeSent,
    the phase cap = 1/5 of the timeout
  → capture the result
```

- **The load event** — the `chromedp.Events(page.LoadEventFired)`
  subscription is set before navigation (buffered from the moment of
  subscribing, events are not lost). The raw `Page.navigate` command is
  used, not `chromedp.Navigate`: the latter itself waits for the end of
  loading without a separate cap — a hanging page would eat the budget
  before the quiet phase. For an SPA, load fires before painting finishes —
  the next phase waits for the painting.
- **Network quiet** — a 2 s window with no *new* requests: every
  `Network.requestWillBeSent` (the `Network` domain is enabled on the tab)
  restarts the window; requests sent before entering the phase do not count
  toward the window. The 2 s value — the top of the recommended 1.5–2 s
  range: an SPA's XHR chain fits in ~1 s, the window takes a margin and is
  paid once per page.
- **The phase caps are soft**: a cap expired (a stuck subresource, a
  continuous analytics/polling background) → move on, the result is
  captured as is — the spec bounds "quiet" both from below and from above.
  The budget shares (1/2 and 1/5) leave ~30 % for capturing the result; the
  quiet window — a constant of the site's request rate, but no more than
  half the phase cap, otherwise with a small `render.timeout` the window
  would never be confirmed.
- **The overall timeout** `render.timeout` (25 s default, spec 5.6/5.10) —
  a wrapper over the whole visit. Its expiry is a **task error** (`task`),
  not a partial result: per spec §8 an interruption by a limit is recorded
  as an error code, there is no "partial" DOM in the schema — replacing the
  full raw HTML with an unfinished DOM (T-067) without a marker is worse
  than a retry; the raw HTML is already saved by the HTTP stage, and
  rendering is an asynchronous supplement (spec §7), a failure goes to
  redelivery by the queue (T-066).

## Error classification

All package errors are `*render.Error` with a class (`render.ClassOf`):

| Class | Cause | Reaction |
|---|---|---|
| `task` | page timeout, task context cancellation, `net::ERR_*` | redelivery — the queue level (T-066) |
| `context` | the isolated context died (the tab/renderer crashed, the profile is unresponsive) | `Pool` retries once on a new context |
| `browser` | Chromium unavailable / connection lost | `Manager` (T-065) restarts the browser and retries the task (up to two times), the pool does not retry |

Classification goes by the state of the contexts and the connection (not by
error texts): task context cancellation and timeout — by the contexts;
browser death — `LostConnection`/browser context cancellation; tab death —
its context being canceled or a live probe (a trivial `Evaluate`; chromedp
does not cancel a tab's context when it crashes). The page load error code
(`net::ERR_*`) is still available structurally — `render.LoadErrorCode`
(the `chromedp.LoadError.ErrorText` field, not text parsing); the consumers
— the policies for permanent certificate errors (T-159) and DNS failure
(T-211), the sections below.

## Queue cycle (T-067, spec 5.6/§5.7/§7)

The q:render consumer — the `internal/renderworker` package, the
`renderworker.Run` cycle in `cmd/render-worker`. The consumption stream is
a worker build parameter (T-236, FR-12): `renderworker.Options.Stream`
(the default `DefaultStream` = q:render; the separate wiring points — the
cycle read, the RecoverLoop claim, and the `crawler_queue_depth` depth
collector in `cmd/render-worker`); the render fallback worker (the fallback
spec, §4.5) — the same binary, a different build: the q:render-fallback
stream and a separate consumer group. A task — a `Result` message
(the `subdomain_results` row key, T-024); the final URL is read from the
row, the message carries no identifier bloat (spec 4.2). Processing one
delivery — the order "DB write → publish → XACK" (spec 4.2):

```
SELECT the row (final_url, content_hash)
  → Pool.Run (isolated context, DOM + cookies + headers)
  → ContentStore.Put(DOM)                          — +1 new ref
  → transaction: UPDATE the row (rendered = true, content_hash = DOM,
      the render's final_url/cookies/headers, rules_version = NULL)
      + SwapRefTx(old, zero)                       — −1 old ref
  → Publish to q:analyze (the same key; under the T-026 gate)
  → XACK (Processing, T-025)
```

- **Redelivery idempotency** (spec 3.4): overwriting without the
  `rendered` guard — a repeated render would rewrite the content and
  refcount (balance in the spirit of T-058); a DOM byte-identical to the
  stored one — `Put` and the swap are skipped.
- **The race with rechecking** (the T-030 recheck rewrites rows
  DELETE+INSERT, T-058) is closed by an optimistic UPDATE guard on
  `content_hash`: a changed row is not overwritten (0 rows — the `Put` ref
  is released, the delivery is acknowledged: the task was about the
  previous episode).
- **Aliases** (T-055) are rendered by their own final URL and get their own
  ref to the DOM: profiles are isolated, the equivalence class — an HTTP
  stage optimization (not downloading twice), not a prohibition on a row
  owning content.
- **`rules_version` is reset to NULL**: the content changed, the repeated
  q:analyze publication (§7) re-analyzes the row with fresh rules (T-068+
  will stamp the new version).
- **The `render_pending` flag** (T-066) render-worker does not touch: the
  flag's owner is the HTTP stage, whose rework pass clears the bit after
  the q:render publication; rendering changes only the result row (§7 —
  the domain is already `done`, rendering is an asynchronous supplement).
  The bit transitions (setting on the result row, clearing by the pass)
  maintain the render debt counter `crawler_counters['render_pending']`
  (T-175) in the same transaction — debt observability without counting
  over the flag mask (the gauge `crawler_render_pending_domains`, the
  `render_debt` alert); render-worker does not touch the counter either.
  The formalized specification of the "bit × debt counter" FSM (states,
  transitions, invariants) — the doc comment of
  `internal/httpworker/renderq.go` (T-258, TZ-fsm spec §3 FR-4).
- **An unavailable browser** (the `browser.Alive` probe in
  `Options.Alive`): the cycle suspends consumption — deliveries stay in
  the stream unread, not in the PEL without XACK; the browser restart —
  T-065. A Chromium start error — an OnRun error (exit code 1,
  Restart=on-failure).
- **Failures**: a render error (the task class — `net::ERR_*`) and
  `ErrStoreFull` — the delivery goes without XACK and returns via
  redelivery; `ErrTooLarge` (a DOM bloated by JS) — a permanent property of
  the page, the attempts will run out and the task will go to q:dead
  (T-025) for operator triage.
- **The timeout policy** (T-148; tarpit counting — T-261, TZ-fsm spec
  §3 FR-7): a page timeout (`render.timeout`) — a predictable tarpit, not
  a random error. While the number of **tarpit-classified deliveries of
  the episode** is below `render.timeout_attempts` — an ordinary retry
  (no XACK, the claim); on exhausting the limit the row **is finalized
  without a render**: the outcome — the raw HTML of the HTTP phase
  (`rendered = false`), a q:analyze publication, XACK — a heavy page does
  not drive the work into q:dead (only infrastructure errors remain there;
  triaging the accumulated — runbooks §2.5). The count — an
  application-level Redis set `render:tarpit:{task_id}`: every delivery
  that ended in a timeout does a SADD of a member "entry-id#delivery-number"
  (idempotent — a replay of the number after the T-222 attempt return does
  not grow the count), the 48 h TTL is refreshed by every mark, the
  decision — SCARD ≥ `render.timeout_attempts` ([redis.md](redis.md) §2).
  Before T-261 the PEL delivery number served as the threshold — mixed
  series (an infra failure between tarpits) were finalized "by attempt"
  earlier: all deliveries grow the delivery number, the set only tarpits.
  The budget is per task (the message's `task_id`; a T-238 fallback
  publication carries it over to q:render-fallback): an episode that
  exhausted its tarpits on engine A is finalized on the first tarpit of
  engine B — the second engine confirmed the page's tarpit. Losing Redis
  (command errors, a build without the counter) — degradation to the PEL
  delivery number (the pre-T-261 behavior); losing the set itself with
  Redis alive (a flush, a restart without persistence) — counting anew:
  neither degradation finalizes before the threshold; the overall
  `redis.max_attempts` limit keeps working by the delivery number as
  before. The specification of the outcome policy FSM (the decision table
  of error class × budget) — the doc comment of
  `internal/renderworker/outcomestate.go`. The share of finalizations —
  the metric `crawler_render_tarpit_finalized_total` (with the build's
  `engine` label — S-P2-4: a tarpit is engine-dependent, the label
  attributes it to the engine).
- **Permanent host certificate errors** (T-159, the T-172 extension):
  `ERR_CERT_AUTHORITY_INVALID` (the certificate is not issued by a trusted
  CA), `ERR_CERT_COMMON_NAME_INVALID` (does not cover the host name) and
  `ERR_CERT_DATE_INVALID` (expired/not yet valid — added per the T-172
  measurement: 321 first failures per day, 0 successful retries, 92%
  exhausted the attempts in q:dead; the certificate renewal window of a
  parking — days — is far wider than the retry window — 3 deliveries,
  ~half an hour) — properties of the host; retrying until the host itself
  replaces the certificate is pointless. The cert class is recognized
  structurally — the `net::ERR_*` code is read from `chromedp.LoadError`
  (the ErrorText field, the `render.LoadErrorCode` wrapper), not by parsing
  the error text — and is finalized by the same mechanics as tarpits but
  **from the very first delivery**: the tarpit limit gives a page a chance
  under changed conditions, a certificate has none. The row's outcome — the
  raw HTML of the HTTP phase (`rendered = false`), a q:analyze publication,
  XACK: cert hosts do not burn delivery attempts (the §12 measurement:
  ~27% of render attempts are cert errors, up to 4 deliveries per host
  before q:dead) and do not drive the work into q:dead. The
  `render_failed_at` stamp (T-210) records the impossibility until the
  content changes; the finalization does not touch the `error` column —
  it belongs to the HTTP check (ErrorClass T-058; overwriting it would lie
  about the HTTP phase); the outcome is carried by the finalization itself —
  the raw HTML remains the result (a partial result per spec §8), the row
  goes to analysis; observability — the metric
  `crawler_render_cert_finalized_total`. The marker lives until the row's
  content changes, not forever: the next recheck episode (T-030) rewrites
  the row (marker — NULL) and publishes a new q:render task — with the
  certificate fixed, the render will go through. The code list is closed
  intentionally: network codes (`ERR_CONNECTION_REFUSED` — the host may
  come up) stay retryable errors until the last delivery (the T-172 policy
  below); DNS failure — a separate class, the next item.
- **Host DNS failure** (T-211, following up on T-207):
  `ERR_NAME_NOT_RESOLVED` — the death of the host at the DNS level. The row
  got into q:render through a live HTTP episode (the HTTP phase resolved
  the name itself — otherwise there would be no episode or row), and the
  browser resolves via the system resolver — the same unbound that serves
  the DNS stage: a failure between the episode and the render means the
  host's DNS died. DNS coming back is a days-scale event, the retry window
  (3 deliveries, ~20 min in the PEL per `redis.claim_min_idle`) does not
  cover it — the same determinism argument as for `ERR_CERT_DATE_INVALID`
  (T-172: the property-change window is far wider than the retry window).
  The T-207 measurement (29.10): 252 `NAME_NOT_RESOLVED` finalizations in
  ~7 h, each eating all 3 deliveries and ~20 min of PEL. The class is
  recognized structurally (the `net::ERR_*` code from `chromedp.LoadError`,
  like the cert class) and is finalized by the same mechanics **from the
  very first delivery**, modeled on the cert class: the outcome — the raw
  HTML of the HTTP phase (`rendered = false`), a q:analyze publication,
  XACK — a dead host does not eat 3 deliveries and does not sit in the PEL.
  The `render_failed_at` marker (T-210) lives until the content changes:
  the next recheck episode (T-030) rewrites the row (marker — NULL) and
  publishes a new q:render task — a host with restored DNS will render;
  observability — the metric `crawler_render_dns_finalized_total`
  (the T-211 criterion: −2 deliveries per dead host, the counter grows at
  zero q:dead growth). `ERR_CONNECTION_REFUSED` is **not** in the class:
  connection refusal — a property of the host's service (a closed
  port/filter/a restart), recovery is observed — the T-172 measurement:
  6.6% of retries succeed — it stays retryable until the last delivery
  (the next item).
- **The last attempt × the task class** (T-172): any other `task`-class
  render error — connection refusal, a load error (a host property, not
  infrastructure) — lives the common redelivery path while the delivery
  number (the PEL counter, `ProcessingAttempt`) is below
  `redis.max_attempts` (host recovery is observed: the T-172 measurement —
  6.6% of `ERR_CONNECTION_REFUSED` retries succeed), and on the last
  delivery the row **is finalized without a render** modeled on the
  tarpits: the outcome — the honest raw HTML of the HTTP phase
  (`rendered = false`), a q:analyze publication, XACK — the work does not
  go to q:dead (the 23.10 incident: a wave of 459 deaths in 11 minutes on
  a toxic mix of .рф parkings — a mixed timeout→refused series never
  reached the T-148 tarpit finalization and burned all deliveries). The
  infrastructure class (the DB, storage, publication, the browser, ctx
  cancellation) is not covered by the finalization — the former
  `redis.max_attempts` → q:dead path; q:dead is left only to genuine
  infrastructure errors (in the spirit of T-148). The marker lives until
  the row's content changes, not forever: the next recheck episode
  (T-030) rewrites the row (marker — NULL) and publishes a new q:render
  task — a host that came up will render. Observability — the metric
  `crawler_render_task_finalized_total{reason}` (the `net::ERR_*` code,
  `timeout`, `other`).
- **The fallback publication** (T-238, the stage 35 spec §4.4,
  FR-7/8/10/14): with `render.fallback.enabled` on, engine-dependent
  candidates — the T-148 tarpit and the T-172 task class on the last
  delivery (a property of the "site × engine" pair: a page that hangs on a
  light engine may open in chromium) — are not finalized but published to
  `q:render-fallback` (`publishFallback`): the row stays without a marker
  and without q:analyze ("awaiting fallback" — the state lives in the
  queue and metrics, the spec §4.8), and the chromium worker B reworks it
  (the same binary, the spec §4.5): success — the regular path
  (`rendered = true`, a q:analyze publication), exhausted attempts —
  a finalization with the marker. The DNS class T-211 and the cert class
  T-159 never get the fallback regardless of the flag (engine-independent
  host properties, FR-7). The order — "publication → XACK" modeled on the
  finalization-to-q:analyze link: a publication failure — an error without
  a marker, the delivery returns and retries. Idempotency: a redelivery of
  an already published row does not produce a second stream entry —
  the duplicate filter by the row identifier (the row key + the episode's
  `checked_at`: deliveries of one episode read the same `checked_at`,
  an episode rewritten by the T-058 recheck writes a new one — a fresh
  episode legitimately passes the fallback again; the `content_hash` does
  not identify an episode — it would match for episodes with the same
  content; an in-process memory with capacity above the stream ceiling,
  a duplicate after a restart is allowed and absorbed by consumer B,
  FR-11); a row already rendered by the time of the duplicate is not
  published at all. The publication goes only from the q:render stream —
  worker B does not publish to the fallback (recursion forbidden, FR-10) —
  and only from the candidate engine: a build with the chromium engine
  (including an empty `render.engine`) never publishes the fallback —
  chromium is not its own fallback (step 1 of the spec §6.2 acceptance,
  item 2: enabling the fallback without changing the engine leaves
  q:render-fallback empty; the §6.4 item 1 rollback: returning the workers
  to chromium stops the publication by itself, without touching the flag —
  worker B reworks the accumulated backlog until empty). The publication
  decision — the pure function `fallbackPublishDecision` (candidate ×
  health gate × share budget, T-239, FR-9); a refusal of any input
  suspends the publication (the gate — until the engine recovers, the
  budget — until the end of the window) — the task is finalized
  today-style (the `render_failed_at` marker + q:analyze), without losses.
  **The health gate** (FR-9a, the spec §4.4 item 3): a streak of
  browser-class infra errors (ClassBrowser — the primary engine
  unavailable) since the last successful render, at
  `render.fallback.health_gate_streak` long (default 3), closes the
  publication until recovery — a successful render resets the streak (the
  `renderFailBackoff` sample: reset on success, growth on failure;
  task/context-class errors and graceful refusals without a render do not
  touch the streak — tarpits happen to a healthy engine too, and a no-op
  render never ran), a single infra error — the noise of a live flow, a
  lying-down engine produces a streak within seconds; the browser tasks
  meanwhile wait in the q:render redelivery — the class is
  infrastructural, not covered by the finalization (within the
  `redis.max_attempts` retry window; a prolonged engine outage takes them
  to q:dead for manual triage — the pre-existing T-025 behavior).
  **The share budget** (FR-9b, the spec §4.4 item 4): the share of
  publications per a window of 1000 acknowledged stage deliveries above
  the `render.fallback.share_percent` ceiling (default 30, `0` — no
  ceiling) suspends the publications of the next window: the suspended
  window accumulates no publications, closure resets the counters and the
  fallback resumes (self-return) — a storm breaker with an alert, not a
  canary criterion. The gate and budget states — the worker's in-process
  memory (a restart zeroes them: the streak rebuilds within seconds, the
  window within minutes; with several render instances the ceilings act
  per instance — today there is one instance, docs/deploy §2.2). The
  suspension events —
  `crawler_render_fallback_suspended_total{reason=gate|budget}`
  (one per latching; the material of the spec §4.6 item 3 alerts).
  Publication observability —
  `crawler_render_fallback_published_total{reason}`
  (the `taskFinalizeReason` dictionary: the `net::ERR_*` code, `timeout`,
  `other`). Consumption —
  `crawler_render_fallback_consumed_total{result}`
  (T-245, counted only by the worker B build: `rendered` — the rework
  succeeded, `finalized` — the fallback exhausted, the row closed by the
  finalization path (the `render_failed_at` marker, while the row belongs
  to the episode), `skipped` — the row slipped out from under the task
  (no row, no final URL, the UPDATE guard cut it off) — the delivery
  closed without side effects). The series of the closed dictionaries of
  suspensions `{gate,budget}` and consumption
  `{rendered,finalized,skipped}` are pre-created with zeros in NewMetrics
  (S-P1-4): the families are visible in `/metrics` from the first start —
  the budget alert does not idle in "not measured" on a quiet production,
  and the probe does not miss the first event (before the pre-creation,
  the first point of a lazy series based prev and the 0→1 delta did not
  fire); the open dictionaries (the `reason` of the `net::ERR_*` codes in
  published/finalized) have no pre-creation — the codes are not
  enumerable, the series is born by the first finalization.
  The balance check of step 4 of the spec §6.2 acceptance:
  `increase(published) = increase(consumed) + crawler_queue_depth
  {queue="q:render-fallback"} + increase(crawler_queue_dead_buried_total
  {queue="q:render-fallback"}) + increase(crawler_queue_dead_unidentified_total
  {queue="q:render-fallback"})` (the depth — the collector of the B build:
  the unread lag + the PEL; buried — browser/infrastructure-class errors on
  attempt exhaustion; unidentifiable — rows without task_id, T-260: zero in
  steady state, appearing only when a publication bypasses the package
  contract), reconciled by increments across restarts; with the queue
  drained the residual is zero;
  the formula and applicability bounds — docs/services.md.
- **The "render impossible" marker `render_failed_at`** (T-210, migration
  000017): all four finalizations above stamp the row — a row finalized
  without a render is `rendered = false` forever and before the marker was
  indistinguishable from "never tried"; the rework pass republished it
  indiscriminately (the T-207 measurement: a tail of ~180–220 thousand
  broken rows). The exception — the fallback publication (T-238, the next
  item): the tarpit and the task class do not mark the row with the
  fallback on. The pass row selection (`renderPendingRowsSQL`) excludes
  the marked ones (`render_failed_at IS NULL`) — REPEATED publications
  stop: a repeated pass over a domain whose flag survived the publication
  (a partial publication failure — the published part finalizes within
  seconds, a repeat would publish it again; a Full exit after publishing
  the neighbors of the batch; keep of unreadable content; a restart
  between the publication and the clearing).
  The first attempt of a fresh episode is legitimate and not suppressed
  (an episode rewrite resets the marker). All rows of a domain marked —
  the `render_pending` flag is cleared without publication ("draft", like
  a page that stopped being an SPA). The stamp is set before the
  q:analyze publication with optimistic guards (content — as in content
  replacement; `rendered = false` — a concurrent render closed the
  question), the value — the first finalization (`coalesce`); it is reset
  by an episode rewrite (DELETE + INSERT of the result row, T-058 —
  a fixed host will render on the next recheck episode T-030) and by a
  successful render (`renderUpdateSQL`); the invariant: marker ⇒
  `rendered = false`. Effect control (the T-210 criterion — the share of
  marked rows in the pass selection → 0; the filter by flagged domains —
  the denominator is exactly the pass candidates, separate zones — with
  the `d.zone_id` filter; a seq scan over `subdomain_results` — a rare
  ad-hoc measurement, run on Live deliberately, see T-201 on pass costs):

  ```sql
  SELECT count(*) FILTER (WHERE r.render_failed_at IS NOT NULL)::float
         / nullif(count(*), 0) AS marked_share
  FROM subdomain_results AS r
  JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id
  WHERE (d.flags & 2) <> 0
    AND r.rendered = false AND r.final_url IS NOT NULL;
  ```

The rework pass batch selection — without PK order (T-181): ORDER BY by
PK kept an index scan over `domains_default_pkey`, which, with live debt,
tramples through the drained "empty" prefix of PK order — slow-log query
#2 of 24.10 (137 executions >3 s in 2.7 h, median 44 s, max 143 s:
14.1M Rows Removed by Filter, ≈4.5 GB of buffers per call; without
ORDER BY the plan is the same — there are no statistics for the expression
`(flags & 2) <> 0`, the selectivity was estimated as 41.6/41.6M). The
domain order does not matter (the 24.10 operator decision: clearing the
flag after the publication excludes repeats); the plan shape is pinned in
`renderSelectPending` by a transaction
`SET LOCAL enable_indexscan/bitmapscan/indexonlyscan = off` (all indexed
access forms to domains are closed — index-only over PK would repeat the
pathology) — a seq scan finds the first hundred flagged rows with the
LIMIT early stop (the flagged `.рф` debt is physically at the start of the
heap: the ru/rf/su ingest went first, HOT updates keep versions on their
home pages) — 43 ms vs 30 s in the 24.10 measurement (700×). Physical
locality is an empirical fact, not a contract: if the heap layout changes,
the seq scan will get costlier — the slow-query log as the control
(T-177); the systemic alternative (a keyset cursor) is deliberately
deferred. In steady state without debt the heavy SELECT does not run at
all: a guard by a point read of `crawler_counters['render_pending']`
(T-175) — `value = 0` → an empty batch without touching `domains` (one
row read per tick). The guard's trust boundary: the bit and the counter
are maintained by the code as a pair ±1; with `value > 0` and no
selectable flags the pass pays a full seq scan per tick (a Warn "the debt
counter > 0, the selection is empty"): a desync of manual SQL or orphan
bits — deleted domains with the flag that the selection honestly does not
see (§13.1); the regular orphan source is closed by T-186 — applying a
delete clears the bit and decrements the counter in the same transaction;
the residual — the tail of deletions from before T-186 (a one-off
cleanup) and drift, absorbed by reconciliation; the cleanup —
[`runbooks.md`](runbooks.md)
§6. The drain rate is limited by the q:render gates (T-174), not by the
selection; the initial drain does not spin on a batch without progress
(unreadable content) — it waits for the next tick.

Content storage and refcount accounting — [`content.md`](content.md)
(the section "Content replacement by render"); browser live tests are not
part of the cycle (the `internal/renderworker` unit tests run on fakes
without DB/Redis; the end-to-end pipeline run on the testbed — the
integration tests `test/integration`).

The debt counter is also read by content retention (T-185): rows of
domains with the `render_pending` flag are excluded from cleanup by the
selection criterion, and the time-based retention drain cursor passes
them silently — with live debt the slice behind the cursor accumulates.
A point read of `crawler_counters['render_pending']` at the start of each
hourly retention pass catches the counter's transition to 0 — the moment
the whole render protection lifts at once — and schedules one catch-up
circle from the zero cursor: it catches up everything accumulated over
the debt era; the frequency of circles follows the facts of debt drainage,
not the clock; before the counter row deploy (migration 000015) retention
keeps a fallback circle once a day. The counter contract is not extended —
both sides only read (the rework pass guard and the retention catch-up
with the same operator).

### The late re-publisher: insurance for lost tasks (T-259)

The row render-state FSM (specification — the doc comment of
`internal/renderworker/worker.go`, TZ-fsm spec §3 FR-5: `new episode →
queued → rendered | impossible | awaiting fallback | slipped out from
under the task`; the columnless "awaiting fallback" is observable via the
balance `published = consumed + q:render-fallback remainder + buried`) has
a weak spot: the "queued" and "new episode" states are distinguishable
only outside the row — by the domain's `render_pending` bit or a stream
message. Losing a message (a MAXLEN trim of `q:render-fallback` before
delivery, T-025/T-238; a failed/lost `q:render` publication) left a row
"forever young": no bit, no messages, indistinguishable from "never
tried" — only the episode recheck would close it (months).

The late re-publisher — the background pass `httpworker.RenderLateLoop`
(T-259, FR-5.3; it lives in the http-worker next to the rework pass, once
a minute, batches of 1000 rows, a PK-based cursor — a full cycle over the
table takes on the order of a month, faster than
`scheduler.recheck_interval`): rows
`rendered = false AND final_url IS NOT NULL AND render_failed_at IS
NULL` older than 6 h, whose domain has no `render_pending` bit (the
rework pass owns the debt — duplicate publications are excluded by
dividing ownership) and no unconsumed `q:render`/`q:render-fallback`
messages (the interlock via a stream snapshot, the
`content/retentionpending.go` sample; the adapter — `retentionPending`
in `cmd/http-worker`, the same one as retention protection) — are
republished to `q:render`. The SPA verdict is re-derived from the stored
content (`DetectSPA`, like the rework pass; the age predicate is a
superset: non-SPA rows fit the columns indefinitely, publishing without
a verdict would flood the queue with rechecks of all pages); aliases
inherit the original's verdict (T-055) — from the candidate by body OR
from the already rendered original (the fact of rendering proves SPA
class: q:render publications happen only for SPAs; the content of such an
original — the DOM — does not match the body-based verdict — the T-259
review P2-1); rows without content and without an alias are skipped
(nowhere to take the verdict from — the next recheck episode will rework
them; older than the retention window there is no content — going there
is pointless). The cursor moves only past a batch processed in full:
a full queue (Full) or a publication/snapshot error leaves it in place —
the next tick repeats the batch, the duplicate is bounded and absorbed by
consumer idempotency (§3.4). A stream snapshot read error — a
conservative skip of the pass: republishing "blindly" costs more than
a tick (a duplicate of a live task is exactly what the interlock
excludes).

The batch selection runs in a tx wrapper with a server-side cap (T-271
introduced the wrapper with plan-shape pinning, T-273 removed the
pinning): a single `SET LOCAL statement_timeout = '300000ms'` before the
SELECT — a hung query is impossible, and the client ctx deadline
`SelectTimeout` covers the wrapper too. The working value is passed by
configuration (`render.late_select_timeout`, T-274; 0 — the code default
of 300 s). Pinning `enable_seqscan/hashjoin/mergejoin/
bitmapscan = off` turned out to be a regression: at production volume
(43M domains) the pin left the planner only a Nested Loop and unwound the
join from domains — cost ≈157M at a 60 s deadline, not a single successful
pass in ~21 h (the 09–10.11.2026 incident, an eternal loop of doomed
selections; before T-271 the unpinned T-251 form worked in production for
weeks). The unpinned form is adaptive (the T-273 testbed measurements,
PG 16.15, 2M domains / 550K rows): a zero cursor — a Nested Loop from
domains with row-PK probes and the Limit early stop (19 ms); the middle
of the table — a Merge Join from subdomain_results by PK (42 ms).
Returning to plan freedom does not return the OOM loop: the working
structures of any plan node are squeezed by `work_mem = 16 MB` (the
backend budget, `docs/db.md` §settings — Sort/Hash above the ceiling
spill to disk, not memory), the two heavy scans are serialized by the
`db.ScanGate` gate, and every selection is bounded by statement_timeout.
The `SET LOCAL
join_collapse_limit = 1` option was rejected by measurement: the GUC
keeps the join pairs in FROM order but does not forbid unwinding
outer/inner within a pair — the plan still unwinds from domains. The
client ctx deadline — 300 s (T-273, was 60): the deadline must give
a catch-up selection from the zero cursor a chance to finish and move
the cursor. A failed pass switches the minute ticker of the cycle to
a 15-minute backoff **counted from the end of the pass**
(T-273: reset after every failure — the pause ≥ the pass duration +
backoff, the WARN cadence does not mask the duration); success returns
the regular interval; pass failures are counted by the metric
`crawler_render_late_errors_total` (T-273) — before it, only journalctl.
The heavy selection — under the http-worker's shared scan gate
`db.ScanGate` (T-271): the late re-publisher and the retention drain do
not overlap in PostgreSQL.

Observability: `crawler_render_late_republish_total` — does not grow in
steady state; growth after an incident (a stream trim, a Redis loss) —
draining the losses, a fall back to zero — the drain is complete. Pass
failures — `crawler_render_late_errors_total` (T-273): a growing series
with zero republishes = the cycle is broken (the error text — a Warn in
journalctl). The integration tests —
`test/integration/render_late_test.go` (a lost row → publication;
a row in the stream or under the bit → untouched; the schema CHECK
invariant; the selection's plan shape at production-proportion volume —
T-273).

Since T-259 the "rendered ⇒ no marker" invariant is also held by the
schema: the CHECK
`subdomain_results_rendered_check (rendered = false OR
render_failed_at IS NULL)` — migration 000018, added `NOT VALID`
(without scanning 40+M rows; new and modified rows are checked
immediately, existing ones untouched — their invariant was always held
by the writers' SQL guards). `VALIDATE CONSTRAINT` — a separate step per
the procedure (`docs/schema.md` "CHECK invariants"): on a partitioned
PG16 parent the validation recurses over all partitions in one statement,
and new partitions of `create_zone_partitions` inherit the constraint
already validated. The paired test invariant —
`TestSchema_ResultsRenderedCheck`
(`test/integration/render_late_test.go`): a violation (rendered + marker)
is rejected by the schema, legal combinations pass, `convalidated = false`
until the per-procedure VALIDATE.

### The fallback worker B: build and consumption rate (T-240)

Worker B — consuming `q:render-fallback` with a separate
`render-fallback-worker` group (the stream's canonical pair,
`queue.WorkingGroups`) with the `render.fallback.worker_rate` rate cap
(FR-13): the same `render-worker` binary, a different build — the
`RENDER_WORKER_STREAM` switch (a launch parameter, like `METRICS_ADDR`;
the consumption stream is a worker build parameter, not configuration,
the spec §4.1 item 3). Success — the regular path
(DOM, `rendered = true`, q:analyze); an engine-dependent failure on
exhausted attempts — a finalization with the `render_failed_at` marker
(recursive fallback forbidden, FR-10: `publishFallback` does not publish
from the fallback stream).

The rate — a sliding window `renderworker.RateWindow` (1 min): the reader
of the `Run` cycle trims the batch to the window's free slots before each
read (`Options.Rate` — a generic parameter, no branching by engine/stream,
in effect when set; the main q:render build reads without a cap),
an exhausted window holds a pause until the oldest entry exits. The
exhausted-window pause and the batch trim — states of the pause-rate FSM
of reader modes (`renderworker/readerstate.go`, T-262: priority after
the circuit breaker, before reading). An empty read spends no budget —
with an empty queue the worker idles in the XREADGROUP block with the
full cap. The RecoverLoop claim is not bounded by the cap:
the rate of returning stuck deliveries — `redis.recover_*` (T-153),
a separate knob. The default of 100/min ≈ the full capacity of one
chromium (8 contexts × ~5 s/page) — the batch mode "collect and rework"
without scaling chromium capacity (the spec §3 item 3); the queue
accumulates as much as needed (the MAXLEN of T-237), calibrating the cap —
a bench run (the spec §4.4 item 4). Worker B
keeps the rate even with `render.fallback.enabled` off — the accumulated
backlog is reworked until empty (FR-14).

Deployment — modeled on the existing render workers
([`deploy.md`](deploy.md) §2.3): docker runtime — the `render-fallback`
service of the `runtime` profile (today's render image with its own
headless-shell, unchanged; the profile —
`deploy/docker/config.render-fallback.yaml`), natively — the unit
`crawler-render-fallback-worker.service` on the shared chromium
(`crawler-chromium.service`): the T-208 recycle pair applies as is —
the recycle `ExecStart` restarts exactly this shared unit, no new files
are needed.

### Zone priority of the rework pass (T-184)

Implementation of T-176 decision (a) (the 24.10 operator: render
everything, lose nothing; zones with a high DOM contribution — first,
parking `.рф` — gets the leftovers): a non-empty
`render.requeue_deprioritized_zones` (a list of zone names in punycode,
production — `xn--p1ai`; the config — the table of the `render` section in
[`configuration.md`](configuration.md)) splits the pass batch selection
into two steps WITHOUT sorting — first, domains of zones NOT in the list
(`z.name <> ALL(…)`); on exhausting the first step (the batch incomplete —
the LIMIT did not trip, the seq scan reached the end of the table) the
batch remainder is filled from the list's zones (`z.name = ANY(…)`). Both
steps — in the same plan-pinning transactions (the zone-name filter in
the JOIN with the small zones reference table does not change the shape:
a seq scan over domains with an early stop, indexed forms disabled), the
physical locality of the flagged `.рф` debt at the start of the heap
plays in favor of the second step. The debt queue loses no rows:
everything gets reworked, the list's zones last; the `q:render` gates
(T-174) meter the publications — the priority disposes of the freed
capacity. Publication of live-check SPA results (`renderSPAResults`) is
not prioritized — the same gates meter it; the stream's undelivered tail
drains FIFO as is.

An empty pass of an exhausted first step — a full seq scan over domains
(that same slow-log query #2 pathology) — the pass cycle does not repeat
it while the render debt counter matches the expectation (the value at
the moment the exhaustion was proven minus its own withdrawals).
Resumption — on any discrepancy: counter growth (a new bit set for
a zone of **any** kind, including list zones — inflow cannot be
distinguished with a single counter, and over-caution is cheaper than
lost priority; a batch of new flags between ticks is amortized into one
repeat) and a decrease beyond its own withdrawals (a second pass
instance, manual SQL — runbooks §6). The proof is also lifted without
a counter discrepancy if at least one first-step domain of the batch
stayed flagged (keep on unreadable content, an early Full/error exit):
the selection saw those rows but the pass did not rework them — skipping
the first step would clamp a stuck priority debt forever. The efficiency
boundary: with a continuous inflow of list-zone flags (the gates closed,
.рф recheck episodes) empty-first-step repeats run almost every tick —
the memory saves exactly during inflow pauses; post-deploy control —
the slow-query log (T-177) for the query `z.name <> ALL(...)`:
with regular full scans the systemic answer is a separate counter of
priority bit settings (a T-175 extension), not a weakening of
correctness. The one-off `RequeueOnce` keeps no memory — both steps run
honestly.

Observability — the `deprioritized` field of the "render rework pass"
line of the http-worker log (the batch's domains from list zones): `0`
within the drain window — the priority works, publications go with
non-list zones first; growth — the first step is exhausted, the drain
has reached the deferred zones (order control without manual SQL). After
the ru/com drain — a re-run of the `deploy/sql/t176-dom-share.sh`
measurement (to close the unknown DOM contribution of non-parking
classes); the control sample via re-analysis — the dom_only/needs_dom
candidates of the T-176 measurement, including the Livewire site
`xn--80afc7aid1a.рф` (a direct test of a false SPA verdict).

## DOM signal contribution by mix class (T-176)

A measurement on production data (2026-10-24; a series of slices
14:20–19:25 MSK, the final table — a consistent slice at ~19:20; the
pipeline is live, the render debt drainage ~0.7 tasks/s, the inflow frozen
since 24.10 10:26): the share of Laravel finds owed **exclusively** to
render DOM signals, broken down by mix class — parking `.рф`/`.su`
vs other zones. The occasion — the stage 25 render debt (~443K:
≈95K undelivered q:render messages + ~344.5K domains with the
`render_pending` flag) at a stage cap of ≈1.6 pages/s per worker
(8 contexts, T-169; 2 workers ≈11.5K tasks/h, the actual drainage under
CPU starvation ~0.7 tasks/s) and a live pipeline demand of ≈1.2 pages/s
(the §12 measurement,
[`load-measurements.md`](load-measurements.md)): if the parking classes
contribute ≈0 DOM, rendering them is a waste of slot time.

The reproducible recipe — `deploy/sql/t176-dom-share.sh` (strictly
read-only: only SELECT and a control EXPLAIN; entry via `laravel_findings`
≈91K rows with a PK JOIN to `subdomain_results` — the planner's hash join
would build a seq scan over 4.5M rows, disabled with session hints; the
only pass over `subdomain_results` itself — a sampling TABLESAMPLE SYSTEM
0.5%, reading ~0.5% of blocks, with a ×200 estimate and a binomial spread
of ~±5%; no full count over `rendered` and no scan of `domains` at all).

### Methodology

Rule classification by matching target (`target`, `internal/rules`):

| Class | Target | Rules (weight) |
|---|---|---|
| **H** — header/cookie of the HTTP phase | `cookie_name`, `cookie_value`, `header_name` | cookie_xsrf_token (0.25), cookie_laravel_session (0.35), cookie_cms_session (0.35), cookie_session_suffix (0.15), cookie_encrypted_payload (0.5), cookie_remember_guard (0.35), header_php_cli_server (0.2) — 7 |
| **C** — body/DOM content | `html`, `meta` | meta_csrf_token (0.2), form_csrf_input (0.25), vite_assets (0.2), vite_dev_client (0.1), mix_assets (0.15), inertia (0.2), livewire_attrs (0.5), livewire_script (0.25), ziggy_routes (0.35), sanctum_csrf_cookie (0.3), laravel_debugbar (0.3), window_laravel (0.4), json_message_key (0.25), welcome_classic (0.45), laravel_docs_link (0.3), pagination_aria (0.25) — 16 |

No rules are contested — `target` in `rules/laravel.yaml` is unambiguous.
The key nuance of rendered rows (T-067): render **overwrites**
`cookies` (the browser's whole jar, including httpOnly and JS-set ones)
and `headers` (the final navigation) — the analyzer (`analyzeSelectSQL`)
reads render data for a rendered row, not the original HTTP phase data.
Therefore the presence of an H signal in a rendered find ≠ "the find
would exist without render": part of the H signals may also be owed to
render (JS cookies, cookies from Sanctum XHR endpoints). The measurement
takes two observable estimates, each of which can be biased in both
directions:

- **dom_only**: `is_laravel` ∧ `rendered` ∧ not a single H rule in
  `signals` — the visible composition of the find is purely content-based.
  May overstate the debt to render (server-rendered Livewire carries
  `wire:*` in the raw body — with a false SPA verdict the signals existed
  without render) and misses finds whose H cookies were obtained by
  render;
- **needs_dom**: `is_laravel` ∧ `rendered` ∧ the noisy-OR score of the
  H signals alone < the 0.6 threshold (`analyzer.laravel_min_score`, the
  production value) — content signals are required to pass the threshold
  with the current `signals` composition. Includes dom_only (H-score = 0).
  Misses finds that became finds only thanks to render cookies (the
  H-score crossed the threshold already in the H part): their debt to
  render is hidden.

Both biases are bounded empirically (the borderline class below): the
JS-cookie/XHR-cookie class is negligible, and parking cookies are
available in the raw phase too. The estimates give the order of
magnitude, not a confidence interval.

### Results (consistent snapshot 2026-10-24 ~19:20 MSK)

`laravel_findings`: 91,565 rows, 33,807 `is_laravel`, one rules version
(−214990931). Mix classes: `.рф` = zone 6 (`xn--p1ai`), `.su` =
zone 5 — the original ingest waves with parking platforms; other zones —
the backlog corpus.

| Class | findings (all hits) | is_laravel | rendered | dom_only | needs_dom |
|---|---|---|---|---|---|
| `.рф` | 20,486 | 4,819 | 440 | **2 (0.45%)** | **11 (2.5%)** |
| `.su` | 4,766 | 1,315 | 238 | **0** | **2 (0.8%)** |
| other zones | 66,313 | 27,673 | 34 | **0** | **1 (2.9%)** |
| **total** | 91,565 | 33,807 | 712 | **2 (0.28%)** | **14 (2.0%)** |

The percentages are shares of the class's rendered finds. Of all the
class's finds: dom_only `.рф` 2/4,819 = 0.04%; needs_dom `.рф` 11/4,819 =
0.23%; the pipeline-wide total 14/33,807 = 0.04%. **The DOM contribution
of the parking classes is on the order of zero: 97.5% of the rendered
`.рф` finds (429/440) pass the 0.6 threshold on H signals alone.**

Rule frequencies in `is_laravel` finds (classification validation;
an early slice at ~14:30, 675 rendered finds; raw = rendered=false,
columns: raw/rendered): cookie_encrypted_payload 32,918/673,
cookie_xsrf_token 31,684/625, cookie_session_suffix 18,563/498,
meta_csrf_token 9,896/364, vite_assets 9,760/260, form_csrf_input
7,312/234, cookie_laravel_session 4,188/103, inertia 2,450/99,
livewire_script 2,146/65, livewire_attrs 1,908/55, ziggy_routes 911/58,
mix_assets 1,204/114, cookie_cms_session 968/33, laravel_docs_link
576/3, welcome_classic 65/3, json_message_key 79/3,
header_php_cli_server 171/10, window_laravel 138/1, laravel_debugbar
58/0, pagination_aria 45/0, vite_dev_client 20/0, cookie_remember_guard
4/0, sanctum_csrf_cookie 2/0. In rendered finds the score is almost
always carried by the encrypted cookie (673 of the slice's 675 = 99.7%).

The borderline class (rendered ∧ H signals present) is broken down by
evidence (a sample of 4 `.рф` rows): all carry the server pair
XSRF-TOKEN (the encrypted envelope eyJpdiI6…) and
`laravel_session`/`<app>_session` (809-session, nuts24_session)
— an h-score of 0.625–0.834 with no content. The proof that these
cookies are available **without** render is measured, not only shown by
a sample: 3,782 of 4,379 (86.4%) non-rendered `.рф` finds (verdict from
the raw phase, no render) have the same pair
cookie_xsrf_token + cookie_encrypted_payload in
`signals` — Set-Cookie is set by the raw shell response (script query 6).
A sample of 14 SPA `.рф` domains from the head of the q:render wave:
12 already rendered, all with **empty** cookies — parkings without
Laravel: render gave them nothing. The dom_only rows (2) — one Livewire
site `xn--80afc7aid1a.рф` (wire:snapshot, livewire.min.js, form _token,
vite); needs_dom (14) — real applications (Inertia on 46.рф, the "Kursk
region resident map", Livewire suare.su, 24.рф) — rare genuine SPAs on
Laravel, not parkings. The JS-cookie/XHR-cookie class is negligible:
sanctum_csrf_cookie fired 2 times per 33.8K finds. Findings freshness:
rendered finds whose row has not yet been re-analyzed after render
(`rules_version IS NULL`) — 0 (script query 7).

### Base rates

- **Finds by zones** (`is_laravel`, the ~19:20 snapshot): `.рф` 4,819
  (14.3%), `.su` 1,315 (3.9%), other zones 27,673 (81.8%, including
  ru 3,764 — 11.1% of all). The distribution reflects crawl progress
  (the ingest went ru/рф/su first; com — ≈5.5K of 13.9M checked —
  §12), not only zone "quality".
- **Renders**: rendered rows in `subdomain_results` ≈89K
  (TABLESAMPLE 0.5% — ≈22.7K rows read of ≈4.5M,
  a ×200 estimate, a spread of ~±5%); of them `.рф` ≈88K (98–99%),
  `.su` — hundreds (missed the sample), others ≈0. The `.рф` find
  yield: 440/88,000 ≈ 0.5% per render; dom_only — 2/88,000 ≈ 0.002%
  of slot time.
- **Debt at ~14:45** (the numbers drift with the drainage): XLEN
  q:render
  140,865; the undelivered tail (after last-delivered-id) 95,051 —
  ru 32.2%, com 6.3%, `.рф` 0.2%, `.su` 0.1%, a long tail ~60%;
  PEL 4,927 (angry retries) — **`.рф` 97.2%**, ru 2.8%; another ~344.5K
  domains with the `render_pending` flag are unpublished — the zone
  composition **not measured** (no index over the `domains.flags`
  bits, a scan of 41.8M rows is forbidden).
- **The undelivered tail** (a systematic sample of 476 rows, ~14:50):
  raw-phase finds in 11 (2.3%), `is_laravel` in 3 (0.6%); 97.7%
  of the tail's rows have no raw-data find.

### Limitations

- Rendered rows are analyzed by render data (DOM, the cookie jar,
  the final navigation's headers): the original HTTP-phase
  cookies/headers were overwritten, a direct "before/after" comparison
  for one row is impossible. Both estimates are biased in both
  directions (see "Methodology"): the observed dom_only 0.45% for `.рф`
  may be overstated (both rows — Livewire, whose wire:* also appear in
  the raw body under a false SPA verdict), needs_dom
  2.5% — understated by exactly the finds owed to render cookies; the
  biases are bounded empirically (the JS-cookie class is negligible,
  parking cookies measured in the raw phase for 86.4% of non-rendered
  finds), but they are not confidence intervals — only the order of
  magnitude.
- A "before/after render" measurement for one row is impossible also due
  to the freshness window: findings of a rendered row are written by the
  re-analysis after render;
  at the snapshot rows without re-analysis were 0, but the window exists
  theoretically (script query 7).
- A C signal could in principle also be present in the raw SPA shell
  (meta csrf in the head, links to the vite bundle) — dom_only contains
  no such rows without Livewire/Inertia hydration, but the class is
  theoretically not empty; conversely, DetectSPA (T-057) — a heuristic,
  the "SPA" verdict could be false for server-rendered Livewire pages
  (then their dom_only signals were in the raw body too).
- The measurement records **completed** renders: 98–99% of them — `.рф`.
  For classes where renders almost did not happen (ru, com, the long
  tail — 95.5% of the undelivered debt), the DOM contribution **is not
  measurable until renders happen**; the "order of zero" conclusion does
  not transfer to them automatically (the tail — not parkings but real
  SPAs: 97.7% of rows without raw-phase finds).
  The first rendered find of other zones with content necessity
  (needs_dom=1) appeared already between the measurement's slices.
- The composition of the PEL and the undelivered q:render tail, the
  sample of 14 head-of-wave domains and the tail sample of 476 rows —
  one-off checks of the measurement session (Redis XRANGE/XPENDING and
  point reads by PK), not part of the script; the numbers drift with
  the drainage.
- The counters drift (a live pipeline): the results table — one
  consistent slice; the rule frequencies and the debt rates were taken
  at neighboring moments (discrepancies of ±units, for rendered rows of
  other zones — tens over the drainage hours).

### Conclusion and recommendation (the decision is the operator's)

The `.рф` parking class: a DOM contribution on the order of zero —
the observed 0.45% of rendered finds (dom_only; with false Livewire
SPA verdicts — as low as 0) and 2.5% (needs_dom; counting render cookies —
no less); in slot time 0.002%. 90.9% of `.рф` finds (4,379/4,819) exist
without render, the score carried by the raw phase's server cookies.
Meanwhile `.рф` — 97.2% of the angry PEL retries (tarpits/cert errors)
and consumed 98–99% of all completed renders with a find yield of
0.5%.

- **(b) — the data support it**: not rendering the `.рф` parking class
  at all (verdict from HTTP signals). The observed coverage loss —
  2–11 rows of 4,819 (0.04–0.23% of the zone's finds; both estimates are
  biased in both directions, the order — fractions of a percent), all
  lost ones — real applications, not parkings; the PEL tail of angry
  retries is freed.
  The product decision on coverage — the operator's; record the
  deviation modeled on T-166 (docs/content.md, "Recorded deviations").
- **(a) — debt prioritization** is relevant for the rest of the debt:
  the undelivered tail (95K) is already de facto sorted "non-`.рф`
  first" (`.рф` in it 0.2%); the RequeueRenderPending order does not
  need changing for it while decision (b) is not taken — then the
  sources of `.рф` demand will be only the PEL and the unpublished
  `render_pending` (the latter's composition not measured).
- The acceptance criterion when implementing shaping (from the task):
  the baseline of this measurement — ~99% of render slot time goes to
  classes with a DOM contribution of ≈0 (`.рф` 0.45%); the goal —
  a reduction of ≥ an order of magnitude; the find yield does not fall
  (a control sample via re-analysis; candidates — the dom_only/needs_dom
  domains of this measurement, including the Livewire site
  `xn--80afc7aid1a.рф` — a direct test of a false SPA verdict). After
  the ru/com tail is drained, re-run the measurement with the same
  script — to close the unknown DOM contribution of non-parking classes.

**The operator's decision (2026-10-24, evening): variant (a)** — debt
prioritization by zones, no find losses: the RequeueRenderPending
selection puts zones with a high DOM contribution first, parking
`.рф` — gets the leftovers. Implementation — task T-184 (a two-step
selection without sorting, coordination with T-181 — both reshape
renderSelectPending); variant (b) (abandoning `.рф` rendering with
a recorded deviation) not adopted. After the ru/com drainage the
measurement is re-run with the same script — the decision is revisited
by the actual DOM contribution of non-parking classes.

## Live tests on the stack

The browser tests (`browser_live_test.go`) are skipped by default;
they are enabled by `CRAWLER_RENDER_TEST=1` against the chromium compose
service (the address: `CRAWLER_RENDER_REMOTE_URL`, then
`CHROMIUM_WS_URL`,
default `ws://172.28.100.14:9222`):

```bash
docker compose --project-directory . -f deploy/docker/compose.yaml \
  run --use-aliases -e CRAWLER_RENDER_TEST=1 --rm tools \
  go test ./internal/render -run BrowserLive -v -count=1
```

`--use-aliases` enables the tools service's DNS aliases (needed by the
tracker-blocking live tests — T-214 — and the per-engine slice below;
harmless to the rest).

The per-engine engine slice (T-254, `engines_live_test.go`): the
chromium/moli/lightpanda table — one code × sets of address variables
(`CHROMIUM_WS_URL`/`MOLI_WS_URL`/`LIGHTPANDA_WS_URL`, the defaults — the
compose service addresses; the `CRAWLER_RENDER_REMOTE_URL` override of
the other live tests is not read by this slice). For each engine —
a `/json/version` probe (an unavailable engine at the default address —
SKIP with a reason, with the variable explicitly set — a refusal), then
a slice of the spec 5.6 invariants (FR-5 of the render engines spec):
per-task context isolation, navigate + capture of DOM/JS- and
httpOnly cookies/response headers, the Fetch interception by types
(T-062) and tracker domains (T-214, the shared differentiator
`blockLiveTrackerDomains`),
the SSRF perimeter (T-219: task rejection with the task class; T-230:
navigation to a private testbed address is cut off by the interception —
`net::ERR_BLOCKED_BY_CLIENT`; the perimeter's resolve in the test —
the stack's unbound via the `Options.LookupHost` seam: the tools run
container's resolver is external and does not know the test zones), the
UA override (both in `navigator.userAgent` and in the request header at
the fixture server). moli/lightpanda are brought up by the
`render-engines` profile; the pinned-version state against the slice —
the file's doc comment and the FR-17 smoke procedure in
[`dev-environment.md`](dev-environment.md).

Task pages — HTTP fixtures: the test starts a server in the run
container (listening on 0.0.0.0, pages are given the container's address
in `crawler_net` — the `liveIPv4`/`liveServe` helpers in
`browser_live_test.go`), Chromium reaches it from the neighboring
container. data: pages (the "cheap" hermeticity of the early live tests)
are impossible after R-09:
`validateTaskURL` rejects non-http schemes, the tests were moved to
fixtures (T-216).

The composition: parallel rendering of fixture pages through the pool
(4 tasks on 2 contexts, the page counter), profile isolation (a cookie
does not survive a context switch), the User-Agent override. Resource
blocking (T-062,
`block_live_test.go`): a local HTTP server with counters — the image and
the font are not requested, CSS/script/XHR are requested; CSS with the
T-213 option — not requested at all; tracker domains with the T-214
option — the differentiator of two runs (the probes
`c.clarity.ms`/`mc.yandex.ru` resolve via the tools service's DNS
aliases, for which compose run is called with `--use-aliases`: in the
control run without the option the probes load, with the option —
they are cut off before being sent). Waiting for readiness
(T-063, `wait_live_test.go`): the fixture's quiet page ends with the
quiet window, the "noisy" one (a fetch to TEST-NET every 300 ms) —
with the quiet phase cap, the page with an eternal script — with the
overall timeout and a `task`/`DeadlineExceeded` error. Capturing the
result T-064
(`extract_live_test.go`): the exact `outerHTML` of the fixture page
after the script runs, the JS cookie `js_session` on the fixture's
internal page
`spa-shell.crawl.test` and its response headers. The flag is also
available to the worktree agents of the next tasks (T-065…T-067).

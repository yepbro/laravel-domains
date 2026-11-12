# OS tuning of the production server (spec §10.2)

The baseline values come from the project configuration and the worker code;
the settings file is
[`deploy/sysctl/99-crawler.conf`](../deploy/sysctl/99-crawler.conf),
the instructions for applying and verifying are in
[`deploy/sysctl/README.md`](../deploy/sysctl/README.md).

## 1. The load the tuning is sized for

One server, one external IP (spec §3.1). The planned values (config defaults,
spec §5.10):

| Parameter | Value | Where |
|---|---|---|
| dns.concurrency | 2000 | `internal/config`, spec §5.4 |
| dns.udp_pool_size | 2000 (= concurrency) | the UDP socket pool of `internal/dnsclient` |
| dns.max_qps | 10,000 | the QPS limiter (toward the local unbound) |
| http.concurrency | 2000 = the global connection cap | the `internal/httpfetch` LimitOptions |
| http.max_rps | 1000 | the global request rate |
| http.per_ip_limit | 4 | connections per one IP |
| keep-alive | disabled | spec §5.5, `DisableKeepAlives: true` |

The key consequence of keep-alive being disabled (spec §5.5: "keep-alive is
disabled or has a very short idle"): **every HTTP request is a new outgoing
TCP connection**. At max_rps 1000 this is up to ~1000 connection setups and
closes per second from a single IP. The DNS stage runs in parallel: up to 2000
concurrent UDP exchanges with the local unbound (one pool socket — one
request in flight, the `UDPPool` contract), and on TC=1 the TCP fallback —
up to 2000 concurrent short TCP connections to unbound as well.

DNS datagrams are small: the `internal/dnsclient` client does not set EDNS0 —
requests ≤ ~300 B, responses ≤ 512 B (a truncated response goes to the TCP
fallback, where there is no size limit). The 64 KiB read buffer
(`dns.MaxMsgSize`) is application memory, not kernel memory.

## 2. Ephemeral ports and TIME_WAIT (spec §10.2 item 1)

### net.ipv4.ip_local_port_range = 10240 65535

Sizing the port demand:

- active outgoing HTTP connections: ≤ 2000 (http.concurrency);
- TIME_WAIT: a connection closed by our side lives 2×MSL = 60 s.
  The worst-case churn of 1000/s × 60 s = **up to ~60,000 4-tuples
  in TIME_WAIT at the same time** (remote hosts do not always close first —
  timeouts and tarpits are closed by us).

Uniqueness is required not for a port but for a 4-tuple (local port +
remote address:port): the same local port serves different destination
addresses at the same time. The hard demand is the maximum number of
concurrent 4-tuples **to one destination address** (active + the TIME_WAIT
accumulated toward it): per_ip_limit 4 keeps the active ones, and
`tcp_tw_reuse=1` (below) allows TIME_WAIT ports to be reused for new
outgoing connections. So the range is wider than the hard demand and is
chosen by an upper-bound estimate: 55,296 ports (the kernel default
32768–60999 — 28,232) covers the peak of 60k TIME_WAIT + 2k active, with
the reuse between destinations taken into account.

The lower bound is 10240, not 1024: all listeners of the production server
sit below it — sshd 22, unbound 53, postgres 5432, redis 6379 (local),
service metrics 9090+, DevTools Chromium 9222. The kernel does not point an
ephemeral port at the port of an active listener, but an outgoing connection
that has taken a port keeps the service from **coming up** after a restart
(the crawler runs for weeks). There are no local listeners above 10240.

The dev-environment containers set `net.ipv4.ip_local_port_range
= 1024 65535` for themselves in compose separately (the container's netns
is isolated); for the production server the conservative option without
risk to the listeners was chosen.

### net.ipv4.tcp_tw_reuse = 1

The semantics of the values on kernels ≥ 4.12: 0 — disabled, 1 — globally
for outgoing, **2 — loopback only (the default of modern kernels)**. DNS
TCP fallback goes to loopback — the default covers it too, but the HTTP
worker connects to remote sites → 1 is needed.

Reuse is allowed by the kernel only ~1 RTO after an entry into
TIME_WAIT, and only with TCP timestamps enabled (PAWS protects against
receiving stale segments of the previous connection) — `tcp_timestamps = 1`
is pinned explicitly. The risk to outgoing connections is minimal: the
mechanism acts on active open and does not touch incoming ones.

`tcp_tw_recycle` **is not used**: it was removed from the kernel in 4.12
precisely for breaking connections behind NAT — it is mentioned here so it
does not "surface" in searches; it is absent on the target server.

### net.ipv4.tcp_max_tw_buckets = 262144

A cap on the number of TIME_WAIT sockets: the expected peak is ~60,000
(the calculation above) — a ×4 margin. On overflow the kernel kills the
TIME_WAIT entries immediately, logging
`TCP: time wait bucket table overflow` to the log — for outgoing
connections this is safe, but noisy and masks problems; the default on an
8 GB machine may turn out to be close to the peak. The memory per entry
(~a slot in the ehash) is small.

## 3. Receive queues: somaxconn, syn_backlog, backlog

The listening sockets of the production server are not the crawler's
connections but local services: unbound TCP 53 (takes the DNS worker's
TCP fallback — up to ~2000 concurrent), the metrics of every Go service,
postgres, redis.

- `net.core.somaxconn = 4096` — the cap on a listener's accept queue
  (caps `listen(backlog)`). The default of 128 on some distributions: a
  TC=1 burst (mass truncation of DNSSEC responses) can knock out part of
  the TCP fallback by overflow.
- `net.ipv4.tcp_max_syn_backlog = 4096` — the SYN queue of the same
  listeners; kept in sync with somaxconn.
- `net.core.netdev_max_backlog = 4096` — the packet queue between softirq
  and the protocol. The crawler's RX load is downloading bodies of up to
  2 MiB × up to 1000 RPS: gigabit peaks are dropped at the default of 1000
  (the check is the second column of `/proc/net/softnet_stat`); a drop =
  retransmissions. 4096 — with a margin; the memory per slot is small.

Risks: the queue length × `sk_buff` — negligible memory against 8 GB of
RAM; individual bursts will land in the queue, not in a drop.

## 4. File descriptors: fs.file-max and nofile (spec §10.2 item 1)

### nofile — the per-process limit

The largest consumer is dns-worker:

| Component | Calculation |
|---|---|
| UDP socket pool | 2000 (dns.udp_pool_size) |
| TCP fallback | ≤ 2000 concurrent (each TC=1 exchange is its own connection; bounded by concurrency) |
| DB pool + Redis + epfd/timers/metrics | ~100 |
| **total** | **~4100** |

http-worker: ≤ 2000 connections (GlobalConns — redirect hops come out of
the same limiter budget) + ~200 (pools, pack files being written) ≈ 2200.

Hence **LimitNOFILE=65535** for every Go service — a ×16 margin over the
largest process. In systemd it is set by T-085 (`deploy/systemd/`),
in Docker it is already in place (`ulimits.nofile` in compose). It is not
a sysctl — not part of `99-crawler.conf`; the nofile cap `fs.nr_open`
(default 1,048,576) is above 65535 and is left untouched.

### fs.file-max = 2097152 — the system-wide cap

The sum across all processes (peak):

| Consumer | fd peak |
|---|---|
| dns-worker | ~4100 |
| http-worker | ~2200 |
| unbound (accepting the TCP fallback) | ~2100 |
| render-worker + Chromium | ~1500 |
| analyzer (reading packs) | ~300 |
| scheduler, ingest, monitor | ~300 |
| PostgreSQL (max_connections) | ~300 |
| Redis, systemd/journald/sshd | ~700 |
| **total** | **~11,600** |

The role of the value is not a "budget" (the limit allocates no memory:
a `struct file` is allocated only for a file actually open), but a
system-wide guard against descriptor leaks: the finite cap trips before
the leak eats the memory. 2,097,152 is two orders of magnitude above the
planned peak. On new kernels the default is LONG_MAX ("no limit"), on
older LTS ones it is computed from RAM; in both cases 2M is enough and
does not lower the default to a dangerously close distance from the peak:
file-max is a kill-switch at the whole-OS level (ENFILE blocks sshd and
postgres together with the crawler).

Checking the process limit: `cat /proc/$(pidof dns-worker)/limits`;
the system-wide counter: `cat /proc/sys/fs/file-nr`.

## 5. Conntrack (spec §10.2 item 2)

If the `nf_conntrack` module is loaded (iptables/nftables rules, Docker),
every flow takes a table entry.

Sizing: a churn of ~1000 closed TCP/s; conntrack keeps an entry for another
`nf_conntrack_tcp_timeout_time_wait` = 120 s after closing →
**~120,000 entries in steady state** + ~4000 active TCP + the UDP flows
(2000 connected pool sockets to unbound — loopback traffic is tracked too:
the entries are constantly refreshed and do not expire; plus
unbound → upstreams).

The default `nf_conntrack_max` on an 8 GB machine is usually 262,144:
workable, but without a margin for peaks and bursts. Overflow drops packets
of **all** of the server's connections
(`nf_conntrack: table full, dropping packet`) — including postgres/unbound
loopback. Hence:

- `net.netfilter.nf_conntrack_max = 1048576` — a ×8 margin over steady
  state; the memory is lazy, ~320 B × a live entry (120k ≈ 37 MiB).
- `net.netfilter.nf_conntrack_buckets = 262144` (the hash table = max/4):
  chains longer than ~8 entries degrade lookup in the hot path of every
  packet. Writable via sysctl since kernel 5.15; on older ones — the
  module parameter `hashsize=262144` (see deploy/sysctl/README.md).

The `net.netfilter.*` keys exist only when the module is loaded — without
it systemd-sysctl skips the lines with a warning. For persistence the
module is added to the autoload list
(`/etc/modules-load.d/nf_conntrack.conf` — `systemd-modules-load` runs
before `systemd-sysctl`), otherwise the values will be lost on reboot: a
module raised later by Docker or by firewall rules will get the defaults.
Changing the hash table on the fly on kernels < 5.15 — by writing to
`/sys/module/nf_conntrack/parameters/hashsize` (unloading the module on a
running server is impossible).

The alternative from the spec ("or exclude the crawler's traffic") —
NOTRACK for the HTTP worker's outgoing traffic:

```
iptables -t raw -A OUTPUT -m owner --uid-owner http-worker -j CT --notrack
```

Pros: no entries are created at all. Cons: the traffic drops out of the
stateful firewall (the ESTABLISHED rules no longer match — a separate
allow is needed), breaks NAT — incompatible with Docker hosts. The
project's main path is the enlarged table; NOTRACK is kept as an emergency
lever for bottlenecks surfaced by measurements (T-092).

## 6. DNS UDP buffers (spec §10.2 item 3, §5.4)

- `net.core.rmem_max = 16777216`, `net.core.wmem_max = 16777216` —
  the cap on `setsockopt(SO_RCVBUF/SO_SNDBUF)` for processes without
  CAP_NET_ADMIN. Raised primarily for the sake of **unbound**: at high
  QPS its `so-rcvbuf` runs into rmem_max (kernel datagrams from the
  upstreams queue up on slow zones), and without the CAP_NET_ADMIN
  capability the request is silently clipped. The margin also covers a
  future `SetReadBuffer` of the `internal/dnsclient` pool (the pool does
  not set the buffers now — rmem_default applies).
- `net.core.rmem_default` / `net.core.wmem_default` — deliberately **not
  set** (commented out in the config, with the rationale). The defaults
  are already sufficient: exactly one request in flight per pool socket
  (the UDPPool contract), datagrams ≤ 512 B — the default 208 KiB per
  socket gives a ×400 margin. Pinning the kernel default (212,992)
  explicitly would override the distribution's/administrator's tuning:
  the files in `/etc/sysctl.d` are applied in lexicographic order, and
  `99-crawler.conf` would overwrite any value set earlier, with no
  functional gain.
- TCP fallback (responses up to 64 KiB) and HTTP bodies (up to 2 MiB) live
  on `net.ipv4.tcp_rmem/tcp_wmem` with autotuning — the default max of
  4–6 MiB suffices (the body is read streaming into the pack files; the
  window is bounded by the BDP, not by the body size). Left alone.

## 7. What is deliberately not configured

| Parameter | Reason |
|---|---|
| `net.ipv4.tcp_tw_recycle` | removed from the kernel (4.12); absent on the target server |
| `net.ipv4.tcp_mem` | autotuned from RAM (8 GB ≈ 344/458/686 MiB); the bodies are read streaming and the per-socket window is bounded by tcp_rmem — pinning an explicit value without measurements (T-092) is premature; exceeding it is unlikely and not fatal (thrashing, not drops) |
| `net.ipv4.udp_mem` | autotuned from RAM; datagrams ≤ 512 B × 2000 sockets — orders of magnitude below the pressure zone |
| `net.ipv4.ip_local_reserved_ports` | the range's lower bound of 10240 already takes all local listeners out of the conflict zone |
| `net.ipv4.tcp_fin_timeout` | the crawler's FIN_WAIT-2 orphans are bounded by churn × 60 s ≈ within 60k and are killed by the timer; there is no separate problem |
| `net.ipv4.tcp_syncookies` | protection of inbound listening sockets; a SYN flood is not a scenario for an internal server |

## 8. Monitoring after applying

Control under load (the daily run of T-093 — done, the window
Oct 23–24 ≈25 h, no OOM events —
[load-measurements.md](load-measurements.md)
§13; the commands remain the standard control kit):

```bash
ss -tan state time-wait | wc -l          # growth toward tcp_max_tw_buckets — recompute the churn
cat /proc/sys/net/netfilter/nf_conntrack_count   # growth toward max — recompute the table
cat /proc/sys/fs/file-nr                 # first column → toward fs.file-max
journalctl -t smartd -p warning          # the drive's SMART/TBW (T-089, docs/disks.md)
dmesg | grep -E "table full|time wait bucket"    # must be empty
awk '{print $2}' /proc/net/softnet_stat | sort | uniq -c | tail   # RX drops = 0
```

Degradations are fixed with targeted changes (the values are the
calculations above), not by restoring the whole file: every parameter is
independent and reversible (`sysctl -w`).

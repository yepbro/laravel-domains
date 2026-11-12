# Production server disks: the PostgreSQL and content volumes, SMART/TBW (spec §10.3)

The disk layout and drive wear monitoring of the target server
(Ubuntu Server LTS, systemd — spec §10.1; a single NVMe with spare
capacity — §3.1). The SMART monitoring kit is
[`deploy/smartd/`](../deploy/smartd/) (the smartd config, the warning
handler, the udev rule and the monitor unit drop-in); the mount points
and permissions are aligned with the T-085 units ([`deploy.md`](deploy.md),
[`deploy/systemd/`](../deploy/systemd/)). Related: the PostgreSQL tuning
that affects WAL writes — T-086 ([`db.md`](db.md)); Redis giving up
persistence — T-087 ([`redis.md`](redis.md)); sysctl — T-088
([`os-tuning.md`](os-tuning.md)).

Spec §10.3 — two requirements, both covered here:

1. **Separate directories/volumes for PostgreSQL and the content pack
   files** — the "Layout" section below;
2. **SMART and NVMe wear monitoring (TBW)** — the write load estimate
   ("The TBW estimate") and the two observation layers ("SMART and TBW
   monitoring").

## 1. Why separate volumes (and what they do not give)

There is one server and one drive (§3.1) — physically spreading the WAL
writes and the content across different devices is impossible. An honest
statement: **NVMe wear is reduced not by the layout but by the write
volume** — wear-leveling redistributes writes across the whole die
regardless of LBA numbers; volumes change nothing in this respect. Wear
is limited by the write configurations (a 15 min checkpoint and
`wal_compression` — T-086; Redis persistence turned off — T-087) and is
observed by the SMART monitoring below.

What separate volumes give in practice:

| Benefit | Mechanics |
|---|---|
| Fill isolation | Content is the most unpredictable consumer of space (fat pages, weak dedup of individual zones, compaction). Having filled the volume shared with PGDATA, it would have stopped PostgreSQL: WAL does not grow, autovacuum and result writing stall — the source of truth goes down (§3.4). Separately, the content volume filling triggers the regular `Put` pause (`content.disk_free_percent`, spec §8): results are written, the crawler degrades as designed, not with a crash |
| A precise disk threshold | `statfs` of the `content.dir` directory (the `Put` pause and the `disk_free` alert, T-080) measures exactly the content volume. On a shared volume the "low space" warning would fire because of someone else's growth (WAL during a mass recheck, the database) — a desync with the actual content write pause |
| Independent options | `noatime`, fsck checks, separate fill monitoring and the growth rate per volume |
| Evolution | When a second NVMe is added, the volume is moved as a whole (`pvmove` or a new PV in the group) without remounting paths or editing service configurations |

The converse holds the other way too: a WAL burst (a mass recheck, ingest
of new zones) does not eat the space for pack files.

## 2. The layout: LVM, ext4, noatime

### LVM, not partitions

The decision: **LVM** logical volumes on top of the NVMe data partition.

- **online-resize**: content grows and grows unpredictably (the estimate
  — §3); extending a volume by adding free extents (`lvextend -r`) —
  without downtime; a GPT partition grows only into free space right
  next to it and with partition-table gymnastics;
- moving to a larger device: `pvmove` (even over the network) or a new
  PV in the same VG — the paths and permissions do not change;
- LVM is the Ubuntu Server installer's default; nothing needs to be
  installed on top.

### ext4, not XFS

The decision: **ext4** for both volumes.

- the distribution default, a proven pairing with the packaged
  PostgreSQL;
- pack files are written strictly sequentially and by a single writer at
  a time (the flock `.packs.lock`, [`content.md`](content.md)
  "Concurrent writing") — XFS's parallel allocations are not exercised,
  there is nothing to monetize their advantage with;
- ext4 can online-shrink (XFS cannot): volume rebalancing without moving
  the data out;
- XFS is strong on dozens of parallel large files — not our pattern (one
  active pack + random `ReadAt` reads from closed ones).

### Volumes and mount points

| LV volume | Mount point | FS, label | Options | What lives there |
|---|---|---|---|---|
| `/dev/vg-crawler/pg` | `/var/lib/postgresql` | ext4, `crawler-pg` | `noatime` | PGDATA and `pg_wal` of the packaged PG (`/var/lib/postgresql/<version>/<cluster>`) |
| `/dev/vg-crawler/content` | `/var/lib/crawler/content` | ext4, `crawler-content` | `noatime` | the content pack files, the zstd dictionary (T-085 §3) |

The root FS (the system, `/opt/crawler`, `incoming/`) is separate; we do
not touch it. The names: VG `vg-crawler`, LVs `pg` and `content`.

**WAL is not moved to a separate volume**: on a single drive it does not
split the wear (wear-leveling is per device) and only adds another
filling point and a size recalculation; the WAL volume is capped by
config (`max_wal_size` 8 GB, T-086). Moving `pg_wal` out becomes
relevant with a second NVMe — at the same time as content.

`noatime` on both volumes: neither PostgreSQL nor the pack readers (the
analyzer, `crawlerctl domain --content`) use the last-access time; the
default `relatime` writes atime once a day per file — a trifle, but
`noatime` is free and removes it entirely.

TRIM: the periodic `fstrim.timer` (enabled in Ubuntu by default) is kept
— discard passes through device-mapper and helps the NVMe garbage
collector. Continuous discard as a mount option is not used (the
periodic one is enough, fewer disturbances on hot writes).

## 3. Volume sizes (estimated from the planned volumes)

The estimates are rough (the assumptions are in parentheses); exact
values will come from operating the first cycle (the spec §11.3
measurements); LVM was chosen precisely so that a mistake is fixed by
`lvextend`, not a reinstall.

### PostgreSQL ≈ 0.3 TB of live data

| Component | Estimate | Size |
|---|---|---|
| `domains` | 350M rows × ~190 B (the heap ~120 B with fillfactor 85 + the PK, the unique `(zone_id, label)`, the partial `check_state=0`) | ≈ 65 GB |
| `subdomain_results` | 350M × ~50% delegated × 2–3 records (subdomains × variants) × ~300 B with indexes | ≈ 100–160 GB |
| `contents` | 150–300M unique hashes × ~80 B | ≈ 12–25 GB |
| `laravel_*` | single-digit percents of domains × ~300 B | ≈ 5–15 GB |
| WAL + bloat | ≤ 8 GB of segments (T-086) + ~15% junk (autovacuum 0.02) | ≈ 30–50 GB |
| **total** | | **≈ 0.2–0.3 TB** |

The `pg` volume: **0.5–1 TB** — headroom for the zone reference table's
growth and statistics (`pg_stat_statements`).

### Content ≈ 5–15 TB of live blocks

350M × ~50% of domains with content × ~2 pages × 20–60 KB per zstd block
(raw HTML 100–300 KB × 4–6 compression with a dictionary) = 7–21 TB −
~20–30% parking deduplication (spec §8) ≈ **5–15 TB of live blocks for
the first cycle**; dedup can be fatter in individual zones — the
estimate is rather an upper bound. Until the first compaction, the space
is also taken by the dead blocks of rechecks (T-045).

The `content` volume: **the entire rest of the disk**. Layout examples:

| NVMe | root | `pg` | `content` |
|---|---|---|---|
| 4 TB | 100 GB | 1 TB | ≈ 2.9 TB — tight: the estimated content does not fit; the counter is compaction and `disk_free_percent` (the write pause per spec §8); take such a disk only deliberately |
| 8 TB | 100 GB | 1 TB | ≈ 6.9 TB — the working minimum of "plenty of space" (§3.1) |
| 4 TB + a second 4 TB | 100 GB | 1 TB | the second NVMe — a new PV in the VG, `content` moves onto it as a whole (`pvmove`): the volumes are not mixed across drives, one drive's failure affects one volume |

## 4. Setting up the layout

Commands run as root. The order per [`deploy.md`](deploy.md) §4:
**volumes first, packages second** — the PG cluster is created at
package installation directly in `/var/lib/postgresql`; a volume mounted
later would hide the data on the root FS.

### A fresh server (at OS installation)

In the Ubuntu installer's partitioning: GPT, an EFI partition, `/`
(root ~100 GB; swap to taste) — the tail of the disk is left
unpartitioned. After installation a data partition is created in the
free space (in the installer as "without a file system", or with any
tool afterwards; below `/dev/nvme0n1p4` is assumed — substitute the
actual number, `lsblk`):

```bash
pvcreate /dev/nvme0n1p4                     # the data partition
vgcreate vg-crawler /dev/nvme0n1p4
lvcreate -n pg -L 1T vg-crawler
lvcreate -n content -l 100%FREE vg-crawler
mkfs.ext4 -L crawler-pg /dev/vg-crawler/pg
mkfs.ext4 -L crawler-content /dev/vg-crawler/content
```

`/etc/fstab` (by labels — readable and survives name reordering):

```
LABEL=crawler-pg       /var/lib/postgresql        ext4 noatime 0 2
LABEL=crawler-content  /var/lib/crawler/content   ext4 noatime 0 2
```

Mount points and permissions (aligned with T-085 §3–4):

```bash
install -d /var/lib/postgresql /var/lib/crawler/content
mount /var/lib/postgresql                  # per the fstab entries
mount /var/lib/crawler/content             # (or mount -a)
chown postgres:postgres /var/lib/postgresql && chmod 0750 /var/lib/postgresql
install -d -o root -g crawler -m 2775 /var/lib/crawler/content
```

The package creates the PGDATA inside at `apt install postgresql`
(`pg_createcluster`); the content directory is setgid root:crawler, as
in deploy.md §4.4: two writer units (http/render-worker) and the readers
(analyzer, crawlerctl) work without permission juggling.

### An existing server

Free extents in the volume group (the Ubuntu installer with the standard
LVM scheme often leaves space unallocated; the VG name is shown by
`vgs` — the installer default `ubuntu-vg`):

```bash
vgs                                       # VFree — how much is available
lvcreate -n pg -L 1T ubuntu-vg            # the production server's VG
lvcreate -n content -l 100%FREE ubuntu-vg
```

Without free extents — add a partition or a second NVMe to the VG:

```bash
pvcreate /dev/nvme0n1p4 /dev/nvme1n1
vgextend vg-crawler /dev/nvme0n1p4 /dev/nvme1n1
```

Data migration (stop the services; packs are regular files — rsync over
them is consistent only with the writers stopped):

```bash
systemctl stop postgresql crawler.target
mkfs.ext4 -L crawler-pg /dev/vg-crawler/pg
mount /dev/vg-crawler/pg /mnt
rsync -aHAX /var/lib/postgresql/ /mnt/
umount /mnt
mount -a                                  # brings the volumes up per fstab
systemctl start postgresql crawler.target
```

The old contents of `/var/lib/postgresql` remain on the root FS under
the mount point — delete them after verification. Content is moved the
same way (`/var/lib/crawler/content`).

### Verification

```bash
findmnt /var/lib/postgresql /var/lib/crawler/content   # SOURCE, the noatime options
lsblk -f                                                # the volumes, labels, mount points
df -T /var/lib/postgresql /var/lib/crawler/content      # the separate capacities
sudo -u postgres psql -c 'SHOW data_directory;'         # PGDATA on the pg volume
sudo -u crawler-http-worker test -w /var/lib/crawler/content && echo ok
```

The last line — content writing is available to the writer unit (the
`crawler` group, T-085); the Ubuntu package's `data_directory` is
`/var/lib/postgresql/<version>/<cluster>` (e.g. `16/main`).

## 5. The TBW estimate from the crawler load

Spec §10.3 requires wear monitoring, "since the write load is high". The
estimate is a lower bound based on the project's configured volumes.

### PostgreSQL

The volume's writes over the first cycle (350M domains, weeks — §3.3):

| Stream | Estimate | Size |
|---|---|---|
| WAL: ingest | the COPY of 350M rows + indexes × ~1.5 | ≈ 45–70 GB |
| WAL: statuses | 2–3 updates per domain (pending→queued→done) × ~150 B per new version (HOT, indexes are not written) | ≈ 100–160 GB |
| WAL: results | inserts/updates of `subdomain_results`, `contents`, `laravel_*` | ≈ 100–170 GB |
| full-page images | checkpoints every 15 min, `wal_compression=on` — +30–50% to the page writes after a checkpoint | ×1.3–1.5 on data |
| dirty pages | the data is written to the files too (a double write: WAL + checkpoint) | ≈ the data volume ≈ 0.3 TB |
| **total volume writes** | | **≈ 0.5–1 TB over the first cycle** |

Steady state (a recheck every 6 months — §3.3): the same streams spread
over ~180 days → **~3–6 GB/day**. `synchronous_commit=off` does not
change the volume — it cuts the number of fsyncs, not the bytes (T-086).

### Content

| Stream | Estimate | Size |
|---|---|---|
| fill | all the live pages of the first cycle (§3) | ≈ 5–15 TB |
| recheck churn | unchanged pages are not written at all (a Put by the same hash → `refcount+1`, `content.md`); 20–40% change | ≈ 2–8 TB per half a year |
| compaction | rewrites the live blocks of packs whose live share is < 50% (T-045) | ≈ +50–100% churn |
| **total writes** | | **≈ 5–15 TB per half a year** |

Redis does not write to disk at all (persistence is off — T-087),
Unbound keeps a cache in memory, journald — pennies.

### Conclusion

**On the order of 10–30 TB of writes per year** against the TBW of
typical consumer NVMe drives — 600–2400 TB for 2–4 TB models — a margin
of tens of times. Wear does not limit the service life at the designed
volumes, but monitoring is mandatory (§10.3): real pages are fatter than
average, cheap OEM drives have a multiple lower TBW, and actual wear is
not the estimated one. Control points: `percentage_used` ≥ 90% (the
monitor alert, T-080) and 95% (smartd on smartmontools ≥ 7.5); critical
states — critical warning/available spare (smartd `-H`).

## 6. SMART and TBW monitoring

Two layers complementing each other; there is one warning channel —
syslog/journald, the same feed as all services (T-085): the operator
reads one journal.

### Layer 1: smartd (the smartmontools package daemon)

The config is [`deploy/smartd/smartd.conf`](../deploy/smartd/smartd.conf);
installation and verification —
[`deploy/smartd/README.md`](../deploy/smartd/README.md).

| Directive | What it catches | Reaction |
|---|---|---|
| `-H` | the health log's Critical Warning: available spare below the threshold, temperature out of range, reliability degradation, read-only | LOG_CRIT to syslog + the handler |
| `-l error` | growth of "Error Information Log Entries" — media/integrity errors | LOG_CRIT/LOG_INFO + the handler |
| `-W 4,45,60` | Composite Temperature: a change ≥4 °C, INFO ≥45 °C, CRIT ≥60 °C | syslog + the handler |
| `-f`, `-p` (7.5+, commented out) | Percentage Used > 95%, Media Errors growth; an Available Spare change | LOG_CRIT + the handler |

What it sends and who picks it up: smartd itself writes firings to
syslog (LOG_CRIT/INFO; journald shows them with the identifier
`smartd`); additionally, at each firing the handler
`/usr/local/lib/crawler/smartd-warning.sh` is run (`-m <nomailer>
-M exec` — mail is not used: there is no MTA on the server, spec §5.9
does not require mail as a channel). The handler writes a structured
record (the device, the event type, the first firing, the repeat
counter) tagged `smartd` and stays silent on stdout/stderr (smartd
treats output as a handler failure). The result:

```bash
journalctl -t smartd -p warning        # SMART warnings
journalctl -u smartmontools            # the daemon's own life
```

The check interval is the package default (30 min); for wear and
temperature that is enough with a margin.

### Layer 2: the monitor alert (T-080)

`internal/monitor/alerts.go` reads the health log directly via ioctl
(without nvme-cli): `percentage_used` ≥ 90% — a Warn (the `nvme_wear`
alert), in the details — the device's TBW from `data_units_written`; the
pass period is `monitor.alerts_interval` (5 min by default, T-206). In a
Docker container `/dev/nvme*` is not passed through — the alert honestly
reports "not measured" (no Warn, no false firings).

On a production systemd host the layer is enabled by the
`crawler-monitor` unit's access to the devices:

- `deploy/smartd/99-crawler-nvme.rules` — udev hands the controllers'
  char nodes (`/dev/nvme0`) to the `nvme-smart` group with 0660
  permissions: an ioctl admin command requires opening the node for
  writing; a separate group instead of `disk` gives access exactly to
  the NVMe, not to all block devices (the write permission is a kernel
  requirement; only the logs are actually read);
- `deploy/smartd/crawler-monitor.service.d/90-nvme-smart.conf` — the
  drop-in adds `SupplementaryGroups=nvme-smart` to the unit.

The result of the access: an alert firing (≥ 90%) writes a Warn to the
shared journald next to smartd (`journalctl -t crawler-monitor |
grep -i alert`); the 90% threshold fires earlier than smartd's 95% (on
smartmontools ≥ 7.5) and well before the resource is exhausted. Normally
the "ok" state is not written to the journal (the frontend signal is
transitions only, T-080): the disappearance of the "the alert is not
measured" Debug records at `CRAWLER_LOG_LEVEL=debug` is the sign that
the access works.

**Why two layers.** smartd runs as root independently of the crawler —
it catches the drive's death even if the whole `crawler.target` set is
down; the monitor layer ties wear to the other spec §5.9 alerts and
holds the `percentage_used` threshold where smartd cannot: in
smartmontools 7.4 (Ubuntu 24.04 LTS) there are no wear-threshold
directives for NVMe (`-f`/`-p` — experimental 7.5 features; the config
carries them commented out). On 7.4 the monitor layer is mandatory for
the §10.3 requirement (TBW); on 7.5 it is redundant.

### Operations

```bash
smartctl -H -A /dev/nvme0        # health: percentage_used, TBW, temperature
smartctl -l error /dev/nvme0     # the NVMe error log
journalctl -t smartd -p warning  # smartd warnings (empty = quiet)
```

`percentage_used` is monotonic (it never recovers): a ≥ 90% firing means
scheduling a drive replacement; critical warning/available spare —
immediate measures (replacement/moving; the data is recoverable by
re-checking — §6.3, but the cycle downtime is not cheap).

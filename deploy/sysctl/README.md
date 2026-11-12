# Applying the crawler's sysctl tuning

`99-crawler.conf` is a drop-in for sysctl.d(5): kernel parameters of the
production server sized for the crawler's single-IP load (spec §10.2).
The rationale and the calculation of each value —
[`docs/os-tuning.md`](../../docs/os-tuning.md).

Apply on the production server (Ubuntu LTS, systemd; spec §10.1). The
Docker dev environment is isolated in containers and does not need these
settings — the host needs the file only for load tests close to
production ([`docs/dev-environment.md` §9](../../docs/dev-environment.md)).

## Installation and applying

```bash
# as root (or via sudo)
install -m 0644 99-crawler.conf /etc/sysctl.d/99-crawler.conf

# apply all files in /etc/sysctl.d (lexicographic order)
sysctl --system
```

Persistence: `/etc/sysctl.d` is read by `systemd-sysctl.service` at every
boot — for most keys nothing else needs to be enabled after copying.
The exception is conntrack, see below.

The `net.netfilter.*` lines apply only if the `nf_conntrack` module is
already loaded (iptables/nftables rules, Docker). Without the module the
keys are absent from `/proc/sys`, and `systemd-sysctl` skips them with a
warning — that is not an error. Important for persistence:
`systemd-sysctl` runs early in the boot, and if nobody has loaded the
module by then, the values will not be applied at all (a module raised
later by Docker will get the defaults). Therefore the file comes together
with enabling the module's autoload — `systemd-modules-load` runs before
`systemd-sysctl`:

```bash
# apply now and at every boot
modprobe nf_conntrack
echo nf_conntrack | tee /etc/modules-load.d/nf_conntrack.conf
sysctl --system
```

## The conntrack hash table on kernels < 5.15

`net.netfilter.nf_conntrack_buckets` can be written via sysctl only since
kernel 5.15; on older LTS kernels the size is set by a module parameter —
`/etc/modprobe.d/nf_conntrack-crawler.conf`:

```
options nf_conntrack hashsize=262144
```

(takes effect from the next load of the module). Changing the size on the
fly, without unloading the module (on a production server with live
traffic unloading is impossible — `Module nf_conntrack is in use`), is
done by a write to sysfs — the interface is available on all kernels:

```bash
echo 262144 > /sys/module/nf_conntrack/parameters/hashsize
```

## Verification

```bash
# the values applied
sysctl net.ipv4.ip_local_port_range net.ipv4.tcp_tw_reuse \
       net.ipv4.tcp_max_tw_buckets net.core.somaxconn fs.file-max \
       net.netfilter.nf_conntrack_max net.core.rmem_max

# the whole kernel (slow, for cross-checking after incidents)
sysctl -a | less

# under load: normally TIME_WAIT does not approach tcp_max_tw_buckets
ss -tan state time-wait | wc -l

# conntrack: count must not approach max
cat /proc/sys/net/netfilter/nf_conntrack_count

# drops on overflow (must be empty)
dmesg | grep -E "table full|time wait bucket"

# the process descriptor limits (LimitNOFILE — the T-085 units)
cat /proc/$(pidof dns-worker)/limits | grep "open files"
```

`nf_conntrack_count` growing toward the maximum, or `table full` entries
in dmesg — a signal to recompute the table (docs/os-tuning.md,
"Conntrack").

## Rollback

All keys of the file are reversible on the fly (`sysctl -w`) — no reboot
is needed; check the effective defaults of the specific server with
`sysctl -a` before applying the file. Reference points for the kernel
defaults:

| Key | The file sets | Kernel default |
|---|---|---|
| net.ipv4.ip_local_port_range | 10240 65535 | 32768 60999 |
| net.ipv4.tcp_tw_reuse | 1 | 2 (kernels ≥ 4.12) |
| net.ipv4.tcp_timestamps | 1 | 1 |
| net.ipv4.tcp_max_tw_buckets | 262144 | depends on RAM (~65k–262k) |
| net.core.somaxconn | 4096 | 128 / 4096 (kernel-dependent) |
| net.ipv4.tcp_max_syn_backlog | 4096 | 128–2048 |
| net.core.netdev_max_backlog | 4096 | 1000 |
| fs.file-max | 2097152 | LONG_MAX (new) / from RAM (old) |
| net.netfilter.nf_conntrack_max | 1048576 | from RAM (~262k at 8 GB) |
| net.netfilter.nf_conntrack_buckets | 262144 | max/4 |
| net.core.rmem_max / wmem_max | 16777216 | 212992 |

```bash
# restore a single key
sysctl -w net.ipv4.ip_local_port_range="32768 60999"

# remove everything the file sets (afterwards, optionally restore the defaults
# per the table above)
rm /etc/sysctl.d/99-crawler.conf /etc/modules-load.d/nf_conntrack.conf
```

A full guarantee of a clean state is a reboot after removing the file;
for rollback and debugging purposes `sysctl -w` is enough.

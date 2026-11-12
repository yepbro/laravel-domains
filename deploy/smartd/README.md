# Applying NVMe SMART/TBW monitoring (T-089)

The production monitoring kit for the server's drive (spec §10.3:
"monitoring of SMART and NVMe wear (TBW), since the write load is high").
The rationale for the disk layout, the TBW estimate from the crawler's
load, and how the monitoring layers relate —
[`docs/disks.md`](../../docs/disks.md). Apply on the production server
(Ubuntu Server LTS, systemd; spec §10.1).

| File | Purpose |
|---|---|
| `smartd.conf` | the smartd config: NVMe checks, warnings to syslog/journald |
| `smartd-warning.sh` | the smartd warning handler (installed to `/usr/local/lib/crawler`) |
| `99-crawler-nvme.rules` | udev: the `/dev/nvme*` nodes — to the `nvme-smart` group |
| `crawler-monitor.service.d/` | a drop-in for the monitor unit: the supplementary group `nvme-smart` |

The layers: smartd (the package daemon, root) — critical health warnings,
growth of the error log, temperature; the `nvme_wear` alert of the monitor
service (T-080) — percentage_used ≥ 90% and the TBW in the details; it
works after access to the devices is granted (the rules + the drop-in).
Both write to journald.

## Installation

```bash
apt install smartmontools

# 1. smartd: our config instead of the package sample (keep the original)
cp -a /etc/smartd.conf /etc/smartd.conf.orig
install -m 0644 smartd.conf /etc/smartd.conf

# 2. the warning handler (the path — from smartd.conf, -M exec)
install -D -m 0755 smartd-warning.sh /usr/local/lib/crawler/smartd-warning.sh

# 3. the monitoring alert's access to the NVMe (layer 2, docs/disks.md)
groupadd -r nvme-smart
install -m 0644 99-crawler-nvme.rules /etc/udev/rules.d/99-crawler-nvme.rules
udevadm control --reload && udevadm trigger --subsystem-match=nvme
cp -r crawler-monitor.service.d /etc/systemd/system/
systemctl daemon-reload

# 4. start
systemctl enable --now smartmontools      # a unit alias: smartd.service
systemctl restart crawler-monitor
```

Step 3 and the drop-in can be skipped: the wear alert stays in the honest
"not measured" state, and smartd keeps working (see the layer boundaries
in docs/disks.md — on smartmontools 7.4, step 3 is needed for the TBW
threshold).

## Verification

```bash
systemctl status smartmontools            # active; devices found: 1
journalctl -t smartd -b                   # NVMe registration without warnings

# live readings — the same pages smartd and the monitor alert read
smartctl -H -A /dev/nvme0                 # health, percentage_used, TBW
smartctl -l error /dev/nvme0              # the NVMe error log

# the monitor layer's access (the alert opens the node for writing — ioctl admin)
ls -l /dev/nvme0                          # root:nvme-smart 0660
sudo -u crawler-monitor test -r /dev/nvme0 -a -w /dev/nvme0 && echo access ok
journalctl -t crawler-monitor | grep -i alert   # Warn when it trips (≥90%)
```

The alert's "not measured" state is visible at `CRAWLER_LOG_LEVEL=debug`
(the service's env): before access is enabled, every evaluation logs
Debug "alert not measured: NVMe wear"; after — no entry. A trip
(≥ 90% of the resource) is always logged as Warn.

Testing the handler: add the `-M test` directive to the DEVICESCAN line
in `/etc/smartd.conf` and reload the config (`systemctl kill -s HUP
smartmontools`) — an EmailTest entry (info) will appear in
`journalctl -t smartd`; remove the directive afterwards.
`smartd -d -q onecheck -c /etc/smartd.conf` performs one pass over the
devices and exits — handy for checking config edits without restarting
the daemon.

## Rollback

```bash
systemctl disable --now smartmontools
mv /etc/smartd.conf.orig /etc/smartd.conf
rm /usr/local/lib/crawler/smartd-warning.sh /etc/udev/rules.d/99-crawler-nvme.rules
rm -r /etc/systemd/system/crawler-monitor.service.d   # if there are no other drop-ins
udevadm control --reload && udevadm trigger --subsystem-match=nvme
systemctl daemon-reload && systemctl restart crawler-monitor
groupdel nvme-smart
```

## Configuration syntax check

On the development machine (with an NVMe) the config was accepted by
`smartd` (smartmontools 7.5): `smartd -d -q onecheck -c smartd.conf` —
the directives are accepted, the device registers, no warnings. The
directives used (`DEVICESCAN -d nvme`, `-H`, `-l error`, `-W`) are
identical for NVMe in 7.4 and 7.5 — the target Ubuntu 24.04 LTS (7.4)
takes the config without edits; the commented-out line with `-f`/`-p` is
for 7.5+ only (see the notes in `smartd.conf`).

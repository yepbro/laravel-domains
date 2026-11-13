#!/usr/bin/env python3
# T-092: analysis of the sampler's metrics.tsv — stage rates from counter
# deltas, memory/DB/storage. Counters are cumulative; a service restart
# zeroes them — such intervals are skipped (a delta < 0).
# The stage 35/36 columns (fb_*/rend_*/q_render_fb/m_fbworker): before the
# stage is deployed to production they are absent — zeros/"no data" in the
# fallback block is normal, not degradation; they do not tear stage deltas
# (they are not counted in restart skips). The column format — the header
# of t092-sample.sh.
#
#   ./t092-analyze.py <metrics.tsv> [--from ts --to ts]
import sys

COLS = ("ts load1 memavail_b psi_cpu psi_mem psi_io dns_tasks http_tasks "
        "render_tasks analyze_tasks "
        "fb_pub fb_cons fb_susp_gate fb_susp_budget "
        "rend_chromium rend_moli rend_lightpanda "
        "q_dns q_http q_render q_render_fb q_analyze "
        "backlog_pending backlog_queued db_size content_size disk_avail "
        "m_ingest m_sched m_dns m_http m_render m_analyzer m_monitor "
        "m_web m_dash m_chromium m_fbworker m_pg m_redis m_unbound").split()

def sanitize(v):
    # systemctl show may return "[not set]" (the unit is restarting),
    # the sampler may not have made it in time — interpreted tolerantly as 0, the row is not lost
    v = v.strip()
    try:
        return float(v)
    except ValueError:
        return 0.0

def load(path):
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            vals = line.split("\t")
            if len(vals) < len(COLS):
                continue
            rows.append([sanitize(v) for v in vals[:len(COLS)]])
    return rows

def idx(name):
    return COLS.index(name)

def fmt_bps(b):
    for unit in ("B/s", "KiB/s", "MiB/s", "GiB/s"):
        if abs(b) < 1024:
            return f"{b:,.1f} {unit}"
        b /= 1024
    return f"{b:,.1f} TiB/s"

def fmt_mem(b):
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(b) < 1024:
            return f"{b:,.0f} {unit}"
        b /= 1024
    return f"{b:,.2f} TiB"

def main():
    if len(sys.argv) < 2:
        sys.exit("usage: t092-analyze.py <metrics.tsv> [--from ts --to ts]")
    path = sys.argv[1]
    lo = hi = None
    args = sys.argv[2:]
    try:
        for i, a in enumerate(args):
            if a == "--from":
                lo = float(args[i + 1])
            if a == "--to":
                hi = float(args[i + 1])
    except (IndexError, ValueError):
        sys.exit("usage: t092-analyze.py <metrics.tsv> [--from ts --to ts]")
    rows = [r for r in load(path) if (lo is None or r[0] >= lo) and (hi is None or r[0] <= hi)]
    if len(rows) < 2:
        sys.exit("at least 2 samples are needed")

    i_ts = idx("ts")
    counters = [idx(c) for c in ("dns_tasks", "http_tasks", "render_tasks", "analyze_tasks")]
    sizes = [idx(c) for c in ("db_size", "content_size")]
    mems = [idx(c) for c in COLS if c.startswith("m_")]

    print(f"window: {rows[0][0]:.0f} … {rows[-1][0]:.0f} ({(rows[-1][0]-rows[0][0])/60:.1f} min, {len(rows)} samples)")

    # rates: the sum of counter deltas / the sum of valid interval time
    dt_total = 0.0
    d_sum = [0.0] * len(counters)
    skips = 0
    for a, b in zip(rows, rows[1:]):
        dt = b[i_ts] - a[i_ts]
        if dt <= 0:
            continue
        # a restart (a counter lower than the previous one) and a failed
        # sample (0 at a counter — the service was down at sample time) tear
        # the interval: the delta is unknown, the time does not join the sum
        if any(b[c] < a[c] for c in counters) or any(a[c] == 0 or b[c] == 0 for c in counters):
            skips += 1
            continue
        dt_total += dt
        for j, c in enumerate(counters):
            d_sum[j] += b[c] - a[c]
    if dt_total > 0:
        names = ("dns domains/s", "http domains/s", "render tasks/s", "analyze tasks/s")
        print(f"intervals skipped (restarts): {skips}")
        for name, s in zip(names, d_sum):
            print(f"  {name:16s} {s/dt_total:10.2f}")

    # the render fallback (stage 35/36): before the stage is deployed to
    # production the entities do not exist — zeros/no data is normal, not
    # degradation. The counters are per-process: a restart of worker A/B
    # zeroes its own — negative increments are ignored, the window is torn
    # (as with the main counters above). The fallback does not join the
    # restart skips of the main intervals: zero counters before the deploy
    # must not tear the stage deltas.
    fb_names = ("fb_pub", "fb_cons", "fb_susp_gate", "fb_susp_budget",
                "rend_chromium", "rend_moli", "rend_lightpanda")
    fb_cols = [idx(c) for c in fb_names]
    # deployed — by the fallback-specific columns: rend_* are already
    # non-zero for an engine-labeled build before the fallback itself is
    # enabled (T-235), they do not carry the deploy sign
    i_fbq = idx("q_render_fb")
    fb_d = [0.0] * len(fb_cols)
    for a, b in zip(rows, rows[1:]):
        if b[i_ts] - a[i_ts] <= 0:
            continue
        for j, c in enumerate(fb_cols):
            if b[c] > a[c]:
                fb_d[j] += b[c] - a[c]
    deployed = any(r[c] > 0 for r in rows for c in fb_cols[:4]) \
        or any(r[i_fbq] > 0 for r in rows)
    print("render fallback (stage 35/36):%s" % (
        "" if deployed else " zeros/no data — normal (before the stage deploy or an empty fallback)"))
    if dt_total > 0:
        print(f"  {'publications/s':14s} {fb_d[0]/dt_total:10.2f}")
        print(f"  {'consumptions/s':14s} {fb_d[1]/dt_total:10.2f}")
    rate = "—" if d_sum[2] <= 0 else f"{100.0 * fb_d[0] / d_sum[2]:.1f}%"
    print(f"  {'fallback rate':14s} {rate:>10s}  (publications / render stage tasks; the canary — the target is ≤15%)")
    print(f"  suspended gate/budget: +{fb_d[2]:,.0f} / +{fb_d[3]:,.0f}")
    print(f"  rendered by engines: chromium +{fb_d[4]:,.0f}, moli +{fb_d[5]:,.0f}, lightpanda +{fb_d[6]:,.0f}")

    # DB/storage growth (last - first)
    for name, c in zip(("DB", "content"), sizes):
        d = rows[-1][c] - rows[0][c]
        dt = rows[-1][i_ts] - rows[0][i_ts]
        print(f"  {name:8s} growth {d/1024/1024:12.1f} MiB in {dt/60:.1f} min = {fmt_bps(d/dt) if dt>0 else '—'}")

    # memory: min/avg/max per unit
    print("memory per unit (min / avg / max):")
    for c in mems:
        vals = [r[c] for r in rows if r[c] > 0]
        if not vals:
            print(f"  {COLS[c][2:]:12s} no data")
            continue
        print(f"  {COLS[c][2:]:12s} {fmt_mem(min(vals))} / {fmt_mem(sum(vals)/len(vals))} / {fmt_mem(max(vals))}")

    # queues and system metrics: the last sample + the maximum
    print("last sample: load1=%.2f memavail=%s psi_cpu=%.2f psi_mem=%.2f" % (
        rows[-1][idx("load1")], fmt_mem(rows[-1][idx("memavail_b")] * 1024),
        rows[-1][idx("psi_cpu")], rows[-1][idx("psi_mem")]))
    for q in ("q_dns", "q_http", "q_render", "q_render_fb", "q_analyze", "backlog_pending", "backlog_queued"):
        c = idx(q)
        print(f"  {q:16s} last={rows[-1][c]:,.0f}  max={max(r[c] for r in rows):,.0f}")

if __name__ == "__main__":
    main()

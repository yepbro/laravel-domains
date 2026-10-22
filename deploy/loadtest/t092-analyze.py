#!/usr/bin/env python3
# T-092: анализ metrics.tsv сэмплера — скорости этапов из дельт счётчиков,
# память/БД/хранилище. Счётчики кумулятивные; рестарт сервиса обнуляет —
# такие интервалы пропускаются (дельта < 0).
#
#   ./t092-analyze.py <metrics.tsv> [--from ts --to ts]
import sys

COLS = ("ts load1 memavail_b psi_cpu psi_mem psi_io dns_tasks http_tasks "
        "render_tasks analyze_tasks q_dns q_http q_render q_analyze "
        "backlog_pending backlog_queued db_size content_size disk_avail "
        "m_ingest m_sched m_dns m_http m_render m_analyzer m_monitor "
        "m_web m_dash m_chromium m_pg m_redis m_unbound").split()

def sanitize(v):
    # systemctl show может отдать «[not set]» (юнит перезапускается),
    # сэмплер мог не успеть — толерантно трактуем как 0, строка не теряется
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
        sys.exit("использование: t092-analyze.py <metrics.tsv> [--from ts --to ts]")
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
        sys.exit("использование: t092-analyze.py <metrics.tsv> [--from ts --to ts]")
    rows = [r for r in load(path) if (lo is None or r[0] >= lo) and (hi is None or r[0] <= hi)]
    if len(rows) < 2:
        sys.exit("нужно ≥2 замера")

    i_ts = idx("ts")
    counters = [idx(c) for c in ("dns_tasks", "http_tasks", "render_tasks", "analyze_tasks")]
    sizes = [idx(c) for c in ("db_size", "content_size")]
    mems = [idx(c) for c in COLS if c.startswith("m_")]

    print(f"окно: {rows[0][0]:.0f} … {rows[-1][0]:.0f} ({(rows[-1][0]-rows[0][0])/60:.1f} мин, {len(rows)} замеров)")

    # скорости: сумма дельт счётчиков / сумма времени валидных интервалов
    dt_total = 0.0
    d_sum = [0.0] * len(counters)
    skips = 0
    for a, b in zip(rows, rows[1:]):
        dt = b[i_ts] - a[i_ts]
        if dt <= 0:
            continue
        # рестарт (счётчик меньше прошлого) и провальный замер (0 у
        # счётчика — сервис лежал в момент сэмпла) интервал рвут: дельта
        # неизвестна, время в сумму не входит
        if any(b[c] < a[c] for c in counters) or any(a[c] == 0 or b[c] == 0 for c in counters):
            skips += 1
            continue
        dt_total += dt
        for j, c in enumerate(counters):
            d_sum[j] += b[c] - a[c]
    if dt_total > 0:
        names = ("dns дом/с", "http дом/с", "render зап/с", "analyze зап/с")
        print(f"интервалов пропущено (рестарты): {skips}")
        for name, s in zip(names, d_sum):
            print(f"  {name:16s} {s/dt_total:10.2f}")

    # рост БД/хранилища (последний - первый)
    for name, c in zip(("БД", "контент"), sizes):
        d = rows[-1][c] - rows[0][c]
        dt = rows[-1][i_ts] - rows[0][i_ts]
        print(f"  рост {name:8s} {d/1024/1024:12.1f} MiB за {dt/60:.1f} мин = {fmt_bps(d/dt) if dt>0 else '—'}")

    # память: min/avg/max по юнитам
    print("память по юнитам (min / avg / max):")
    for c in mems:
        vals = [r[c] for r in rows if r[c] > 0]
        if not vals:
            print(f"  {COLS[c][2:]:12s} нет данных")
            continue
        print(f"  {COLS[c][2:]:12s} {fmt_mem(min(vals))} / {fmt_mem(sum(vals)/len(vals))} / {fmt_mem(max(vals))}")

    # очереди и системные: последний замер + максимум
    print("последний замер: load1=%.2f memavail=%s psi_cpu=%.2f psi_mem=%.2f" % (
        rows[-1][idx("load1")], fmt_mem(rows[-1][idx("memavail_b")] * 1024),
        rows[-1][idx("psi_cpu")], rows[-1][idx("psi_mem")]))
    for q in ("q_dns", "q_http", "q_render", "q_analyze", "backlog_pending", "backlog_queued"):
        c = idx(q)
        print(f"  {q:16s} last={rows[-1][c]:,.0f}  max={max(r[c] for r in rows):,.0f}")

if __name__ == "__main__":
    main()

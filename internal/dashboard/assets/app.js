/*
 * app.js — the operator dashboard client (dashboard spec §5, §6.4–6.5, T-130):
 * periodic polling of GET /api/status and rendering of the snapshot sections.
 * No dependencies or build (dashboard spec §3.2.2 — an environment without the
 * internet; CSP §6.3 allows only same-origin scripts and connections), the
 * landing.js style.
 *
 *  - the poll period — meta[name=dashboard-refresh] in page.html (seconds,
 *    default 5); it does not have to match the server's dashboard.refresh:
 *    the client shows the last ready snapshot;
 *  - sparklines — an SVG polyline over the in-memory polling history
 *    (~120 points, ≈10 minutes at 5 s) for the three rates and queue depths;
 *  - 401 — stop polling and location.reload(): fetch does not show the
 *    browser's Basic dialog, and a repeated request for the document itself
 *    receives WWW-Authenticate — the browser offers a re-login (it will
 *    resend the cached credentials silently) — §6.4;
 *  - "n/a" — degraded sections (null + the errors map, §5.9), values without
 *    a window (rate/ETA) and "until the first snapshot" (503).
 *
 * All DOM values are written via textContent/createElement — no markup comes
 * from the data; dynamic styles (bar widths, segment colors) — only CSSOM
 * property assignment, which CSP style-src does not block.
 */
(function () {
    'use strict';

    var ND = 'n/a';
    var HISTORY_LIMIT = 120; // ~10 minutes of history at a 5 s period (§6.4)

    // The status legend of spec 6.2 in the model.Statuses order (the snapshot's
    // statuses key order is the same; here — the band order and the ru labels;
    // an unknown status gets a fallback color and its own name).
    var STATUS_ORDER = ['new', 'not_delegated', 'delegated_no_address', 'no_http',
        'alive', 'temp_error', 'deleted', 'invalid'];
    var STATUS_LABEL = {
        new: 'new',
        not_delegated: 'not delegated',
        delegated_no_address: 'delegated, no addresses',
        no_http: 'no web',
        alive: 'alive',
        temp_error: 'temp error',
        deleted: 'deleted',
        invalid: 'junk'
    };
    var STATUS_COLOR = {
        new: '#58a6ff',
        not_delegated: '#8957e5',
        delegated_no_address: '#bf8700',
        no_http: '#d29922',
        alive: '#3fb950',
        temp_error: '#f0883e',
        deleted: '#8b949e',
        invalid: '#f85149'
    };
    var STATUS_FALLBACK_COLOR = '#6e7681';

    // Working streams in the output order (status.Streams ← queue.AllStreams,
    // spec 4.2; q:render-fallback — the render fallback queue, T-237); q:dead —
    // a separate row with its own highlight (dashboard spec §5.4).
    var STREAM_DEAD = 'q:dead';
    var STREAMS = ['q:dns', 'q:http', 'q:render', 'q:render-fallback', 'q:analyze', STREAM_DEAD];

    // The queue highlight threshold: a working stream's depth is considered
    // approaching its effective watermark from 90% (§5.4 "approaching").
    // The thresholds arrive in the snapshot's queue_watermarks — per stream,
    // clamped by MAXLEN (like the monitor's alert).
    var WATERMARK_WARN = 0.9;

    var SVG_NS = 'http://www.w3.org/2000/svg';

    // ------------------------------------------------------------ poll period

    var refreshSecs = 5;
    var meta = document.querySelector('meta[name="dashboard-refresh"]');
    if (meta) {
        var secs = parseInt(meta.getAttribute('content'), 10);
        if (secs > 0) refreshSecs = secs;
    }
    var refreshLabel = document.getElementById('refresh-secs');
    if (refreshLabel) refreshLabel.textContent = String(refreshSecs);

    // ------------------------------------------------------------ history

    var hist = {
        dps: [],
        rps: [],
        renders: [],
        queues: {} // stream → [depth]
    };

    // ------------------------------------------------------------ utilities

    function el(tag, cls, text) {
        var node = document.createElement(tag);
        if (cls) node.className = cls;
        if (text !== undefined) node.textContent = text;
        return node;
    }

    function num(v) {
        return typeof v === 'number' && isFinite(v);
    }

    // fmtInt — thousands grouping ("1,234,567"); null/undefined — "n/a".
    function fmtInt(v) {
        if (!num(v)) return ND;
        return v.toLocaleString('en-US');
    }

    // fmtRate — a rate: large values as an integer, small ones with up to
    // three decimals.
    function fmtRate(v) {
        if (!num(v)) return ND;
        if (v >= 100) return Math.round(v).toLocaleString('en-US');
        if (v >= 1) return v.toFixed(1);
        return v.toFixed(3);
    }

    // fmtPct — percentages with a decimal comma.
    function fmtPct(v) {
        return v.toFixed(1) + '%';
    }

    // fmtBytes — human-readable bytes (1.2 GiB / 850 MiB / 512 KiB), like the
    // monitor's alertBytes; null/undefined — "n/a".
    function fmtBytes(v) {
        if (!num(v)) return ND;
        var units = [[1 << 30, 'GiB'], [1 << 20, 'MiB'], [1 << 10, 'KiB']];
        for (var i = 0; i < units.length; i++) {
            if (v >= units[i][0]) {
                return (v / units[i][0]).toFixed(1) + ' ' + units[i][1];
            }
        }
        return v + ' B';
    }

    // fmtDur — a human-readable duration, like the CLI's status.FormatDuration:
    // "45s", "13m", "5h 20m", "3d 4h".
    function fmtDur(sec) {
        if (!num(sec)) return ND;
        if (sec < 60) return sec + 's';
        if (sec < 3600) return Math.round(sec / 60) + 'm';
        if (sec < 86400) return Math.floor(sec / 3600) + 'h ' +
            String(Math.floor((sec % 3600) / 60)).padStart(2, '0') + 'm';
        return Math.floor(sec / 86400) + 'd ' + Math.floor((sec % 86400) / 3600) + 'h';
    }

    // fmtTime — the snapshot's RFC3339 stamp into the operator's local time.
    function fmtTime(iso) {
        if (!iso) return ND;
        var d = new Date(iso);
        if (isNaN(d.getTime())) return ND;
        return d.toLocaleString('en-US');
    }

    function setVal(id, text, title) {
        var node = document.getElementById(id);
        if (!node) return;
        node.textContent = text;
        if (title) {
            node.title = title;
        } else {
            node.removeAttribute('title');
        }
    }

    // errTitle — the reason a section degraded, for the "n/a" caption (§5.9).
    function errTitle(snap, section) {
        var reason = snap && snap.errors ? snap.errors[section] : undefined;
        return reason ? ('section unavailable: ' + reason) : undefined;
    }

    // ndRow — the "n/a" placeholder row of a table section.
    function ndRow(body, colSpan, title) {
        var row = el('tr');
        var td = el('td', 'nd', ND);
        if (title) td.title = title;
        td.colSpan = colSpan;
        row.appendChild(td);
        body.appendChild(row);
    }

    // ------------------------------------------------------------ sparklines

    // drawSpark — a polyline over the history: normalized into the container's
    // viewBox coordinate system; fewer than two points — empty (the tape has
    // just started). A flat series is drawn at the center (min == max).
    function drawSpark(svg, values) {
        if (!svg) return;
        var poly = svg.querySelector('polyline');
        if (!poly) return;
        if (!values || values.length < 2) {
            poly.setAttribute('points', '');
            return;
        }
        var parts = (svg.getAttribute('viewBox') || '0 0 120 28').split(' ');
        var w = parseFloat(parts[2]) || 120;
        var h = parseFloat(parts[3]) || 28;
        var pad = 2;
        var min = Infinity;
        var max = -Infinity;
        values.forEach(function (v) {
            if (v < min) min = v;
            if (v > max) max = v;
        });
        if (min === max) {
            min -= 1;
            max += 1;
        }
        var points = [];
        for (var i = 0; i < values.length; i++) {
            var x = pad + (i / (values.length - 1)) * (w - 2 * pad);
            var y = h - pad - ((values[i] - min) / (max - min)) * (h - 2 * pad);
            points.push(x.toFixed(1) + ',' + y.toFixed(1));
        }
        poly.setAttribute('points', points.join(' '));
    }

    // pushSeries — a point into the history (~120 latest); null skips a gap
    // (the section degraded — no point).
    function pushSeries(series, v) {
        if (!num(v)) return;
        series.push(v);
        if (series.length > HISTORY_LIMIT) series.shift();
    }

    // ------------------------------------------------------------ cards (§5.1–5.2)

    function renderCards(snap) {
        var tot = snap.totals;
        var zonesErr = errTitle(snap, 'zones');
        var bar = document.getElementById('bar-checked');

        if (tot) {
            var pct = tot.domains_total > 0
                ? (tot.domains_checked / tot.domains_total) * 100 : 0;
            setVal('v-total', fmtInt(tot.domains_total));
            setVal('v-checked', fmtInt(tot.domains_checked));
            setVal('v-checked-sub', fmtPct(pct) + ' of ' + fmtInt(tot.domains_total));
            if (bar) bar.style.width = Math.min(100, pct) + '%';
            setVal('v-inwork', fmtInt(tot.pending + tot.queued));
        } else {
            setVal('v-total', ND, zonesErr);
            setVal('v-checked', ND, zonesErr);
            setVal('v-checked-sub', '—');
            setVal('v-inwork', ND, zonesErr);
            if (bar) bar.style.width = '0%';
        }

        var rates = snap.rates;
        var ratesErr = zonesErr || errTitle(snap, 'throughput') ||
            'the rates are computed from the zones and throughput sections — one is unavailable';
        if (rates) {
            setVal('v-dps', rates.domains_per_sec != null
                ? fmtRate(rates.domains_per_sec) : ND,
            rates.domains_per_sec != null
                ? undefined
                : 'the daily window is shorter than 10 minutes — the rate is not yet determined');
            setVal('v-rps', fmtRate(rates.results_per_sec));
            setVal('v-renders', fmtRate(rates.renders_per_sec));
            setVal('v-eta', rates.eta_sec != null ? fmtDur(rates.eta_sec) : ND,
                rates.eta_sec != null ? undefined : 'the rate is unknown — the ETA is not computed');
            setVal('v-eta-sub', 'first-cycle remainder: ' + fmtInt(rates.remaining));
        } else {
            setVal('v-dps', ND, ratesErr);
            setVal('v-rps', ND, ratesErr);
            setVal('v-renders', ND, ratesErr);
            setVal('v-eta', ND, ratesErr);
            setVal('v-eta-sub', 'first-cycle remainder: ' + ND);
        }

        pushSeries(hist.dps, rates ? rates.domains_per_sec : null);
        pushSeries(hist.rps, rates ? rates.results_per_sec : null);
        pushSeries(hist.renders, rates ? rates.renders_per_sec : null);
        drawSpark(document.getElementById('spark-dps'), hist.dps);
        drawSpark(document.getElementById('spark-rps'), hist.rps);
        drawSpark(document.getElementById('spark-renders'), hist.renders);
    }

    // ------------------------------------------------------------ Laravel card (§5.6)

    function renderLaravelCard(snap) {
        var l = snap.laravel;
        if (l) {
            setVal('v-laravel', fmtInt(l.total));
            setVal('v-laravel-sub', 'last 24 h: ' + fmtInt(l.last_24h));
        } else {
            setVal('v-laravel', ND, errTitle(snap, 'laravel'));
            setVal('v-laravel-sub', 'last 24 h: ' + ND);
        }
    }

    // ------------------------------------------------------------ queues (§5.4)

    function renderQueues(snap) {
        var body = document.getElementById('queues-body');
        var hint = document.getElementById('queue-hint');
        if (!body) return;
        body.textContent = '';

        var queues = snap.queues;

        // Effective backpressure thresholds — per stream (queue_watermarks:
        // clamped by MAXLEN, one calculation shared with the gate and the
        // monitor's alert). A sum of depths against a single threshold is
        // meaningless: the stream thresholds differ, and so do the depth
        // units (q:dns is chunks). The hint is configuration data: visible
        // to the operator even when the depths are "n/a" (a degraded queues
        // section).
        var watermarks = snap.queue_watermarks || {};
        var wmParts = [];
        STREAMS.forEach(function (s) {
            if (s === STREAM_DEAD) return;
            var wm = watermarks[s] || 0;
            if (wm > 0) {
                // q:dns is counted in chunks, the per-item streams in records.
                wmParts.push(s + ' ' + fmtInt(wm) +
                    (s === 'q:dns' ? ' chunks' : ''));
            }
        });
        if (hint) {
            hint.textContent = wmParts.length
                ? 'backpressure thresholds: ' + wmParts.join(' · ')
                : '';
        }

        if (!queues) {
            ndRow(body, 3, errTitle(snap, 'queues'));
            return;
        }

        var streams = STREAMS.slice();
        Object.keys(queues).forEach(function (s) {
            if (streams.indexOf(s) < 0) streams.push(s); // do not drop new streams
        });

        streams.forEach(function (s) {
            var depth = queues[s] || 0;
            var series = hist.queues[s] || (hist.queues[s] = []);
            pushSeries(series, depth);

            var row = el('tr');
            if (s !== STREAM_DEAD) {
                // Highlighting — by each working stream's own threshold
                // (q:dead is not driven by the gate: dead tasks are an
                // operator triage matter, not pipeline lag).
                var wm = watermarks[s] || 0;
                if (wm > 0) {
                    if (depth >= wm) {
                        row.className = 'q-over'; // at the threshold: backpressure is imminent
                    } else if (depth >= WATERMARK_WARN * wm) {
                        row.className = 'q-warn'; // approaching (≥90%)
                    }
                }
            }

            var dead = s === STREAM_DEAD && depth > 0;
            var name = el('td', dead ? 'q-dead-nonzero' : null, s);
            if (dead) name.title = 'dead tasks: operator triage required';
            row.appendChild(name);

            var depthTd = el('td', 'num' + (dead ? ' q-dead-nonzero' : ''), fmtInt(depth));
            row.appendChild(depthTd);

            var sparkTd = el('td');
            var svg = document.createElementNS(SVG_NS, 'svg');
            svg.setAttribute('class', 'spark spark-row');
            svg.setAttribute('viewBox', '0 0 120 20');
            svg.setAttribute('preserveAspectRatio', 'none');
            var poly = document.createElementNS(SVG_NS, 'polyline');
            poly.setAttribute('vector-effect', 'non-scaling-stroke');
            svg.appendChild(poly);
            sparkTd.appendChild(svg);
            row.appendChild(sparkTd);

            body.appendChild(row);
            drawSpark(svg, series);
        });
    }

    // ------------------------------------------------------------ render engines (render spec §4.6)

    // renderRender — the engines panel: the rate/success/tarpit rate/RSS per
    // engine (the stage's metric labels, T-235/S-P2-4) and the fallback — the
    // fallback rate (the published share of stage pages), consumed/depth of
    // q:render-fallback. A null section — the addresses are not set ("n/a"
    // without an error) or the exposures are unreachable (the reason is in
    // errors).
    function renderRender(snap) {
        var body = document.getElementById('render-engines-body');
        var fb = document.getElementById('render-fallback');
        var hint = document.getElementById('render-hint');
        if (!body || !fb) return;
        body.textContent = '';
        fb.textContent = '';

        var r = snap.render;
        if (!r) {
            ndRow(body, 7, errTitle(snap, 'render'));
            var nd = el('span', 'nd', ND);
            if (!snap.errors || !snap.errors.render) {
                nd.title = 'exposure addresses are not set (dashboard.render_metrics_urls is empty)';
            } else {
                nd.title = errTitle(snap, 'render');
            }
            fb.appendChild(nd);
            if (hint) hint.textContent = '';
            return;
        }

        if (!r.engines || !r.engines.length) {
            ndRow(body, 7);
            body.lastChild.firstChild.textContent = 'workers without renders';
        } else {
            r.engines.forEach(function (e) {
                var row = el('tr');
                row.appendChild(el('td', 'mono', e.engine));
                row.appendChild(el('td', 'num', fmtRate(e.rendered_per_sec)));
                row.appendChild(el('td', 'num', fmtInt(e.rendered_total)));
                row.appendChild(el('td', 'num', fmtInt(e.finalized_total)));
                var pct = el('td', 'num',
                    num(e.success_pct) ? fmtPct(e.success_pct) : '—');
                if (!num(e.success_pct)) {
                    pct.title = 'no engine-attributed finalizations yet';
                } else {
                    pct.title = 'the share of successful among the engine-labeled finalizations ' +
                        '(engine-independent DNS/cert finalizations are excluded)';
                }
                row.appendChild(pct);
                // Tarpit rate — the share of the engine's tarpit finalizations
                // (T-148 — an engine-dependent rejection, S-P2-4); the tooltip
                // names the counter.
                var tar = el('td', 'num',
                    num(e.tarpit_pct) ? fmtPct(e.tarpit_pct) : '—');
                if (num(e.tarpit_pct)) {
                    tar.title = fmtInt(e.tarpit_total || 0) +
                        ' tarpit finalizations out of the engine finalizations since the worker restart';
                } else {
                    tar.title = 'no engine-attributed finalizations yet';
                }
                row.appendChild(tar);
                // RSS — the sum of the engine workers' memory (S-P3-2); 0 —
                // an exposure without the engine label (a build older than
                // T-235).
                var rss = el('td', 'num', e.rss_bytes > 0 ? fmtBytes(e.rss_bytes) : '—');
                rss.title = e.rss_bytes > 0
                    ? 'process_resident_memory_bytes of the engine workers'
                    : 'RSS unavailable (the exposure has no engine label)';
                row.appendChild(rss);
                body.appendChild(row);
            });
        }

        // The fallback: the fallback rate (the published share of stage pages —
        // the spec §5.3 checkpoints), published (worker A) / consumed by
        // outcome (worker B) / the depth of q:render-fallback. The rates are 0
        // until the first pair of passes; comparing the publication and
        // consumption rates diagnoses "worker B cannot keep up" (the
        // render_fallback_depth alert). The consumed outcomes go through || 0:
        // the series are pre-created with zeros since T-250; an old exposure
        // without a series is 0, not "n/a" (S-P3-2).
        var f = r.fallback || {};
        var by = f.consumed_by_result || {};
        var parts = [];
        parts.push('fallback: published ' + fmtInt(f.published_total) +
            ' (' + fmtRate(f.published_per_sec) + '/s)');
        if (num(f.rate_pct)) {
            parts.push('fallback rate ' + fmtPct(f.rate_pct) +
                ' of stage pages');
        } else {
            parts.push('fallback rate —');
        }
        parts.push('consumed ' + fmtInt(f.consumed_total) +
            ' (' + fmtRate(f.consumed_per_sec) + '/s) — reworked ' + fmtInt(by.rendered || 0) +
            ' / finalized ' + fmtInt(by.finalized || 0) +
            ' / skipped ' + fmtInt(by.skipped || 0));
        var depth = el('span');
        if (num(f.depth)) {
            depth.appendChild(document.createTextNode('q:render-fallback depth: ' + fmtInt(f.depth)));
        } else {
            var dnd = el('span', 'nd', 'q:render-fallback depth: ' + ND);
            dnd.title = 'the depth is unavailable (Redis)';
            depth.appendChild(dnd);
        }
        fb.appendChild(document.createTextNode(parts.join(' · ')));
        fb.appendChild(document.createTextNode(' · '));
        fb.appendChild(depth);
        if (hint) {
            hint.textContent = 'success/tarpit rate — by the stage metrics engine label; ' +
                'cumulatives — since the worker restarts; the fallback rate — published/stage pages';
        }
    }

    // ------------------------------------------------------------ zones (§5.3)

    function renderZones(snap) {
        var body = document.getElementById('zones-body');
        if (!body) return;
        body.textContent = '';

        if (!snap.zones) {
            ndRow(body, 8, errTitle(snap, 'zones'));
            return;
        }
        if (snap.zones.length === 0) {
            ndRow(body, 8);
            body.lastChild.firstChild.textContent = 'no zones';
            return;
        }

        snap.zones.forEach(function (z) {
            var row = el('tr');
            var name = el('td', 'mono', z.name);
            if (z.name_unicode && z.name_unicode !== z.name) {
                name.title = z.name_unicode; // the unicode form is display-only (§5.3)
            }
            row.appendChild(name);

            var state = el('td');
            state.appendChild(el('span',
                z.active ? 'badge badge-on' : 'badge badge-off',
                z.active ? 'active' : 'paused'));
            row.appendChild(state);

            row.appendChild(el('td', 'num', fmtInt(z.domains_total)));
            row.appendChild(el('td', 'num', fmtInt(z.domains_checked)));

            var progress = el('td');
            var pct = z.domains_total > 0
                ? (z.domains_checked / z.domains_total) * 100 : 0;
            var wrap = el('div', 'bar bar-sm');
            var fill = el('div', 'bar-fill');
            fill.style.width = Math.min(100, pct) + '%';
            wrap.appendChild(fill);
            progress.appendChild(wrap);
            progress.appendChild(el('span', 'pct', fmtPct(pct)));
            row.appendChild(progress);

            row.appendChild(el('td', 'num', fmtInt(z.pending)));
            row.appendChild(el('td', 'num', fmtInt(z.queued)));
            row.appendChild(el('td', 'num', fmtInt(z.done)));

            body.appendChild(row);
        });
    }

    // ------------------------------------------------------------ statuses (§5.5)

    function renderStatuses(snap) {
        var barEl = document.getElementById('statusbar');
        var legend = document.getElementById('statuslegend');
        if (!barEl || !legend) return;
        barEl.textContent = '';
        legend.textContent = '';

        var statuses = snap.statuses;
        if (!statuses) {
            var nd = el('span', 'nd', ND);
            var reason = errTitle(snap, 'statuses');
            if (reason) nd.title = reason;
            barEl.appendChild(nd);
            return;
        }

        var names = STATUS_ORDER.slice();
        Object.keys(statuses).forEach(function (k) {
            if (names.indexOf(k) < 0) names.push(k);
        });

        var total = 0;
        names.forEach(function (n) { total += statuses[n] || 0; });

        names.forEach(function (n) {
            var count = statuses[n] || 0;
            var color = STATUS_COLOR[n] || STATUS_FALLBACK_COLOR;
            var label = STATUS_LABEL[n] || n;

            if (count > 0) {
                var seg = el('div', 'seg');
                seg.style.background = color; // CSSOM: CSP-safe
                seg.style.flexGrow = String(count / total);
                seg.title = label + ': ' + fmtInt(count);
                barEl.appendChild(seg);
            }

            var item = el('span');
            var swatch = el('span', 'swatch');
            swatch.style.background = color;
            item.appendChild(swatch);
            item.appendChild(document.createTextNode(
                label + ' · ' + fmtInt(count)));
            legend.appendChild(item);
        });

        var summary = el('span');
        summary.appendChild(document.createTextNode('total · ' + fmtInt(total)));
        legend.appendChild(summary);
    }

    // ------------------------------------------------------------ finds (§5.6)

    function renderLaravel(snap) {
        var body = document.getElementById('laravel-body');
        if (!body) return;
        body.textContent = '';

        var l = snap.laravel;
        if (!l) {
            ndRow(body, 5, errTitle(snap, 'laravel'));
            return;
        }
        if (!l.recent || !l.recent.length) {
            ndRow(body, 5);
            body.lastChild.firstChild.textContent = 'no finds yet';
            return;
        }

        l.recent.forEach(function (it) {
            var row = el('tr');
            row.appendChild(el('td', 'mono', it.domain));
            row.appendChild(el('td', null, it.zone));
            row.appendChild(el('td', 'num',
                num(it.score) ? String(it.score.toFixed(3)).replace('.', ',') : ND));
            var hasVer = !!(it.ver_min || it.ver_max);
            row.appendChild(el('td', null,
                hasVer ? (it.ver_min || '?') + '–' + (it.ver_max || '?') : '—'));
            row.appendChild(el('td', 'hint-time', fmtTime(it.updated_at)));
            body.appendChild(row);
        });
    }

    // ------------------------------------------------------------ ingest (§5.7)

    function renderIngest(snap) {
        var body = document.getElementById('ingest-body');
        if (!body) return;
        body.textContent = '';

        if (!snap.ingest) {
            ndRow(body, 8, errTitle(snap, 'ingest'));
            return;
        }
        if (snap.ingest.length === 0) {
            ndRow(body, 8);
            body.lastChild.firstChild.textContent = 'no files yet';
            return;
        }

        snap.ingest.forEach(function (f) {
            var row = el('tr');
            row.appendChild(el('td', 'mono', f.path));
            row.appendChild(el('td', null, f.action || '—'));
            row.appendChild(el('td', 'num', fmtInt(f.lines_total)));
            row.appendChild(el('td', 'num', fmtInt(f.added)));
            row.appendChild(el('td', 'num', fmtInt(f.existed)));
            row.appendChild(el('td', 'num', fmtInt(f.deleted)));
            row.appendChild(el('td', 'num', fmtInt(f.rejected)));
            row.appendChild(el('td', 'hint-time', fmtTime(f.processed_at)));
            body.appendChild(row);
        });
    }

    // ------------------------------------------------------------ health (§5.8)

    function renderHealth(snap) {
        var box = document.getElementById('health');
        if (!box) return;
        box.textContent = '';

        if (!snap.health) {
            // Not an error: no probes are set (dashboard.probes is empty, §5.9).
            var nd = el('span', 'nd', ND);
            nd.title = 'no probes set (dashboard.probes is empty)';
            box.appendChild(nd);
            return;
        }

        snap.health.forEach(function (p) {
            var chip = el('span', 'chip' + (p.up ? '' : ' down'));
            chip.appendChild(el('span', 'dot'));
            chip.appendChild(document.createTextNode(p.name));
            if (!p.up && p.error) chip.title = p.error; // the down reason
            box.appendChild(chip);
        });
    }

    // ------------------------------------------------------------ poll loop

    function setConn(text, cls) {
        var conn = document.getElementById('conn');
        if (!conn) return;
        conn.textContent = text;
        conn.className = 'conn' + (cls ? ' ' + cls : '');
    }

    function render(snap) {
        renderCards(snap);
        renderLaravelCard(snap);
        renderQueues(snap);
        renderRender(snap);
        renderZones(snap);
        renderStatuses(snap);
        renderLaravel(snap);
        renderIngest(snap);
        renderHealth(snap);
        setVal('built-at', 'snapshot: ' + fmtTime(snap.built_at));
    }

    var timer = null;

    function poll() {
        fetch('/api/status', {headers: {Accept: 'application/json'}})
            .then(function (resp) {
                if (resp.status === 401) {
                    // §6.4: the session/credentials no longer pass. fetch does
                    // not trigger the browser's Basic dialog — reloading the
                    // page moves the 401 onto the document itself: the browser
                    // either silently resends the cached credentials or shows
                    // the re-login dialog.
                    if (timer) clearInterval(timer);
                    setConn('re-login required…', 'conn-err');
                    location.reload();
                    return null;
                }
                if (resp.status === 503) {
                    // §5.9: the first snapshot has not been collected yet.
                    setConn('snapshot is being prepared…', 'conn-wait');
                    return null;
                }
                if (!resp.ok) {
                    setConn('refresh error: HTTP ' + resp.status, 'conn-err');
                    return null;
                }
                return resp.json().catch(function () { return null; });
            })
            .then(function (snap) {
                if (!snap) return;
                try {
                    render(snap);
                    setConn('updated ' + new Date().toLocaleTimeString('en-US'), 'conn-ok');
                } catch (e) {
                    setConn('snapshot render error', 'conn-err');
                }
            })
            .catch(function () {
                setConn('no connection to the dashboard', 'conn-err');
            });
    }

    poll();
    timer = setInterval(poll, refreshSecs * 1000);
})();

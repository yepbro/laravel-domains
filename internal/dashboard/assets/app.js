/*
 * app.js — клиент операторского дашборда (ТЗД §5, §6.4–6.5, T-130):
 * периодический опрос GET /api/status и рендер секций снимка. Без
 * зависимостей и сборки (ТЗД §3.2.2 — окружение без интернета; CSP
 * §6.3 пускает только свои скрипты и соединения), стиль landing.js.
 *
 *  - период опроса — meta[name=dashboard-refresh] в page.html (секунды,
 *    дефолт 5); он не обязан совпадать с dashboard.refresh сервера:
 *    клиент показывает последний готовый снимок;
 *  - спарклайны — SVG polyline по истории опросов в памяти (~120 точек,
 *    ≈10 минут при 5 с) для трёх скоростей и глубин очередей;
 *  - 401 — остановка опроса и location.reload(): fetch не показывает
 *    браузерный Basic-диалог, а повторный запрос самого документа
 *    получает WWW-Authenticate — браузер предлагает повторный вход
 *    (кэшированные креды переотправит молча) — §6.4;
 *  - «н/д» — деградировавшие секции (null + карта errors, §5.9),
 *    значения без окна (скорость/ETA) и «до первого снимка» (503).
 *
 * Все значения DOM пишутся через textContent/createElement — разметки
 * из данных нет; динамические стили (ширины баров, цвета сегментов) —
 * только присвоением свойств CSSOM, которое CSP style-src не блокирует.
 */
(function () {
    'use strict';

    var ND = 'н/д';
    var HISTORY_LIMIT = 120; // ~10 минут истории при периоде 5 с (§6.4)

    // Легенда статусов ТЗ 6.2 в порядке model.Statuses (порядок ключей
    // statuses снимка тот же; здесь — порядок полосы и ru-подписи,
    // неизвестный статус получает резервный цвет и своё имя).
    var STATUS_ORDER = ['new', 'not_delegated', 'delegated_no_address', 'no_http',
        'alive', 'temp_error', 'deleted', 'invalid'];
    var STATUS_LABEL = {
        new: 'новый',
        not_delegated: 'не делегирован',
        delegated_no_address: 'делегирован без адресов',
        no_http: 'без веба',
        alive: 'жив',
        temp_error: 'врем. ошибка',
        deleted: 'удалён',
        invalid: 'мусор'
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

    // Рабочие стримы в порядке вывода (status.Streams, ТЗ 4.2); q:dead —
    // отдельная строка с собственной подсветкой (ТЗД §5.4).
    var STREAM_DEAD = 'q:dead';
    var STREAMS = ['q:dns', 'q:http', 'q:render', 'q:analyze', STREAM_DEAD];

    // Порог подсветки очередей: сумма рабочих стримов считается
    // приближающейся к watermark от 90 % (§5.4 «при приближении»).
    var WATERMARK_WARN = 0.9;

    var SVG_NS = 'http://www.w3.org/2000/svg';

    // ------------------------------------------------------------ период опроса

    var refreshSecs = 5;
    var meta = document.querySelector('meta[name="dashboard-refresh"]');
    if (meta) {
        var secs = parseInt(meta.getAttribute('content'), 10);
        if (secs > 0) refreshSecs = secs;
    }
    var refreshLabel = document.getElementById('refresh-secs');
    if (refreshLabel) refreshLabel.textContent = String(refreshSecs);

    // ------------------------------------------------------------ история

    var hist = {
        dps: [],
        rps: [],
        renders: [],
        queues: {} // stream → [глубина]
    };

    // ------------------------------------------------------------ утилиты

    function el(tag, cls, text) {
        var node = document.createElement(tag);
        if (cls) node.className = cls;
        if (text !== undefined) node.textContent = text;
        return node;
    }

    function num(v) {
        return typeof v === 'number' && isFinite(v);
    }

    // fmtInt — группировка разрядов («1 234 567»); null/undefined — «н/д».
    function fmtInt(v) {
        if (!num(v)) return ND;
        return v.toLocaleString('ru-RU');
    }

    // fmtRate — скорость: крупная — целым, мелкая — до трёх знаков.
    function fmtRate(v) {
        if (!num(v)) return ND;
        if (v >= 100) return Math.round(v).toLocaleString('ru-RU');
        if (v >= 1) return String(v.toFixed(1)).replace('.', ',');
        return String(v.toFixed(3)).replace('.', ',');
    }

    // fmtPct — проценты с запятой.
    function fmtPct(v) {
        return String(v.toFixed(1)).replace('.', ',') + '%';
    }

    // fmtDur — человекочитаемый интервал, как status.FormatDuration CLI:
    // «45с», «13м», «5ч 20м», «3д 4ч».
    function fmtDur(sec) {
        if (!num(sec)) return ND;
        if (sec < 60) return sec + ' с';
        if (sec < 3600) return Math.round(sec / 60) + ' м';
        if (sec < 86400) return Math.floor(sec / 3600) + ' ч ' +
            String(Math.floor((sec % 3600) / 60)).padStart(2, '0') + ' м';
        return Math.floor(sec / 86400) + ' д ' + Math.floor((sec % 86400) / 3600) + ' ч';
    }

    // fmtTime — RFC3339 снимка в локальное время оператора.
    function fmtTime(iso) {
        if (!iso) return ND;
        var d = new Date(iso);
        if (isNaN(d.getTime())) return ND;
        return d.toLocaleString('ru-RU');
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

    // errTitle — причина деградации секции для подписи «н/д» (§5.9).
    function errTitle(snap, section) {
        var reason = snap && snap.errors ? snap.errors[section] : undefined;
        return reason ? ('секция недоступна: ' + reason) : undefined;
    }

    // ndRow — строка-плейсхолдер «н/д» секции таблицы.
    function ndRow(body, colSpan, title) {
        var row = el('tr');
        var td = el('td', 'nd', ND);
        if (title) td.title = title;
        td.colSpan = colSpan;
        row.appendChild(td);
        body.appendChild(row);
    }

    // ------------------------------------------------------------ спарклайны

    // drawSpark — polyline по истории: нормировка в систему координат
    // viewBox контейнера; меньше двух точек — пусто (лента только
    // началась). Плоский ряд рисуется по центру (min == max).
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

    // pushSeries — точка в историю (~120 последних); null пропускает
    // разрыв (секция деградировала — точки нет).
    function pushSeries(series, v) {
        if (!num(v)) return;
        series.push(v);
        if (series.length > HISTORY_LIMIT) series.shift();
    }

    // ------------------------------------------------------------ карточки (§5.1–5.2)

    function renderCards(snap) {
        var tot = snap.totals;
        var zonesErr = errTitle(snap, 'zones');
        var bar = document.getElementById('bar-checked');

        if (tot) {
            var pct = tot.domains_total > 0
                ? (tot.domains_checked / tot.domains_total) * 100 : 0;
            setVal('v-total', fmtInt(tot.domains_total));
            setVal('v-checked', fmtInt(tot.domains_checked));
            setVal('v-checked-sub', fmtPct(pct) + ' от ' + fmtInt(tot.domains_total));
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
            'скорости считаются из секций зон и темпа — одна недоступна';
        if (rates) {
            setVal('v-dps', rates.domains_per_sec != null
                ? fmtRate(rates.domains_per_sec) : ND,
            rates.domains_per_sec != null
                ? undefined
                : 'окно суток короче 10 минут — скорость ещё не определена');
            setVal('v-rps', fmtRate(rates.results_per_sec));
            setVal('v-renders', fmtRate(rates.renders_per_sec));
            setVal('v-eta', rates.eta_sec != null ? fmtDur(rates.eta_sec) : ND,
                rates.eta_sec != null ? undefined : 'скорость неизвестна — ETA не считается');
            setVal('v-eta-sub', 'остаток первого цикла: ' + fmtInt(rates.remaining));
        } else {
            setVal('v-dps', ND, ratesErr);
            setVal('v-rps', ND, ratesErr);
            setVal('v-renders', ND, ratesErr);
            setVal('v-eta', ND, ratesErr);
            setVal('v-eta-sub', 'остаток первого цикла: ' + ND);
        }

        pushSeries(hist.dps, rates ? rates.domains_per_sec : null);
        pushSeries(hist.rps, rates ? rates.results_per_sec : null);
        pushSeries(hist.renders, rates ? rates.renders_per_sec : null);
        drawSpark(document.getElementById('spark-dps'), hist.dps);
        drawSpark(document.getElementById('spark-rps'), hist.rps);
        drawSpark(document.getElementById('spark-renders'), hist.renders);
    }

    // ------------------------------------------------------------ Laravel-карточка (§5.6)

    function renderLaravelCard(snap) {
        var l = snap.laravel;
        if (l) {
            setVal('v-laravel', fmtInt(l.total));
            setVal('v-laravel-sub', 'за 24 ч: ' + fmtInt(l.last_24h));
        } else {
            setVal('v-laravel', ND, errTitle(snap, 'laravel'));
            setVal('v-laravel-sub', 'за 24 ч: ' + ND);
        }
    }

    // ------------------------------------------------------------ очереди (§5.4)

    function renderQueues(snap) {
        var body = document.getElementById('queues-body');
        var hint = document.getElementById('queue-hint');
        if (!body) return;
        body.textContent = '';

        var queues = snap.queues;
        if (!queues) {
            ndRow(body, 3, errTitle(snap, 'queues'));
            if (hint) hint.textContent = '';
            return;
        }

        var streams = STREAMS.slice();
        Object.keys(queues).forEach(function (s) {
            if (streams.indexOf(s) < 0) streams.push(s); // новые стримы не теряем
        });

        // Сумма рабочих стримов: q:dead гейтом backpressure не управляется
        // (мертвые задачи — разбор оператора, не отставание конвейера).
        var workSum = 0;
        streams.forEach(function (s) {
            if (s !== STREAM_DEAD) workSum += queues[s] || 0;
        });

        var watermark = snap.queue_watermark || 0;
        if (hint) {
            hint.textContent = watermark > 0
                ? 'порог backpressure: ' + fmtInt(watermark) +
                    ' · сумма рабочих стримов: ' + fmtInt(workSum)
                : '';
        }

        streams.forEach(function (s) {
            var depth = queues[s] || 0;
            var series = hist.queues[s] || (hist.queues[s] = []);
            pushSeries(series, depth);

            var row = el('tr');
            if (watermark > 0 && s !== STREAM_DEAD) {
                if (workSum >= watermark) {
                    row.className = 'q-over'; // у порога: backpressure вот-вот
                } else if (workSum >= WATERMARK_WARN * watermark) {
                    row.className = 'q-warn'; // приближение (≥90 %)
                }
            }

            var dead = s === STREAM_DEAD && depth > 0;
            var name = el('td', dead ? 'q-dead-nonzero' : null, s);
            if (dead) name.title = 'мертвые задачи: требуется разбор оператора';
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

    // ------------------------------------------------------------ зоны (§5.3)

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
            body.lastChild.firstChild.textContent = 'зон нет';
            return;
        }

        snap.zones.forEach(function (z) {
            var row = el('tr');
            var name = el('td', 'mono', z.name);
            if (z.name_unicode && z.name_unicode !== z.name) {
                name.title = z.name_unicode; // unicode-форма — только отображение (§5.3)
            }
            row.appendChild(name);

            var state = el('td');
            state.appendChild(el('span',
                z.active ? 'badge badge-on' : 'badge badge-off',
                z.active ? 'активна' : 'пауза'));
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

    // ------------------------------------------------------------ статусы (§5.5)

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
                seg.style.background = color; // CSSOM: CSP-безопасно
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
        summary.appendChild(document.createTextNode('всего · ' + fmtInt(total)));
        legend.appendChild(summary);
    }

    // ------------------------------------------------------------ находки (§5.6)

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
            body.lastChild.firstChild.textContent = 'находок ещё нет';
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
            body.lastChild.firstChild.textContent = 'файлов ещё нет';
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

    // ------------------------------------------------------------ здоровье (§5.8)

    function renderHealth(snap) {
        var box = document.getElementById('health');
        if (!box) return;
        box.textContent = '';

        if (!snap.health) {
            // Не ошибка: пробы не заданы (dashboard.probes пуст, §5.9).
            var nd = el('span', 'nd', ND);
            nd.title = 'пробы не заданы (dashboard.probes пуст)';
            box.appendChild(nd);
            return;
        }

        snap.health.forEach(function (p) {
            var chip = el('span', 'chip' + (p.up ? '' : ' down'));
            chip.appendChild(el('span', 'dot'));
            chip.appendChild(document.createTextNode(p.name));
            if (!p.up && p.error) chip.title = p.error; // причина down
            box.appendChild(chip);
        });
    }

    // ------------------------------------------------------------ цикл опроса

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
        renderZones(snap);
        renderStatuses(snap);
        renderLaravel(snap);
        renderIngest(snap);
        renderHealth(snap);
        setVal('built-at', 'снимок: ' + fmtTime(snap.built_at));
    }

    var timer = null;

    function poll() {
        fetch('/api/status', {headers: {Accept: 'application/json'}})
            .then(function (resp) {
                if (resp.status === 401) {
                    // §6.4: сессия/креды больше не проходят. fetch не
                    // вызывает браузерный Basic-диалог — перезагрузка
                    // страницы переносит 401 на сам документ: браузер
                    // либо молча переотправит кэшированные креды, либо
                    // покажет диалог повторного входа.
                    if (timer) clearInterval(timer);
                    setConn('требуется повторный вход…', 'conn-err');
                    location.reload();
                    return null;
                }
                if (resp.status === 503) {
                    // §5.9: первый снимок ещё не собран.
                    setConn('снимок готовится…', 'conn-wait');
                    return null;
                }
                if (!resp.ok) {
                    setConn('ошибка обновления: HTTP ' + resp.status, 'conn-err');
                    return null;
                }
                return resp.json().catch(function () { return null; });
            })
            .then(function (snap) {
                if (!snap) return;
                try {
                    render(snap);
                    setConn('обновлено ' + new Date().toLocaleTimeString('ru-RU'), 'conn-ok');
                } catch (e) {
                    setConn('ошибка отрисовки снимка', 'conn-err');
                }
            })
            .catch(function () {
                setConn('нет соединения с дашбордом', 'conn-err');
            });
    }

    poll();
    timer = setInterval(poll, refreshSecs * 1000);
})();

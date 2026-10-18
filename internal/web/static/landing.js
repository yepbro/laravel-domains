/*
 * landing.js — перенос из larasites (T-122) с адаптацией:
 *  - эндпоинт формы — data-атрибут (без Laravel-роутов);
 *  - CSRF-заголовок удалён (JSON API + рейт-лимит, без сессий);
 *  - ответ рендерится по контракту T-123 (found/score/версии/
 *    per-subdomain).
 *
 * Состав: count-up карточек статистики (IntersectionObserver), облако
 * зон с коллизиями (клеточная раскладка с анимацией появления),
 * сортировка таблицы зон, AJAX-форма проверки домена.
 */
(function () {
    'use strict';

    // ------------------------------------------------------------ count-up

    function animateCount(el) {
        var target = parseInt(el.getAttribute('data-count') || '0', 10);
        if (!target) {
            el.textContent = '0';
            return;
        }
        var duration = 1200;
        var start = null;
        function step(ts) {
            if (start === null) start = ts;
            var progress = Math.min((ts - start) / duration, 1);
            var eased = 1 - Math.pow(1 - progress, 3);
            el.textContent = Math.floor(eased * target).toLocaleString('ru-RU');
            if (progress < 1) requestAnimationFrame(step);
        }
        requestAnimationFrame(step);
    }

    var counters = document.querySelectorAll('[data-count]');
    if ('IntersectionObserver' in window && counters.length) {
        var seen = new WeakSet();
        var io = new IntersectionObserver(function (entries) {
            entries.forEach(function (e) {
                if (e.isIntersecting && !seen.has(e.target)) {
                    seen.add(e.target);
                    animateCount(e.target);
                    io.unobserve(e.target);
                }
            });
        }, {threshold: 0.4});
        counters.forEach(function (el) { io.observe(el); });
    } else {
        counters.forEach(animateCount);
    }

    // ------------------------------------------------------------ zone cloud

    var cloud = document.getElementById('zone-cloud');
    if (cloud) {
        var items = Array.prototype.slice.call(cloud.querySelectorAll('.zone-item'));
        layoutCloud(cloud, items);
        window.addEventListener('resize', function () {
            clearTimeout(cloud._relayoutTimer);
            cloud._relayoutTimer = setTimeout(function () {
                layoutCloud(cloud, items);
            }, 150);
        });
    }

    // Раскладка облака: клетки по сетке с проверкой перекрытия
    // (rects-коллизии), случайный джиттер против рядов.
    function layoutCloud(container, items) {
        if (!items.length) return;
        var w = container.clientWidth || 600;
        var h = Math.max(300, Math.min(480, items.length * 26));
        container.style.height = h + 'px';
        var rects = [];
        items.forEach(function (el, i) {
            el.style.visibility = 'hidden';
            el.classList.remove('animating');
            var box = el.getBoundingClientRect();
            var width = box.width || 120;
            var height = box.height || 40;
            var placed = false;
            for (var attempt = 0; attempt < 200 && !placed; attempt++) {
                var x = Math.random() * Math.max(1, w - width);
                var y = Math.random() * Math.max(1, h - height);
                var rect = {x: x, y: y, w: width + 14, h: height + 10};
                var clash = rects.some(function (r) {
                    return rect.x < r.x + r.w && rect.x + rect.w > r.x &&
                        rect.y < r.y + r.h && rect.y + rect.h > r.y;
                });
                if (!clash) {
                    rects.push(rect);
                    el.style.left = Math.round(x) + 'px';
                    el.style.top = Math.round(y) + 'px';
                    placed = true;
                }
            }
            if (!placed) {
                // Не влезло честно — наслаиваем в центр (режутся крайние
                // редкие зоны; плотность важнее идеальной читаемости).
                el.style.left = Math.round(w / 2 - width / 2 + (Math.random() - 0.5) * w * 0.6) + 'px';
                el.style.top = Math.round(h / 2 - height / 2 + (Math.random() - 0.5) * h * 0.6) + 'px';
            }
            el.style.visibility = '';
            setTimeout(function (idx) {
                items[idx].classList.add('animating');
            }, 40 + i * 45, i);
        });
    }

    // ------------------------------------------------------------ zone table

    var table = document.getElementById('zone-table');
    if (table) {
        var headers = table.querySelectorAll('th[data-sort]');
        headers.forEach(function (th) {
            th.addEventListener('click', function () {
                var key = th.getAttribute('data-sort');
                var type = th.getAttribute('data-sort-type');
                var tbody = table.querySelector('tbody');
                var rows = Array.prototype.slice.call(tbody.querySelectorAll('tr'));
                var desc = th._desc = !th._desc;
                headers.forEach(function (other) {
                    other.querySelector('.sort-indicator').style.opacity = '0';
                });
                th.querySelector('.sort-indicator').style.opacity = '1';
                th.querySelector('.sort-indicator').textContent = desc ? '▼' : '▲';
                rows.sort(function (a, b) {
                    var av = a.querySelector('td[data-sort-value="' + key + '"], td:nth-child(1)').getAttribute('data-sort-value');
                    var bv = b.querySelector('td[data-sort-value="' + key + '"], td:nth-child(1)').getAttribute('data-sort-value');
                    var cmp;
                    if (type === 'number') {
                        cmp = (parseFloat(av) || 0) - (parseFloat(bv) || 0);
                    } else {
                        cmp = String(av).localeCompare(String(bv));
                    }
                    return desc ? -cmp : cmp;
                });
                rows.forEach(function (r) { tbody.appendChild(r); });
            });
        });
    }

    // ------------------------------------------------------------ check form

    var form = document.getElementById('domain-check-form');
    if (!form) return;

    var messagesEl = document.getElementById('domain-check-messages');
    var messages = messagesEl ? JSON.parse(messagesEl.textContent) : {};
    var input = document.getElementById('domain-input');
    var button = document.getElementById('check-button');
    var result = document.getElementById('domain-result');
    var resultText = document.getElementById('domain-result-text');
    var details = document.getElementById('domain-details');
    var detailsContent = document.getElementById('domain-details-content');

    form.addEventListener('submit', function (e) {
        e.preventDefault();
        var domain = (input.value || '').trim();
        if (!domain) return;

        button.disabled = true;
        setResult(' ', 'info');

        fetch(form.getAttribute('data-endpoint') || '/api/check-domain', {
            method: 'POST',
            headers: {'Content-Type': 'application/json', 'Accept': 'application/json'},
            body: JSON.stringify({domain: domain})
        }).then(function (resp) {
            return resp.json().catch(function () { return {}; }).then(function (payload) {
                return {status: resp.status, payload: payload};
            });
        }).then(function (r) {
            if (r.status === 429) {
                setResult(messages.rate_limited || 'rate limited', 'error');
                return;
            }
            if (r.status === 422 || r.status === 400) {
                setResult(messages.invalid || 'invalid domain', 'error');
                return;
            }
            if (r.status === 501) {
                setResult(messages.error || 'not implemented yet', 'error');
                return;
            }
            if (r.status >= 500) {
                setResult(messages.server_error || 'server error', 'error');
                return;
            }
            renderCheckResult(r.payload);
        }).catch(function () {
            setResult(messages.error || 'check failed', 'error');
        }).finally(function () {
            button.disabled = false;
        });

        function renderCheckResult(p) {
            var found = !!p.found;
            setResult(found
                ? (messages.found || 'Laravel: found')
                : (messages.not_found || 'Laravel: not found'), found ? 'ok' : 'muted');
            if (p.score !== undefined && p.score !== null) {
                appendRow('score', Number(p.score).toFixed(3));
            }
            if (p.ver_min || p.ver_max) {
                appendRow('Laravel', (p.ver_min || '?') + '–' + (p.ver_max || '?'));
            }
            (p.subdomains || []).forEach(function (s) {
                var line = (s.subdomain || '') + (s.variant ? '/' + s.variant : '') +
                    ' — ' + (s.status || '') + (s.final_url ? ' → ' + s.final_url : '');
                appendRow('', line);
                (s.signals || []).forEach(function (sig) {
                    appendRow('  ·', sig.rule + ' (' + sig.weight + ') ' + (sig.evidence || ''));
                });
            });
        }

        function appendRow(label, value) {
            if (!details.classList.contains('hidden') === false) details.classList.remove('hidden');
            var row = document.createElement('div');
            row.className = 'flex gap-2';
            var l = document.createElement('span');
            l.className = 'text-zinc-500 min-w-[7rem]';
            l.textContent = label;
            var v = document.createElement('span');
            v.className = 'text-zinc-200 break-all';
            v.textContent = value;
            row.appendChild(l);
            row.appendChild(v);
            detailsContent.appendChild(row);
        }

        function setResult(text, kind) {
            result.classList.remove('hidden', 'bg-orange-500/10', 'bg-red-500/10', 'bg-zinc-800');
            resultText.textContent = text;
            if (kind === 'ok') {
                result.classList.add('bg-orange-500/10');
                resultText.className = 'text-orange-400 font-semibold';
            } else if (kind === 'error') {
                result.classList.add('bg-red-500/10');
                resultText.className = 'text-red-400';
            } else {
                result.classList.add('bg-zinc-800');
                resultText.className = 'text-zinc-300';
            }
            details.classList.add('hidden');
            detailsContent.textContent = '';
        }
    });
})();

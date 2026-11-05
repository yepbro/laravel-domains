# Прод-топология larasites и команды углубления

Сервер: `ssh larasites` → root@155.212.174.88 (pulpirzvk.fvds.ru, IXcellerate).
4 vCPU / 8 ГБ RAM / NVMe 79 ГБ, Ubuntu 24.04.5, ядро 6.8.
Load 12–17 во время активного рендера — известный рабочий режим (проект
сознательно перегружает CPU рендерами; docs/load-measurements.md).

## Деплой (нативный systemd, НЕ docker)

- Бинарники: `/opt/crawler/bin/` (ingest, scheduler, dns-worker, http-worker,
  render-worker, analyzer, monitor, web, dashboard, **crawlerctl**).
- Рабочий каталог сервисов: `/var/lib/crawler` (контент — `content/`,
  входящие — `incoming/`).
- Окружение: `/etc/crawler/crawler.env`; конфиг: `/etc/crawler/config.yaml`.
- Юниты: `crawler-{ingest,scheduler,dns-worker,http-worker,render-worker,
  render-worker@9109,analyzer,monitor,web,dashboard,chromium}`, плюс
  `redis-server`, `postgresql@16-main`, `unbound`, `nginx`, `fail2ban`.

### Подводный камень crawler.env

Строка `CRAWLER_HTTP_USER_AGENT=... (+https://...; ...)` содержит скобки —
`source` в bash падает на ней с syntax error, переменные после неё не
загружаются. Для `crawlerctl status` хватает ранних строк (DSN на строках
2–3), поэтому рабочий вызов:

```bash
set -a; . /etc/crawler/crawler.env 2>/dev/null; set +a
/opt/crawler/bin/crawlerctl status          # зоны… итого, очереди, скорость, цикл
```

## Конвейер и очереди (Redis Streams)

```
scheduler ──> q:dns ──> dns-worker ──> q:http ──> http-worker ─┬─> q:analyze ──> analyzer ──> PostgreSQL
                                          │                    └─> q:render ──> render-worker ──> q:analyze
                          (исчерпаны retry ──> q:dead — без потребителя, ручной разбор)
```

Группы: `dns-worker`, `http-worker`, `render-worker`, `analyzer` — по одной
на стрим. Поле сообщений — `subdomain_results` (DNSBatch/HTTPTask/Result).

- Глубина: `min(lag, XLEN) + pending` — так считает и `crawler_queue_depth`.
- Два среза `XINFO GROUPS` (Δentries-read) → скорость. См. формулы в SKILL.md.
- XINFO GROUPS --raw, одна группа, позиции строк (Redis 7.4):
  2=name, 4=consumers, 6=pending, 8=last-delivered-id, 10=entries-read, 12=lag.
- Backpressure: общий `scheduler.queue_high_watermark` (100 тыс.), а у
  q:render — отдельный `render.queue_high_watermark` (T-174, на проде
  10 тыс., `/etc/crawler/config.yaml` секция `render:`). Выше порога
  SPA-записи не публикуются, у домена ставится флаг `render_pending`
  (бит 1 `domains.flags`).
- Пополнение q:render из долга (T-066): пока глубина стрима ниже
  `render.queue_high_watermark`, проход доработки `RequeueRenderPending`
  в http-worker (тик ~1 мин, пачка до 100 доменов) публикует SPA-задачи
  доменов с флагом и снимает бит — переходы бита ведут счётчик
  `crawler_counters['render_pending']` (T-175). Следствие: лаг q:render
  держится у порога, пока долг не исчерпан. Между снимками сравнивать
  только сумму **lag(q:render) + render_pending** (готовая метрика
  monitor: `crawler_render_pending_domains`); дренаж жив — если в
  журнале http-worker идёт «проход доработки рендера» (full=true —
  пачка упёрлась в порог, это норма).

## Порты метрик (Prometheus, 127.0.0.1, с сервера)

| Порт | Сервис |
|---|---|
| 9100 | monitor (сводные + алерты: frozen-очереди, redis, PG slow, OOM) |
| 9104 | http-worker (retention, компактификация, HTTP-этап) |
| 9105 / 9109 | render-worker (осн. + второй экземпляр @9109) |
| 9106 | analyzer |

```bash
curl -s 127.0.0.1:9104/metrics | grep -E '^crawler_queue_depth|^crawler_content_'
curl -s 127.0.0.1:9100/metrics | grep -E '^crawler_(queue|unit|redis)'
```

## PostgreSQL (sudo -u postgres psql -d crawler)

- Размеры: `SELECT relname, pg_size_pretty(pg_total_relation_size(relid))
  FROM pg_statio_user_tables ORDER BY 2 DESC LIMIT 8;`
  Ориентир на 03.11.2026: domains_default ~10 Г, subdomain_results_default
  ~7.5 Г, contents ~0.26 Г (сам контент — в паках на диске, не в БД).
- Живость таблиц: `n_live_tup`/`n_dead_tup` у `contents`,
  `subdomain_results_default`, `laravel_domains`.
- Счётчики `crawler_counters` (3 колонки: имя, значение, время):
  `checks:ГГГГ-ММ-ДД` — проверок доменов за сутки (0 при стоящем фронте);
  `laravel_appeared_total` / `laravel_lost_total` — находки;
  `render_pending` — рендер-долг (защита retention, T-166/T-185);
  полный рендер-долг = лаг q:render + это значение — сравнивать q:render
  между снимками только вместе с ним (метрика
  `crawler_render_pending_domains`);
  `reanalyze:*` — шум, фильтровать.

## Контент-хранилище и диск

- Паки: `/var/lib/crawler/content/pack-NNNNNN.pack` (~489 МБ каждый).
  Рост/освобождение: retention (6 ч, чистит ссылки у старых не-находок) +
  часовая компактификация — ОБА цикла в http-worker. Проверка:
  ```bash
  journalctl -u crawler-http-worker --since -24h | grep -E "компактификации|retention"
  ```
  Норма: проход раз в час, `freed_bytes` > 0. Retention skips при глубине
  q:analyze ≥ watermark — периодически норм, постоянно — блокирует очистку.
- Логи: journal ≤ 1 Г (SystemMaxUse), ForwardToSyslog=no; спам chromium
  погашен drop-in'ом `crawler-chromium.service.d/10-logfilter.conf`
  (репо: `deploy/systemd/crawler-chromium.service.d/`).
- `/var/log/postgresql` ~0.3 Г — норм; растёт — лог-уровень PG.

## Дашборд и входы

- Операторский: `https://dashboard.laracity.ru` (HTTP Basic, JSON-снимок
  `GET /api/status` — те же очереди/скорости, что в crawlerctl).
- Лендинги: `https://sites.laravel.city`, `https://sites.laracity.ru`.

## Известные инциденты (что искать по признакам)

| Дата | Признак | Причина |
|---|---|---|
| 03.11.2026 | «остаток очереди» в 15 раз выше правды | XLEN приняли за глубину; правда — lag |
| 03.11.2026 | скорость 0 дом/с сутками, q:dns/q:http пустые | crawler-scheduler остановлен вручную оператором |
| 23.10.2026 | лаг q:render ≈99 тыс., retention стоит | рендер-долг: защита записей от удаления (T-166/T-167) |
| 03.11.2026 | syslog +2.5 ГБ за 2 дня | консоли чужих страниц из chromium (см. drop-in выше) |
| 04.11.2026 | лаг q:render ~9,9 тыс. сутами «не тает», прогноз дренажа за часы не сбылся | равновесие у `render.queue_high_watermark` (10 тыс.): проход доработки T-066/T-174 пополняет очередь из render_pending (~350 тыс.) — это норма; метрика — lag + render_pending, ETA по сумме |

## Точечные команды по блокам

```bash
# юнит и его рестарты/окружение
systemctl status crawler-render-worker --no-pager -l
systemctl show crawler-scheduler -p ActiveState,NRestarts,MainPID,ActiveEnterTimestamp

# журнал сервиса (ошибки за час)
journalctl -u crawler-http-worker --since -1h -q | grep -iE "err|warn|fatal|panic" | tail -20

# зависшие сообщения очереди (pending у группы)
redis-cli XPENDING q:render render-worker

# дренаж рендер-долга: пополнение q:render из render_pending (T-066)
journalctl -u crawler-http-worker --since -30m -q | grep "проход доработки рендера" | tail -5

# состояние домена
set -a; . /etc/crawler/crawler.env 2>/dev/null; set +a
/opt/crawler/bin/crawlerctl domain example.com --json

# медленные запросы PG
sudo -u postgres psql -d crawler -c "SELECT calls, round(mean_exec_time::numeric,0) ms, left(query,80) FROM pg_stat_statements ORDER BY mean_exec_time DESC LIMIT 10;"
```

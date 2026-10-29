# Боевое развёртывание: systemd

Инструкция по запуску Domain Crawler на целевом сервере (Ubuntu Server LTS,
8 ГБ RAM, один IP — ТЗ 3.1) системными systemd-юнитами. Dev-окружение
Docker — отдельный способ запуска ([`docker.md`](docker.md),
[`dev-environment.md`](dev-environment.md)); соотношение способов — ниже.

Комплект юнитов: [`deploy/systemd/`](../deploy/systemd/) (этап 9, T-085).
ТЗ: §10.1 (окружение), §3.2 (бюджет памяти), §4.1 (состав сервисов),
§11.4 п. 7 (суточный прогон без OOM).

## 1. Способ развёртывания и отношение к Docker

ТЗ §10.1 задаёт боевой способ прямо: **systemd-юниты на каждый сервис**,
PostgreSQL/Redis/Unbound — пакеты ОС, Go-сервисы — статические бинарники,
Chromium — отдельный пакет/каталог. Docker Compose в репозитории —
dev-окружение (hot reload, testbed, инструменты) и не является боевым
деплоем. Решение зафиксировано:

| Слой | Боевой (systemd) | Dev (docker compose) |
|---|---|---|
| PostgreSQL, Redis, Unbound | пакеты ОС + drop-in'ы бюджета (`deploy/systemd/*.service.d`) | контейнеры compose |
| Go-сервисы (9 шт.: 7 конвейерных + web + dashboard) | статические бинарники `/opt/crawler/bin`, юниты `crawler-*.service` | образ `crawler/go-dev:local` + air |
| Chromium | дистрибутив headless-shell в `/opt/crawler/chromium`, юнит `crawler-chromium.service` | образ `chromedp/headless-shell` (dev-профиль app) |
| `crawlerctl` | CLI в `/opt/crawler/bin`, без юнита | контейнер tools |

Docker-профиль `runtime` (боевой render-контейнер, T-065) остаётся
docker-вариантом рендера (воркер + собственный Chromium в одном контейнере,
`mem_limit` 2048m) — для хостов с Docker; его связь с systemd-набором:
`crawler-chromium.service` + `crawler-render-worker.service` дают ту же пару
в нативном виде, но в двух cgroup (см. ниже).

## 2. Бюджет памяти: MemoryMax по юнитам

Бюджет ТЗ 3.2 исполняется cgroup'ами `MemoryMax` (ТЗ 10.1: OOM-killer не
затрагивает PostgreSQL — сработавший потолок убивает процесс внутри своей
cgroup, systemd перезапускает юнит, остальные сервисы не затронуты).

| Юнит | MemoryMax | Строка бюджета ТЗ 3.2 |
|---|---|---|
| `postgresql@.service` + drop-in | **3072M** (бюджет ≈ 2,5 ГБ + запас на пики обслуживания) | PostgreSQL ≈ 2,5 ГБ |
| `redis-server.service` + drop-in | **640M** (maxmemory 512mb + ~128 МБ служебных, docs/redis.md §7) | Redis ≤ 0,5 ГБ |
| `unbound.service` + drop-in | **800M** (бюджет ≈ 0,7 ГБ + рост кэша на ≈47 млн имён: 733.9 MiB у старого потолка 700M, §12; +100M из MemAvailable 2.6 GiB) | Unbound ≈ 0,7 ГБ |
| `crawler-*.service` (9 юнитов) | **2436M суммарно** (второй экземпляр render-worker@ добавляет свой 256M — §2.2) | Go-сервисы ≈ 1,3 ГБ (7 конвейерных) + web этапа 13 + dashboard этапа 15 |
| `crawler-chromium.service` | **2048M** | Chromium ≈ 2 ГБ |

Итого потолки 8996M при 8192M RAM — суммарно выше объёма памяти, и это
допустимо: потолок — не резервация, одновременного касания всех потолков
не бывает (по средним замеров §6/§11 весь набор ≈ 5.1–5.2 GiB, с пиком
Chromium — ≈ 6 GiB; подъём T-160 — unbound +100M, dns-worker +128M —
взят из фактического запаса MemAvailable 2.6 GiB замера §12;
PostgreSQL у потолка 3 GiB держит page cache, отдаваемый при давлении;
разделение OOM по cgroup — вводный абзац этого
раздела, ТЗ 10.1, — сохраняется). Строка «Go-сервисы ≈ 1,3 ГБ» ТЗ
покрывала семь конвейерных юнитов; публичный `web` лендинга — продуктовое
расширение этапа 13 вне основного ТЗ — добавляет свою строку 256M,
операторский `dashboard` — расширение этапа 15 (ТЗД) — 128M. MemoryMax выше плановой строки бюджета
там, где над внутренним лимитом живут служебные памяти процесса или пики
обслуживания — Redis (maxmemory 512mb + ~128 МБ, eviction раньше OOM —
docs/redis.md §7), PostgreSQL (work_mem/maintenance/autovacuum поверх
shared_buffers — docs/db.md, «Память»), http-worker (тела в полёте
на 192 слота — T-143/T-157, ниже). Штатно инстансы живут в строках бюджета:
плановые строки суммарно ≈ 7,4 ГБ, строка «Go-сервисы» по факту замера
§11 ≈ 1.0–1.1 GiB; остаток ≈ 0,6 ГБ и незаполненность потолков — ОС
и page cache (ТЗ 3.2).

Потолки девяти юнитов Go-сервисов (строка «Go-сервисы ≈ 1,3 ГБ»
исполняется фактическим потреблением ≈ 1.0–1.15 GiB, а не суммой
потолков 2436M):

| Юнит | MemoryMax |
|---|---|
| `crawler-http-worker.service` | 900M |
| `crawler-dns-worker.service` | 384M |
| `crawler-web.service` | 256M |
| `crawler-render-worker.service` | 256M |
| `crawler-ingest.service` | 128M |
| `crawler-scheduler.service` | 128M |
| `crawler-analyzer.service` | 128M |
| `crawler-monitor.service` | 128M |
| `crawler-dashboard.service` | 128M |

- **http-worker — 900M**, самый тяжёлый: до `http.task_concurrency`
  доменных задач на экземпляр (192 по умолчанию — T-157, возврат 256 —
  по замеру после T-163; T-139 — скользящее потребление q:http);
  тела страниц в слоте не удерживаются (ранний Put T-147 — каждая
  страница сразу уходит в хранилище по хэшу), в задаче — метаданные и
  ≤ `http.task_per_domain` транзитных буферов читаемых тел, плюс кэш
  итоговых URL и пишущее хранилище контента; мягкий потолок кучи Go —
  `http.mem_limit` (T-163, дефолт 768 MiB: GC у порога сам возвращает
  страницы ОС, не доводя anon до MemoryHigh/MemoryMax — reclaim-штормы
  и фаталы T-157), замер критерия — метрики слота на :9104. Потолок
  поднят 384M → 900M по повторному замеру после
  T-139 (load-measurements.md §11, T-143): 256 слотов держали anon
  avg ≈ 855 MiB (834–873, oom=0 под потолком 900 MiB, заданным на
  сервере `systemctl set-property` — персистентный drop-in, теперь
  закреплён в юните), т.е. ≈ 3.34 MiB на слот — замер кода до T-147
  (тогда тела записей-оригиналов, каждое ≤ 2 МиБ, держались до конца
  задачи; типичная смесь, не верхняя граница: всплеск крупных страниц
  может выйти выше — окно замера 12 мин).
  Суточный прогон это подтвердил (§12, T-157): два фатала за сутки на
  256 — 05:48 killed by OOM killer и 09:17 выход на пик 900.5 MiB =
  MemoryMax, MemoryHigh 850M сработал 120 095 раз (reclaim-штормы,
  swap) — дефолт понижен до 192, устойчиво по пропитке волны 21
  (860–893 MiB, oom=0, NRestarts=0; скорость ≈17–19 дом/с).
  384M подходили впритык уже на 128 слотах (anon ≈ 380 из 384 MiB).
  Строка «Go-сервисы ≈ 1,3 ГБ» проходит: http 900M + прочие Go-сервисы
  по замеру §6/§11 ≈ 130–160 MiB → суммарно ≈ 1.0–1.1 GiB (с
  render-worker по avg §6 — ≈ 1.13 GiB); даже по максимумам §6 —
  ≈ 1.2 GiB. Возврат к 256 — по замеру после T-163 (метрики слота
  `crawler_http_*` на :9104: anon юнита / занятые слоты ≤ 1.5 MiB);
- **dns-worker — 384M**: пул 2000 UDP-сокетов (`dns.udp_pool_size`) и
  буферы обменов, TTL-кэш редиректных резолвов (T-047). Потолок поднят
  256M → 384M по замеру §12 (T-160): на ≈47 млн доменов и глубоком q:dns
  воркер периодически упирался в 256 MiB (в §6 на смеси ru/rf/su max был
  141 MiB), запас — из MemAvailable 2.6 GiB;
- **web — 256M** (этап 13, вне строки ТЗ): кроме лёгкой работы с БД живые
  проверки формы гонят полный конвейер checker (T-123: DNS → HTTP с телами
  страниц до `http.max_body_bytes` → рендер SPA → анализ) — сетевые буферы
  одного доменного эпизода, паритет с dev-профилем app;
- **render-worker — 256M** (T-169, было 128M): только Go-часть (Chromium —
  в отдельной cgroup), но DOM в полёте ≤ `render.contexts` ×
  `content.max_store_bytes` (8 × 2 МиБ + transient-копии сериализации) и
  буферы pgx/очереди: замер T-092 §6 упирался в потолок 128 MiB
  (23/116/128) уже при 4 контекстах, к 23.10 — 117–128 МБ + swap
  (дефицит слот-времени, подъём в стиле T-160); дефолт `render.contexts`
  8 (T-169) умножает окно DOM вдвое — потолок 256M, контроль после
  деплоя: `memory.events oom=0`, NRestarts 0;
- **ingest / scheduler / analyzer / monitor / dashboard — по 128M**:
  пакетная работа с БД без больших буферов (потоковый разбор
  файлов, SQL-пачки, разбор уже сжатого контента постранично, счётчики);
  dashboard — read-only читатель БД/Redis (агрегаты считает SQL-движок,
  в памяти — только снимок и HTML/JSON-артефакты embed);
- `crawlerctl` — CLI без юнита (ТЗ 4.1), запускается оператором эпизодически
  (`crawlerctl status/domain/zones/...`), его память не фиксируется
  потолком — команда короткоживущая; эпизодические десятки MiB ложатся
  в запас строки «Go-сервисы» по средним замера (факт конвейера
  ≈ 1.0–1.15 GiB против ≈ 1,3 ГБ).

Chromium отдельным юнитом, а не процессом воркера (решение): у строки
бюджета своя cgroup — OOM вкладки браузера убивается внутри
`crawler-chromium.service` и перезапускается, не затрагивая воркер;
остановка/рестарт браузера не останавливает потребление q:render (воркер
переживает — пауза потребления при мёртвом браузере, docs/render.md);
профиль и `--no-sandbox` — от выделенного пользователя `crawler-chromium`.
Воркер работает в remote-режиме (`render.remote_url = ws://127.0.0.1:9222`)
— тот же контракт, что у dev-профиля app. Компенсация remote-режима:
перезапуск по числу страниц/RSS изнутри воркера (T-065) невозможен для
чужого процесса — потолок и перезапуск держит юнит (аналогия mem_limit
dev-сервиса chromium, docs/docker.md §6); плановый ресайкл раз в 2 ч —
таймер `crawler-chromium-recycle.timer` (§2.2 ниже, T-208).

Применение T-160 на живом сервере (unbound 700 → 800M, dns-worker
256 → 384M) — **до деплоя**, по образцу §2.1 (T-158): значения те же,
что в drop-in'е/юните, действуют сразу, персистентный drop-in пишется в
`/etc/systemd/system.control/`:

```bash
systemctl set-property unbound.service MemoryMax=800M
systemctl set-property crawler-dns-worker.service MemoryMax=384M
```

После деплоя — синхронизировать drop-in/юнит по §5 (п. 1) и снять
персистентные set-property drop-in'ы (`systemctl revert <юнит>` +
`daemon-reload` + `restart` — как в §2.1, образец T-143,
load-measurements.md §3): единственный источник — файлы комплекта. Критерий T-160: `memory.events oom=0` у
обоих юнитов, MemoryCurrent unbound < MemoryMax с запасом ≥ 50 MiB,
median длительности DNS-задач в гистограммах 9103 не деградирует,
`recursion.time.median` (`unbound-control stats`) не растёт.

Применение T-169 на живом сервере (render-worker 128 → 256M) — вместе
со ступенью 1 (операционно `render.contexts 4 → 8` в
`/etc/crawler/config.yaml` — явное значение в конфиге перебивает
дефолты кода, §5 п. 2; замер комплектом T-092 до/после, 30–60 мин
стационара):

```bash
systemctl set-property crawler-render-worker.service MemoryMax=256M
```

После деплоя — синхронизировать юнит по §5 (п. 1) и снять drop-in
(`systemctl revert` + `daemon-reload` + `restart`). Критерий ступени 1
(T-169): лаг q:render убывает ≥1,5 тыс./ч, PSI CPU some < 95 %,
chromium RSS ниже порога `render.restart_memory_bytes` (1536 MiB —
MemoryCurrent юнита chromium), гистограммы и память 9105, `q:dead` без
роста, NRestarts 0; повторный замер после CPUWeight рендера (T-158) —
ёмкость против приоритета. Ступени 2–3 — configuration.md (`render`,
`timeout`) и §2.2 ниже.

### 2.1. CPU-приоритеты: CPUWeight/CPUQuota (T-158)

Сервер один (4 vCPU, ТЗ 3.1), расширение недоступно (замер §12), поэтому
CPU перераспределяется между юнитами относительными весами `CPUWeight`
(cgroup v2, диапазон 1–10000, дефолт 100). Вес — не потолок: он действует
только при конкуренции за CPU, применяется на лету и не требует рестарта
юнита. Повод — контрольный замер 2026-10-23
([`load-measurements.md`](load-measurements.md) §12): PSI CPU some
91–95%, load 25/4 vCPU; CPU в стационаре распределялся не в пользу
проверки — postgres 177%, chromium 140%, analyzer 66% против ~90% одного
ядра у http-worker (агрегаты мониторинга, рендер-яд и фоновый анализ
отбирали CPU у конвейера). ТЗ: §3.2, §10.

| Юнит | CPUWeight | CPUQuota | Обоснование |
|---|---|---|---|
| `crawler-http-worker.service` | **1000** | — | критический путь: HTTP-этап — узкое место конвейера (load-measurements §3) |
| `crawler-dns-worker.service` | **1000** | — | критический путь: питает q:http, не должен отставать от HTTP (backpressure) |
| `crawler-render-worker.service` | **300** | — | этап конвейера, не узкое место; тяжёлый CPU рендера — в chromium (см. ниже) |
| `crawler-analyzer.service` | **200** | **150%** | фоновый анализ не отбирает CPU у проверки: квота не режет текущие 66% (§12), ограничивает всплески фоновых проходов |
| `crawler-monitor.service` | **50** | — | обвязка: агрегаты по 47 млн строк (BacklogLoop/ZoneStats, §12) — не конкурент конвейеру |
| `crawler-web.service` | **50** | — | обвязка: лендинг, живые проверки формы — разовые эпизоды |
| `crawler-dashboard.service` | **50** | — | обвязка: read-only читатель БД/Redis (агрегаты — SQL-движок) |
| `crawler-ingest.service`, `crawler-scheduler.service` | 100 (дефолт) | — | CPU-лёгкие постановщики: в замере §12 их CPU незначим |
| `crawler-chromium.service` | 100 (дефолт) | — | приоритет связки рендера задаёт render-worker (300); дефолт 100 при весах 1000 у http/dns уже десятикратно понижает относительный приоритет браузера (140% CPU, §12) |

Инфраструктура (postgresql/redis/unbound) не приоритизирована: их CPU
обслуживает сам конвейер (запись результатов, очереди, резолвы) —
понижение веса душило бы этапы; веса конвейера 1000 при их дефолте 100
решают конкуренцию в пользу проверки без правок чужих юнитов.

Применение на живом сервере — **до деплоя** (значения те же, что в
юнитах; действуют сразу, персистентный drop-in в
`/etc/systemd/system.control/`):

```bash
systemctl set-property crawler-http-worker.service CPUWeight=1000
systemctl set-property crawler-dns-worker.service CPUWeight=1000
systemctl set-property crawler-render-worker.service CPUWeight=300
systemctl set-property crawler-analyzer.service CPUWeight=200 CPUQuota=150%
systemctl set-property crawler-monitor.service CPUWeight=50
systemctl set-property crawler-web.service CPUWeight=50
systemctl set-property crawler-dashboard.service CPUWeight=50
```

После деплоя (юниты несут те же значения): синхронизировать файлы юнитов
по §5 («Обновление версий», п. 1) и снять персистентные drop-in'ы по
образцу T-143 (load-measurements.md §3): `systemctl revert <юнит>` +
`daemon-reload` + `restart` — иначе drop-in продолжает перекрывать
юнит-файл; единственный источник — юнит-файл. Проверка:

```bash
systemctl show crawler-http-worker -p CPUWeight        # 1000
systemctl show crawler-analyzer -p CPUQuotaPerSecUSec  # 1s 500ms (= 150%)
```

Критерий приёмки (замер комплектом T-092 до/после —
load-measurements.md §10): PSI CPU some в стационаре < 80%, NRestarts
юнитов 0, скорость HTTP не падает при фоновых проходах анализатора и
пересчётах зон.

### 2.2. Второй экземпляр render-worker (ступень 3 T-169)

Юнит-шаблон [`deploy/systemd/crawler-render-worker@.service`](../deploy/systemd/crawler-render-worker@.service)
поднимает второй и следующие экземпляры render-worker — ступень 3
разгона дренажа q:render (постановка T-169), **после** ступени 1
(`render.contexts 8` + MemoryMax 256M) и при недостатке ступени 2
(`render.timeout` 25 → 15–20 с только по гистограмме —
configuration.md, `render`): второй воркер оправдан, когда лаг q:render
не убывает ≥1,5 тыс./ч при PSI CPU some < 95 % и гейте у watermark —
значит, слоты/процессор первого воркера и его Chromium исчерпаны.

Механика: тот же бинарник, та же группа потребителей `render-worker`
(ТЗ 4.2 — консьюмерские группы масштабируют воркеров горизонтально);
имя консьюмера per-process (`render-worker-<pid>`) — экземпляры не
конфликтуют ни в XREADGROUP (доставки делятся между консьюмерами), ни
в клеймах XAUTOCLAIM (живой воркер подтверждает раньше
`redis.claim_min_idle`), пишут в общее хранилище контента (координация
через БД — как http-worker с render-worker, ТЗ 5.7).

- **Метрики**: имя экземпляра = порт: `crawler-render-worker@9109` →
  `METRICS_ADDR=127.0.0.1:9109` (9101–9108 заняты комплектом, следующие
  свободны с 9109). `METRICS_ADDR` нельзя задавать в общем
  `/etc/crawler/crawler.env` — EnvironmentFile переопределяет
  `Environment` юнита и столкнёт порты всех экземпляров;
- **память**: свой потолок 256M на экземпляр (cgroup отдельная от
  первого), сумма §2 растёт на 256M; контроль — `memory.events oom=0`,
  NRestarts 0, гистограммы 9105/9109;
- **второй Chromium (:9223)** — только при насыщении первого
  (MemoryCurrent юнита chromium у 2048M / стационарный потолок скорости
  при свободных слотах): per-instance файл
  `/etc/crawler/render-worker@9109.env` с
  `CRAWLER_RENDER_REMOTE_URL=ws://127.0.0.1:9223` (шаблон подключает
  его позже общего crawler.env — переопределяет), юнит chromium — копия
  `crawler-chromium.service` с портом 9223 и отдельным user-data-dir
  (например `/var/lib/crawler/chromium2`); его cgroup ≈2 ГБ — отдельная
  строка бюджета ТЗ 3.2, на сервере 8 ГБ — только по замеру свободной
  памяти (MemAvailable, §12); ресайкл таймером
  `crawler-chromium-recycle.timer` (см. ниже) не покрывается — имя юнита
  зашито в `ExecStart`, второму нужен свой таймер по образцу
  (юнит-шаблон второго Chromium — T-209).

Запуск и контроль:

```bash
systemctl enable --now crawler-render-worker@9109.service
curl -fsS 127.0.0.1:9109/livez              # живость второго экземпляра
journalctl -t crawler-render-worker@9109 -f # логи экземпляра
```

Откат — `systemctl disable --now crawler-render-worker@9109`: доставки
экземпляра вернутся клеймом первому (окно удержания ≪
`redis.claim_min_idle`). Копируется шаблон общим порядком установки
(§4.6, `cp deploy/systemd/crawler*.service` — маска захватывает и
шаблон).

#### Ресайкл Chromium: таймер crawler-chromium-recycle (T-208)

Пара [`crawler-chromium-recycle.service`](../deploy/systemd/crawler-chromium-recycle.service)
и [`crawler-chromium-recycle.timer`](../deploy/systemd/crawler-chromium-recycle.timer)
раз в 2 ч перезапускает `crawler-chromium` — анти-деградация
remote-режима рендера. **Когда нужен**: боевой деплой по умолчанию
работает в remote-режиме (§2, `render.remote_url = ws://127.0.0.1:9222`),
где воркер не может перезапустить чужой браузер по числу страниц/RSS
(T-065) — ресайкл держит systemd, а не воркер. Без него chromium копит
память 400→950+ МиБ за ~2 ч, таймауты рендера удваиваются; замер 29.10
(окно дренажа T-198): после ресайкла таймауты вдвое ниже, 227→110. В
local-режиме (`render.remote_url` пуст — браузер как процесс воркера)
таймер не нужен: воркер сам перезапускает браузер (T-065,
[`render.md`](render.md)).

Цена рестарта — прерванные идущие рендеры (<1 мин каждый): доставки
вернутся клеймом XAUTOCLAIM (идемпотентность ТЗ 3.4), воркер переживает
паузу потребления при мёртвом браузере (docs/render.md). Период — по
замеру деградации (~2 ч, `OnUnitActiveSec`), первый запуск через 30 мин
после активации таймера (`OnActiveSec`) — после старта браузер свежий.

Установка — общим порядком §4.6 (маска `cp` захватывает `crawler*.timer`,
таймер в списке `enable --now`); на уже развёрнутом хосте (пара
установлена 29.10 вручную, вне комплекта) достаточно:

```bash
cp deploy/systemd/crawler-chromium-recycle.{service,timer} /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now crawler-chromium-recycle.timer
systemctl list-timers 'crawler-chromium-recycle*' # контроль расписания
```

Таймер живёт в `timers.target`, в `crawler.target` не входит (директивы —
как на сервере) и остановкой набора не гасится: очередной запуск
ресайкла (`systemctl restart`) поднимёт остановленный chromium. При
остановке/выключении набора гасить таймер отдельно — `systemctl
disable --now crawler-chromium-recycle.timer` (обратное включение —
§4.6).

## 3. Пользователи: статические, по одному на сервис

ТЗ §10.1 требует отдельных пользователей на сервис. Решение — **статические
системные пользователи** `crawler-<сервис>` (без shell, home
`/var/lib/crawler`) в общей группе `crawler`, а не `DynamicUser=yes`:

- хранилище контента пишут **два** юнита (http-worker, render-worker) и
  читают analyzer и `crawlerctl`: нужна стабильная принадлежность файлов
  группе — setgid-каталог + фиксированные UID/GID; DynamicUser ротировал бы
  владельца и требовал бы перекладки прав на каждый старт;
- архив ingest (`incoming/processed|failed`) должен оставаться читаемым
  оператору между запусками;
- права на пак-файлы (0660, группа `crawler`) переживают рестарты без
  systemd-механики StateDirectory.

Каталоги:

```
/var/lib/crawler/            root:crawler 0755   — рабочая зона (WorkingDirectory)
/var/lib/crawler/incoming/   crawler-ingest:crawler 0770 — наблюдаемая папка ingest (ТЗ 5.1)
/var/lib/crawler/content/    root:crawler 02775  — пак-файлы контента (setgid: группа crawler)
/var/lib/crawler/chromium/   crawler-chromium:crawler 0770 — служебный профиль браузера
/etc/crawler/                root:crawler 0750 — config.yaml, crawler.env, rules/
/opt/crawler/bin/            root:root 0755 — бинарники; /opt/crawler/chromium/ — headless-shell
```

Оператор, состоящий в группе `crawler`, кладёт файлы в `incoming/` без sudo.

## 4. Установка

Команды ниже выполняются от root (`sudo -i` или с префиксом `sudo`);
исключение — сборка бинарников (4.2): от обычного пользователя, установка
в `/opt` — с `sudo`.

До пакетов и пользователей выполняется **раскладка дисков**
([`disks.md`](disks.md) §4): тома `/var/lib/postgresql` и
`/var/lib/crawler/content` монтируются до `apt install postgresql` —
кластер пакета создаётся сразу в точке монтирования, а не на корневой ФС.

### 4.1. Пакеты ОС

```bash
apt install postgresql redis-server unbound smartmontools jq
```

`jq` нужен скрипту Telegram-доставки monitor (`/opt/crawler/bin/
telegram-notify.sh`, T-162/T-173: алерты и отчёты нагрузки — сборка
JSON-сообщения и маскировка секрета в логе; без него отправка молча
падает exit 127 — замер 24.10 на боевой установке).

Зависимости Chromium headless-shell (`libnspr4 libnss3 fontconfig` +
шрифты) — отдельным списком при его установке: §4.3 ниже.

Drop-in'ы бюджета (копируются каталогами — имя каталога = имя юнита):

```bash
cp -r deploy/systemd/postgresql@.service.d /etc/systemd/system/
cp -r deploy/systemd/redis-server.service.d /etc/systemd/system/
cp -r deploy/systemd/unbound.service.d /etc/systemd/system/
systemctl daemon-reload
```

Роль и БД приложения (аналог deploy/docker/postgres/init — туда смотрит
dev; расширение pg_stat_statements и preload — postgresql-конфиг T-086):

```bash
sudo -u postgres psql -c "CREATE ROLE crawler LOGIN PASSWORD '...';"
sudo -u postgres createdb -O crawler crawler
sudo -u postgres psql -d crawler -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"
sudo -u postgres psql -d crawler -c "GRANT pg_read_all_stats TO crawler;"
```

Грант `pg_read_all_stats` (T-177): без него `pg_stat_activity` прячет
текст запросов чужих сессий от роли `crawler` — цикл медленных запросов
monitor видит длительности, но не тексты (сообщения с пометкой «текст
недоступен»); грант даёт чтение статистики (pg_stat_activity/
pg_stat_statements), не прав.

Unbound: конфиг рекурсии — `deploy/docker/unbound/unbound.internet.conf`
(кэши под бюджет ≈ 0,7 ГБ уже рассчитаны в нём; копия в
`/etc/unbound/unbound.conf.d/crawler.conf` с правками `interface:
127.0.0.1` и `access-control` только для loopback — в нативной установке
клиенты все локальные; DNSSEC-валидатор выключен в самом конфиге).
Хостовому резолверу обязательного пути нет, но чтобы страницы рендера и
прочий софт сервера ходили через локальный кэш (ТЗ 5.4), укажите unbound
в `/etc/resolv.conf` (`nameserver 127.0.0.1`) или в systemd-resolved.

Redis слушает 127.0.0.1 по умолчанию пакета; maxmemory/eviction — конфиг
T-087 (≤ 512M, под потолком MemoryMax).

### 4.2. Бинарники Go

Статические сборки — как боевой образ рендера
(`deploy/docker/Dockerfile.render`): `CGO_ENABLED=0`, `-trimpath -ldflags
"-s -w"`. На машине с Go (или в контейнере tools: `make shell`):

```bash
# Бандл Tailwind лендинга (T-124): go:embed вбирает internal/web/static/
# app.css; в git он не хранится (.gitignore), без этого шага лендинг
# встанет без стилей. Собирается одноразовым контейнером (node не
# нужен); боевая docker-сборка делает это сама (стадия css Dockerfile.go).
make web-css

version="$(git describe --tags --always --dirty 2>/dev/null || echo unknown)"
revision="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
mkdir -p out
for svc in ingest scheduler dns-worker http-worker render-worker analyzer monitor web dashboard crawlerctl; do
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X crawler/internal/metrics.buildMode=prod -X crawler/internal/metrics.buildVersion=$version -X crawler/internal/metrics.buildRevision=$revision" \
      -o "out/$svc" "./cmd/$svc"
done
sudo install -D -t /opt/crawler/bin out/*
```

`buildMode=prod` помечает боевые сборки в метрике `crawler_build_info`
(метка `mode`); `buildVersion`/`buildRevision` (T-111) вычисляются из git —
`version` = `git describe --tags --always --dirty` (тег либо хеш с пометкой
несохранённых правок), `revision` = короткий хеш коммита; при недоступном
git подставляется `unknown`. Dev-сборки без `-X` остаются на `dev-stub` /
`dev` / пустой `revision`. В Docker-сборке значения передаются build-аргами
`BUILD_VERSION`/`BUILD_REVISION`: `.git` исключён из контекста
(`.dockerignore`), значения вычисляет вызывающая сторона — так делает
`make build-runtime`; прямая `docker build` без арок получает `unknown`.

Миграции схем зашиты в бинарники (`migrations/embed.go`, golang-migrate).

### 4.3. Chromium

Та же сборка headless-shell, что в dev/dev-runtime (версия —
`CHROMEDP_HEADLESS_IMAGE` в `.env.example`, фиксируется): дистрибутив
`chromedp/headless-shell` распаковывается в `/opt/crawler/chromium/`
(исполняемый `headless-shell` в корне — путь в `ExecStart` юнита). Отдельный
пакет/каталог вместо пакета ОС: версия браузера согласована с chromedp-стеком
воркера, а Ubuntu-пакет chromium — snap-обёртка, непригодная для headless-CDP
без user-data-dir ухищрений.

На хост переносится только бинарь, а рассчитан он на окружение образа —
после распаковки дистрибутива установите недостающие библиотеки и
шрифтовой стек (до первого старта юнита, §4.6):

```bash
apt install libnspr4 libnss3 fontconfig fonts-liberation fonts-dejavu-core
# библиотеки бинаря: libs ok — разрешаются; LIBS MISSING — not found/сбой ldd:
ldd /opt/crawler/chromium/headless-shell 2>&1 \
  | grep -E 'not found|No such file|not a dynamic' \
  && echo "LIBS MISSING" || echo "libs ok"
# шрифтовой стек: fonts ok — виден; FONTS MISSING — шрифтов нет:
fc-list | grep -q . && echo "fonts ok" || echo "FONTS MISSING"
```

- `libnspr4 libnss3` — обязательные библиотеки для запуска бинаря (ставились
  при физическом деплое T-135, до T-140 в этом списке отсутствовали);
- `fontconfig fonts-liberation fonts-dejavu-core` — шрифтовой стек Skia:
  без fontconfig часть страниц (требующих шрифтового стека) роняет
  Chromium FATAL `SkFontMgr_FontConfigInterface: Not implemented`
  (SIGTRAP) → краш-луп до start-limit (10 рестартов за 5 минут, §5) →
  юнит failed, q:render копит попытки. В dev дефект невидим — fontconfig
  и шрифты уже внутри образа `chromedp/headless-shell`, на голом хосте
  их нет. Инцидент боевого замера T-092 и операционное исправление —
  [`load-measurements.md`](load-measurements.md) §4; юнит, уже ушедший
  в failed, после установки пакетов поднимается руками: `systemctl
  reset-failed crawler-chromium && systemctl start crawler-chromium` (§5).

### 4.4. Пользователи и каталоги

```bash
groupadd -r crawler
for s in ingest scheduler dns-worker http-worker render-worker analyzer monitor web dashboard chromium; do
    useradd -r -g crawler -d /var/lib/crawler -s /usr/sbin/nologin "crawler-$s"
done
install -d -o root -g crawler -m 0755 /var/lib/crawler
install -d -o crawler-ingest -g crawler -m 0770 /var/lib/crawler/incoming
install -d -o root -g crawler -m 2775 /var/lib/crawler/content
install -d -o crawler-chromium -g crawler -m 0770 /var/lib/crawler/chromium
install -d -o root -g crawler -m 0750 /etc/crawler
```

### 4.5. Конфигурация

```bash
cp config.example.yaml /etc/crawler/config.yaml   # правки — ниже
cp deploy/systemd/crawler.env.example /etc/crawler/crawler.env
chmod 0640 /etc/crawler/crawler.env && chown root:crawler /etc/crawler/crawler.env
cp -r rules /etc/crawler/rules                    # правила Laravel для analyzer
```

Обязательные правки `config.yaml` относительно примера
(полный справочник — [`configuration.md`](configuration.md)):

- `postgres.dsn`, `redis.addr` — заменить примеры на локальные значения
  (или оставить как есть и задать точные в `crawler.env` — переменные
  окружения важнее YAML, docs/configuration.md);
- `postgres.pool_size` — на один сервис, а бюджет ТЗ §6.3 (20–50) —
  суммарный на все: значение примера (5) даёт 8 × 5 + dashboard (2,
  задаёт его юнит — девятый сервис по 5 не влезал в потолок;
  предупреждение о глобальной переменной — ниже) +
  `crawlerctl` (2) + админ ≈ 44–46 при `max_connections=50` (3 слота
  резервирует `superuser_reserved_connections`). Менять — только
  с пересчётом суммы; сразу всем сервисам —
  `CRAWLER_POSTGRES_POOL_SIZE` в `crawler.env`
  (docs/db.md, «Бюджет соединений»). Внимание: EnvironmentFile
  (crawler.env) переопределяет Environment юнита — включение глобальной
  переменной затронет и пул дашборда (9 × 5 + 2 + админ ≈ 48 упрётся
  в потолок доступных слотов); безопасный глобальный максимум — 4
  (9 × 4 + 2 + админ ≈ 40), либо поднимайте `max_connections`,
  либо снижайте пул отдельных сервисов;
- `ingest.dir: /var/lib/crawler/incoming`, `content.dir:
  /var/lib/crawler/content`, `content.zstd_dictionary:
  /var/lib/crawler/content/dict.zst`, `analyzer.rules_dir: /etc/crawler/rules`;
- `render.remote_url: "ws://127.0.0.1:9222"` — внешний браузер-юнит
  (в примере пусто = собственный процесс воркера, это режим docker-runtime);
- `monitor.listen` — в примере уже `127.0.0.1:9100`: метрики не
  выставляются на боевой интерфейс (ТЗ §9); юнит monitor дополнительно
  фиксирует loopback переменной `CRAWLER_MONITOR_LISTEN` (окружение важнее
  YAML — страховка от config.yaml без правок), остальные сервисы слушают
  loopback через `METRICS_ADDR` в юнитах (127.0.0.1:9101…9108);
- `monitor.zone_stats_interval: "30m"` — в примере уже боевое значение
  (совпадает с дефолтом, T-199): реконсиляция инкрементальных счётчиков
  — единственный оставшийся полный проход `domains` (~40 с на 42 млн
  строк); при интервале 5 минут она давала 48 % суммарного медленного
  времени БД (замер окна медленных запросов после деплоя 3bffc62,
  26.10: 100 событий >3 с, медиана 39 с, max 501 с) — оператор разредил
  интервал до 30 минут по заготовке ТЗИ §7.1 (дрейф счётчиков в
  стационаре обязан быть нулевым). Цикл алертов отвязан собственным
  полем `monitor.alerts_interval` (T-206, дефолт 5 мин — прежняя
  частота): окна детекции вернулись к прежним минутам (queue_dead
  ≈15 мин, memory_high/render_debt — 10 мин, окно bad_hashes — 75 мин),
  порог остановки прогресса остаётся в периодах реконсиляции —
  `AlertStallFactor × zone_stats_interval`, 2,5 ч (метку
  `zones.updated_at` пишет именно реконсиляция);
- `web.listen` — публичный адрес лендинга: дефолт loopback
  `127.0.0.1:8080` (рядом с nginx — §6 ниже), смена на unix-сокет —
  абсолютным путём в значении;
- `dashboard.username` / `dashboard.password_hash` — креды оператора
  дашборда (обязательны для запуска `crawler-dashboard`, ТЗД §7.7).
  Хэш генерируется CLI (пароль — аргументом или stdin; stdin — не
  остаётся в истории shell):

  ```bash
  /opt/crawler/bin/crawlerctl dashboard-secret < password.txt
  printf 'пароль' | /opt/crawler/bin/crawlerctl dashboard-secret
  ```

  Формат строки — `pbkdf2-sha256$<итерации>$<соль>$<ключ>` (600 000
  итераций по умолчанию); открытый пароль в конфигурации и окружении
  запрещён (ТЗД §7.2), смена `password_hash` инвалидирует все
  cookie-сессии (docs/configuration.md, «dashboard»);
  `dashboard.probes` — адреса `/livez` сервисов конвейера для секции
  здоровья: в systemd-деплое это loopback-адреса метрик юнитов
  (`http://127.0.0.1:9100` monitor, `9101` ingest, `9102` scheduler,
  `9103` dns-worker, `9104` http-worker, `9105` render-worker,
  `9106` analyzer);
- `http.user_agent` — реальные контакты оператора (если не задан в env);
- `dns.ipv6: true` — если у сервера есть IPv6 (в docker-сети его нет).

Необязательная настройка после первого запуска — доставка алертов
monitor и отчётов нагрузки в Telegram (T-162/T-173): **обязательны оба
секрета `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID`** (тот же бот и группа,
что у извещений CI/CD) в `/etc/crawler/crawler.env` — образец и где
брать значения: `deploy/systemd/crawler.env.example`; доставить можно
ops-workflow'ем `ops-telegram-env` (Actions → Run workflow: значения
живут в секретах репозитория GitHub и оттуда не читаются — CI
единственный держатель, workflow дописывает их в env-файл и
перезапускает monitor). Скрипт отправки `telegram-notify.sh` едет в
деплой-архиве вместе с бинарниками (deploy.yml кладёт
`deploy/telegram-notify.sh` в архив, install распаковывает его в
`/opt/crawler/bin` и chmod 0755) — отдельной установки не требует;
скрипту нужен пакет `jq` (§4.1). Третье условие — сеть до
`api.telegram.org`: с российских хостингов API Telegram часто
недоступен (дроп TCP при живом интернете) — доставка тогда идёт
через `https_proxy` в `crawler.env` и прокси на внешнем VPS.
Пошагово — [`operations.md`](operations.md) §4.3 (секреты, скрипт,
прокси) и §5 (запись 24.10 — установка прокси); без секретов алерты
и отчёты остаются Warn/Info в journald monitor.

### 4.6. Юниты и запуск

```bash
cp deploy/systemd/crawler*.service deploy/systemd/crawler*.timer deploy/systemd/crawler.target /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now crawler.target \
    crawler-chromium.service crawler-chromium-recycle.timer crawler-ingest.service crawler-scheduler.service \
    crawler-dns-worker.service crawler-http-worker.service crawler-render-worker.service \
    crawler-analyzer.service crawler-monitor.service crawler-web.service \
    crawler-dashboard.service
```

Инфраструктура включается пакетами: `systemctl enable --now postgresql
redis-server unbound`.

### 4.7. Первый старт: миграции

Отдельного шага миграций нет: каждый сервис применяет миграции PostgreSQL
при старте под advisory-блокировкой (T-002, internal/migrate — docs/services.md,
«Жизненный цикл»). Первым поднявшийся сервис накатывает схему, остальные
ждут блокировку и видят актуальную версию. Порядок включения юнитов не важен.

### 4.8. Мониторинг SMART/TBW накопителя

Комплект `deploy/smartd/` (T-089, ТЗ §10.3): конфиг smartd (critical
warning, рост журнала ошибок, температура), обработчик предупреждений в
syslog/journald и доступ алерту износа сервиса monitor к `/dev/nvme*`
(udev-группа `nvme-smart` + drop-in юнита). Установка, проверка и откат —
[`deploy/smartd/README.md`](../deploy/smartd/README.md); обоснование,
оценка TBW от нагрузки и связь слоёв — [`disks.md`](disks.md).

### 4.9. Входящий фид NRD smet.cz (cron)

Комплект [`deploy/nrd/`](../deploy/nrd/): ежесуточная автоматическая
подача списка новых доменов (NRD) smet.cz (~5·10⁵ доменов/день, разных
зон) в наблюдаемую папку ingest. Cron запускает скрипт в **05:33 UTC**
(перенос с 20:33 UTC 26.10, T-205: применение суточного файла шло
минутами в вечернее окно CPU-голода и полуночный шторм суточной
перепроверки; утро — вне часов голода, а день источника закрыт >9 ч —
контракт закрытых дней сохранён), тот скачивает **закрытый день** —
`https://smet.cz/nrd/data/daily/<дата
вчерашняя по UTC>.txt.gz` (~2,6 МиБ) — в `<ingest.dir>/add/nrd-<дата>.txt.gz`
по контракту доставки ([`incoming-files.md`](incoming-files.md) §2:
временное имя `*.part`, наблюдателем игнорируется, + атомарное
переименование). Список **зона-less**: зоны выводятся из самих доменов
и создаются автоматически (`incoming-files.md` §1), разбивки по
каталогам зон не требуется; `.gz` разбирается как архив (§9, один член).

```bash
install -m 0755 deploy/nrd/fetch-nrd.sh /usr/local/bin/crawler-fetch-nrd
install -m 0644 deploy/nrd/crawler-nrd.cron /etc/cron.d/crawler-nrd
```

Нюансы (подробно — [`deploy/nrd/README.md`](../deploy/nrd/README.md)):

- **почему вчерашний день, а не `today.txt`**: дни у источника —
  immutable-файлы `/daily/<дата>` («растёт до закрытия дня, затем не
  меняется», хранятся 90 дней); генерация идёт ~20:10–20:19 UTC, файл
  текущего дня до неё отсутствует (404), а сразу после — ещё растёт.
  В 05:33 UTC файл вчерашнего дня закрыт > 9 ч (генерация предыдущих
  суток) и неизменен — состав дня гарантированно полный, день читается
  из имени. Справка источника сама рекомендует забирать только закрытые
  дни;
- **cron не умеет часовой пояс**: в `crawler-nrd.cron` часы переведены
  в местное время сервера (Europe/Moscow, UTC+3 → 08:33); при смене
  зоны сервера (`timedatectl`) пересчитать обратно в 05:33 UTC.
  Файлы `/etc/cron.d` на сервере исполняет `systemd-cron` (классический
  демон cron не установлен): таймер виден как
  `systemctl list-timers 'cron-*'` → `cron-crawler-nrd-root-0.timer`;
- **отказы** (сеть/HTTP, не-gzip ответ): файл не ставится вовсе,
  запись `СБОЙ: …` в `journalctl -t crawler-nrd-fetch`, повтор —
  следующими сутками. День хранится 90 дней, пропущенный запуск
  восстанавливается вручную: `/usr/local/bin/crawler-fetch-nrd` или
  `CRAWLER_NRD_DAY=<дата> …` за любой день в глубину; массовая доливка
  истории/пропусков — `deploy/nrd/backfill-nrd.sh` (все закрытые дни
  раньше `NRD_BEFORE` по manifest.json, идемпотентен). Успех — строка
  `OK: день … закрыт` с `sha256` байтов `.gz`, совпадающим с journal-полем
  `ingest_files`. Повторная доставка того же дня — дубль по `sha256`,
  не обрабатывается (`incoming-files.md` §5).

Шаг — конфигурация оператора (как юниты и nginx, [`ci-cd.md`](ci-cd.md)):
конвейер деплоя его не ставит и не обновляет.

## 5. Эксплуатация

```bash
systemctl list-units 'crawler-*'        # статус всех сервисов набора
systemctl stop crawler.target           # весь набор (graceful: PartOf=);
                                       # таймер ресайкла отдельно — §2.2 (T-208)
journalctl -t crawler-scheduler -f      # логи по SyslogIdentifier
curl -fsS 127.0.0.1:9102/livez         # liveness каждого сервиса
curl -fsS 127.0.0.1:9102/healthz       # readiness (пул БД); /metrics — тот же порт
systemctl kill -s HUP crawler-analyzer  # перезагрузка правил Laravel без рестарта
/opt/crawler/bin/crawlerctl status      # CLI: статус, поиск, управление (ТЗ 4.1)
```

- **Рестарты**: `Restart=on-failure`, `RestartSec=5s`, предел
  `StartLimitIntervalSec=300` / `StartLimitBurst=10` — после 10 неудачных
  запусков за 5 минут юнит переходит в failed и ждёт оператора
  (`journalctl -u …` + `systemctl reset-failed crawler-… && systemctl start
  crawler-…`). Это защита от бесконечного цикла падений (например, недоступен
  PostgreSQL), а не от штатных сбоев: разовый crash-рестарт в предел не
  попадает (сбрасывается временем).
- **Обновление версий: границы CD и ручные шаги.** Конвейер деплоя
  ([`ci-cd.md`](ci-cd.md)) заменяет **только бинарники** `/opt/crawler/bin`.
  Юниты systemd, `/etc/crawler/config.yaml` и конфиги инфраструктуры
  (postgresql/redis/unbound) конвейер **не трогает** — при релизе, который
  меняет `deploy/systemd/*` или дефолты конфигурации:
  1. до/сразу после деплоя синхронизировать изменённые юниты:
     `cp deploy/systemd/<юнит> /etc/systemd/system/ && systemctl
     daemon-reload && systemctl restart <юнит>` (пример: MemoryMax=900M,
     T-143; MemoryHigh=850M, T-145; CPUWeight/CPUQuota, T-158 — §2.1;
     MemoryMax unbound 800M / dns-worker 384M, T-160 — §2;
     MemoryMax render-worker 256M / contexts 8, T-169 — §2);
     временные `systemctl set-property` drop-in'и
     (`/etc/systemd/system.control/`) удалить, чтобы юнит-файл
     был единственным источником;
  2. сверить `/etc/crawler/config.yaml` с новыми дефолтами: **явные
     значения в конфиге перебивают дефолты кода** (инцидент 22–23.10.2026:
     `scheduler.batch_size: 5000` и `render.contexts: 3` перекрыли новые
     дефолты 1000/4 волны 21 — эффект фикса не проявился до ручной правки);
  3. повторный запуск деплоя после исправления причины провала —
     `gh workflow run deploy.yml` (workflow_dispatch, катит текущий
     `master`): автосворач. откат (healthcheck) уже вернул `bin.prev`,
     сервер остаётся на прежней версии до повторного прогона.
- **Откат версии** (последний успешный деплой — `/opt/crawler/bin.prev`):
  `deploy/deploy-rollback.sh` — stop → восстановление `bin.prev` → start →
  проверка юнитов; его же автоматически запускает конвейер при провале
  healthcheck ([`ci-cd.md`](ci-cd.md)). Возвращаются только бинарники:
  схема БД остаётся на версии нового релиза, релиз с миграцией откатом не
  восстанавливается ([`operations.md`](operations.md) §4.1). Вручную — по
  SSH из чекаута репозитория (`ssh root@<сервер> 'bash -s' -- <
  deploy/deploy-rollback.sh`); на сервере скрипт не хранится — при
  недоступности чекаута те же шаги руками: `systemctl stop
  crawler.target && rm -rf /opt/crawler/bin && cp -a
  /opt/crawler/bin.prev /opt/crawler/bin && systemctl start
  crawler.target`.
- **Зависимости**: юниты приказаны `After=` инфраструктуре, но без
  `Requires=`/остановочной связки — падение и рестарт PostgreSQL/Redis не
  останавливает воркеры (пул pgx переподключается сам; очередь — Redis
  Streams — переживает рестарт, потеря восстанавливается планировщиком,
  ТЗ 3.4). Готовность БД при старте закрывается тем же restart-циклом
  (миграции падают кодом 1 до её появления).
- **Graceful shutdown** (ТЗ 3.4): SIGTERM от systemd → доработка текущей
  задачи или возврат в очередь, бюджет каркаса 10 с (`ShutdownTimeout`,
  docs/services.md), `TimeoutStopSec=30s` юнита даёт запас на закрытие пула
  и HTTP-сервера метрик.
- **Память**: `systemctl status crawler-http-worker` показывает
  `Memory: …` (текущее потребление cgroup); потолок виден в
  `systemctl show crawler-http-worker -p MemoryMax`. Проверка суточного
  прогона без OOM — критерий приёмки ТЗ 11.4 п. 7 (задача T-093,
  **выполнен** — окно 23–24.10 ≈25 ч: `journalctl -k --since
  "2026-10-23 09:00" --grep 'oom'` пуст — `--since` обязателен:
  без окна команда покажет записи инцидентов до окна, напр. cgroup-OOM
  T-157 от 23.10 05:48; oom_kill/NRestarts юнитов нулевые, пики
  в бюджетах MemoryMax —
  [`load-measurements.md`](load-measurements.md) §13).
- **Метрики**: `127.0.0.1:9100…9108/metrics` (monitor — 9100, web —
  9107, dashboard — 9108; второй+ экземпляр render-worker — 9109 и
  далее по имени экземпляра, §2.2). Скрейпит установленный на сервере
  Prometheus (его юнит — инфраструктура оператора, вне комплекта; конфиг
  джобов — как `deploy/docker/prometheus/prometheus.yml`, адреса
  хостовые).
- **Дашборд**: операторская сводка конвейера — напрямую на loopback
  `curl -fsS -u <username>:<пароль> http://127.0.0.1:8090/api/status`
  (Basic-креды из `dashboard.username`/`password_hash`; числа сходятся с
  `crawlerctl status` — общий пакет `internal/status`); публикация для
  браузера оператора — через reverse-proxy с TLS (§6).
- **Лендинг**: публичный URL системы для посетителей — через
  reverse-proxy с TLS (`curl -fsS https://<домен>/healthz` за nginx,
  §6 ниже); операторский дашборд — отдельный вход того же прокси (§6).
  Напрямую сервис отвечает на loopback `curl -fsS 127.0.0.1:8080/healthz`.
- **SMART/TBW накопителя**: предупреждения smartd и алерта monitor — в
  journald (`journalctl -t smartd -p warning`), живые показания —
  `smartctl -H -A /dev/nvme0` (percentage_used, TBW). Детали —
  [`disks.md`](disks.md) «Мониторинг SMART и TBW».

## 6. Публичный вход: nginx + TLS (T-124; дашборд — ТЗД §9.1)

Лендинг (`crawler-web.service`) слушает loopback (`web.listen`, дефолт
`127.0.0.1:8080`) — наружу систему публикует reverse-proxy,
терминирующий TLS. Первый образец входа —
[`deploy/nginx/`](../deploy/nginx/) (`crawler.example.conf` + README:
установка, заголовки, схема для sitemap.xml, вариант unix-сокета вместо
TCP — `CRAWLER_WEB_LISTEN=/run/crawler-web/web.sock` в `crawler.env`).
Через прокси наружу идёт только лендинг: метрики и пробы web
(`127.0.0.1:9107`) не публикуются (ТЗ §9). HTTP→HTTPS, ACME-проверки
certbot и заголовки безопасности — в образце; правки под конкретный
домен — по месту деплоя.

Дашборд (`crawler-dashboard.service`) использует ту же схему (ТЗД §9.1):
слушает loopback `127.0.0.1:8090`, наружу — отдельный server-блок nginx
на свой домен/поддомен (например `dashboard.crawler.example.com`); блок
ниже сокращён до специфичного — server для HTTP→HTTPS с ACME-проверками
certbot и заголовок HSTS возьмите из блока лендинга выше. TLS обязателен:
авторизация — HTTP Basic, креды нельзя передавать открытым текстом вне
loopback (ТЗД §7.1); `X-Real-IP` нужен per-IP защите от подбора (ТЗД
§7.6 — сервис доверяет заголовку от loopback-пира). Метрики и пробы
дашборда (`127.0.0.1:9108`) через прокси не идут:

```nginx
server {
    listen 443 ssl http2;
    listen [::]:443 ssl http2;
    server_name dashboard.crawler.example.com;
    ssl_certificate     /etc/letsencrypt/live/dashboard.crawler.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/dashboard.crawler.example.com/privkey.pem;
    ssl_protocols       TLSv1.2 TLSv1.3;
    add_header X-Content-Type-Options "nosniff" always;
    location / {
        proxy_pass http://127.0.0.1:8090;
        proxy_set_header Host      $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

## 7. Границы комплекта (смежные задачи этапа 9)

- **T-086** — тюнинг postgresql-конфига под NVMe (docs/db.md): аллокации
  сервера обязаны вписываться в `MemoryMax=3G` drop-in'а (включая пики
  обслуживания — work_mem/maintenance/autovacuum поверх shared_buffers).
- **T-087** — конфиг redis: `maxmemory` ≤ 512M и eviction — под потолком
  drop-in'а (eviction раньше OOM).
- **T-088** — sysctl ядра (`ip_local_port_range`, `tcp_tw_reuse`,
  `fs.file-max`/`nr_open`, `nf_conntrack`, UDP-буферы). Юниты фиксируют
  только per-process `LimitNOFILE=65535` — паритет с dev-окружением
  (`deploy/docker/compose.yaml`, ulimits), потолок sysctl ведёт T-088.
- **T-089** — диски и SMART — [`disks.md`](disks.md): тома PostgreSQL
  (`/var/lib/postgresql`) и контента (`/var/lib/crawler/content`) —
  LVM/ext4/`noatime`, шаг установки §4 выше; мониторинг износа NVMe —
  smartd + доступ юнита monitor к `/dev/nvme*` (`deploy/smartd/`, §4.8).

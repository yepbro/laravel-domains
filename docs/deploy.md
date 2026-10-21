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
| `unbound.service` + drop-in | **700M** | Unbound ≈ 0,7 ГБ |
| `crawler-*.service` (9 юнитов) | **1664M суммарно** | Go-сервисы ≈ 1,3 ГБ (7 конвейерных) + web этапа 13 + dashboard этапа 15 |
| `crawler-chromium.service` | **2048M** | Chromium ≈ 2 ГБ |

Итого 8124M из 8192M (строка «Go-сервисы ≈ 1,3 ГБ» ТЗ покрывала семь
конвейерных юнитов — 1280M; публичный `web` лендинга — продуктовое
расширение этапа 13 вне основного ТЗ — добавляет свою строку 256M,
операторский `dashboard` — расширение этапа 15 (ТЗД) — 128M).
Потолки — не резервация: MemoryMax выше плановой
строки бюджета там, где над внутренним лимитом живут служебные памяти
процесса или пики обслуживания — Redis (maxmemory 512mb + ~128 МБ,
eviction раньше OOM — docs/redis.md §7), PostgreSQL (work_mem/maintenance/
autovacuum поверх shared_buffers — docs/db.md, «Память»); штатно инстансы
живут в строках бюджета (плановые строки суммарно ≈ 7,4 ГБ), остаток
≈ 0,6 ГБ и незаполненность потолков — ОС и page cache (ТЗ 3.2).

Раскладка строки «Go-сервисы ≈ 1,3 ГБ» по девяти юнитам:

| Юнит | MemoryMax |
|---|---|
| `crawler-http-worker.service` | 384M |
| `crawler-dns-worker.service` | 256M |
| `crawler-web.service` | 256M |
| `crawler-ingest.service` | 128M |
| `crawler-scheduler.service` | 128M |
| `crawler-render-worker.service` | 128M |
| `crawler-analyzer.service` | 128M |
| `crawler-monitor.service` | 128M |
| `crawler-dashboard.service` | 128M |

- **http-worker — 384M**, самый тяжёлый: до 64 доменных задач на экземпляр,
  тела страниц до 2 МиБ удерживаются до конца задачи домена (дедупликация
  алиасов — docs/services.md), плюс кэш итоговых URL и пишущее хранилище
  контента; 384m — паритет с dev-профилем app;
- **dns-worker — 256M**: пул 2000 UDP-сокетов (`dns.udp_pool_size`) и буферы
  обменов, TTL-кэш редиректных резолвов (T-047);
- **web — 256M** (этап 13, вне строки ТЗ): кроме лёгкой работы с БД живые
  проверки формы гонят полный конвейер checker (T-123: DNS → HTTP с телами
  страниц до `http.max_body_bytes` → рендер SPA → анализ) — сетевые буферы
  одного доменного эпизода, паритет с dev-профилем app;
- **ingest / scheduler / analyzer / monitor / render-worker / dashboard —
  по 128M**: пакетная работа с БД без больших буферов (потоковый разбор
  файлов, SQL-пачки, разбор уже сжатого контента постранично, счётчики);
  render-worker — только Go-часть: Chromium у него в отдельной cgroup;
  dashboard — read-only читатель БД/Redis (агрегаты считает SQL-движок,
  в памяти — только снимок и HTML/JSON-артефакты embed);
- остаток строки ≈ 50M — `crawlerctl`: CLI без юнита (ТЗ 4.1), запускается
  оператором эпизодически (`crawlerctl status/domain/zones/...`), его память
  не фиксируется потолком — команда короткоживущая.

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
dev-сервиса chromium, docs/docker.md §6).

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
apt install postgresql redis-server unbound smartmontools
```

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
```

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

### 4.6. Юниты и запуск

```bash
cp deploy/systemd/crawler*.service deploy/systemd/crawler.target /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now crawler.target \
    crawler-chromium.service crawler-ingest.service crawler-scheduler.service \
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

## 5. Эксплуатация

```bash
systemctl list-units 'crawler-*'        # статус всех сервисов набора
systemctl stop crawler.target           # весь набор (graceful: PartOf=)
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
  прогона без OOM — критерий приёмки ТЗ 11.4 п. 7 (задача T-093): журнал
  ядра `journalctl -k --grep 'oom'` должен быть пуст, рестарты юнитов —
  по счётчику `NRestarts`.
- **Метрики**: `127.0.0.1:9100…9108/metrics` (monitor — 9100, web —
  9107, dashboard — 9108). Скрейпит установленный на сервере Prometheus
  (его юнит — инфраструктура оператора, вне комплекта; конфиг джобов — как
  `deploy/docker/prometheus/prometheus.yml`, адреса хостовые).
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

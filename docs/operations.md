# Эксплуатация: запуск и обслуживание

Точка входа для оператора: от чистого сервера до регулярной
эксплуатации. Специализированные темы раскрыты в отдельных документах
(карта ниже), здесь — сценарии «что делать по шагам» и регулярное
обслуживание. Разработчику — [`dev-environment.md`](dev-environment.md).

## 1. Карта документации

| Документ | Что внутри | Когда читать |
|---|---|---|
| [`README.md`](../README.md) | быстрый старт, структура репозитория, CI | первое знакомство |
| **эта страница** | сценарии запуска, обновление, логи/метрики/алерты | эксплуатация |
| [`dev-environment.md`](dev-environment.md) | dev-окружение Docker: профили, testbed, CLI, диагностика | разработка и отладка |
| [`docker.md`](docker.md) | шпаргалка Docker Compose (запуск, команды, тома) | работа с dev-стеком |
| [`configuration.md`](configuration.md) | справочник конфигурации: все секции/поля/env, валидация | любая правка config.yaml |
| [`incoming-files.md`](incoming-files.md) | контракт входных файлов `incoming/` | подача доменов |
| [`services.md`](services.md) | каркас сервисов, жизненный цикл, метрики и алерты | поведение сервисов |
| [`schema.md`](schema.md) / [`migrations.md`](migrations.md) / [`db.md`](db.md) | схема БД, миграции, пул и тюнинг PostgreSQL | работа с данными |
| [`redis.md`](redis.md) | очередь задач и конфигурация Redis | очереди |
| [`content.md`](content.md) / [`render.md`](render.md) / [`analyzer.md`](analyzer.md) | хранилище контента, рендер, анализатор | этапы конвейера |
| [`runbooks.md`](runbooks.md) | процедуры: словарь zstd, `q:dead`, переанализ | по необходимости |
| [`mutation-testing.md`](mutation-testing.md) | мутационное тестирование: `make mutate`, пороги, отчёты | качество тестов |
| [`deploy.md`](deploy.md) | боевой деплой systemd: юниты, бюджет памяти | установка на сервер |
| [`ci-cd.md`](ci-cd.md) | непрерывный деплой GitHub Actions: сборка, установка, откат | обновления ПО |
| [`disks.md`](disks.md) / [`os-tuning.md`](os-tuning.md) | тома и SMART, sysctl боевого сервера | подготовка сервера |
| [`load-measurements.md`](load-measurements.md) | нагрузочные замеры боевого сервера (T-092): скорости этапов, память, оценка цикла | планирование мощности |

## 2. Сценарий: первичный запуск (dev)

Полная инструкция — [`dev-environment.md`](dev-environment.md) §1–3;
здесь — контрольные шаги и критерии «получилось».

1. `cp .env.example .env` — окружение (Linux, Docker ≥ 24, Compose v2;
   Go на хосте не нужен).
2. `make up` — инфраструктура + testbed + приложения; первый запуск
   5–15 минут (сборка образов).
3. `make ps` — дождаться `healthy` у всех сервисов (healthcheck —
   `/livez` на порту метрик; readiness `/healthz` с проверкой БД —
   отдельно, [`services.md`](services.md) «Пробы живости и готовности»).
4. `make seed` — тестовые списки доменов в `var/incoming/`; наблюдать
   обработку: файлы уходят в `var/incoming/processed/` (контракт —
   [`incoming-files.md`](incoming-files.md)).
5. Проверить конвейер: `make status` (сводка по зонам/очередям),
   `make domain NAME=<домен из seed>` (статус, результаты, признаки).
6. `make test` / `make test-integration` — проверки окружения.

Дальше: подача своих списков ([`incoming-files.md`](incoming-files.md)),
правила Laravel (§5 dev-environment), мониторинг (§4.3 ниже).

## 3. Сценарий: боевой деплой

Комплект: [`deploy/systemd/`](../deploy/systemd/) (T-085), инструкция —
[`deploy.md`](deploy.md). Порядок первого развёртывания (все шаги
детально — по ссылкам):

1. **Диски** — тома `/var/lib/postgresql` и `/var/lib/crawler/content`
   (LVM/ext4/`noatime`) **до** установки пакетов:
   [`disks.md`](disks.md) §4.
2. **Пакеты ОС** — `postgresql redis-server unbound smartmontools` +
   drop-in'ы бюджета памяти + роль/БД приложения + конфиг unbound:
   [`deploy.md`](deploy.md) §4.1 (зависимости Chromium headless-shell —
   отдельный список там же, §4.3, ставятся на шаге 4).
3. **Sysctl** — `deploy/sysctl/99-crawler.conf` в `/etc/sysctl.d`
   (порты/TIME_WAIT/conntrack/UDP-буферы):
   [`os-tuning.md`](os-tuning.md), `deploy/sysctl/README.md`.
4. **Бинарники** — статические сборки в `/opt/crawler/bin` и Chromium
   headless-shell в `/opt/crawler/chromium` с пакетами его зависимостей
   (`libnspr4 libnss3 fontconfig` + шрифты: без шрифтового стека Skia
   роняет браузер FATAL на части страниц — краш-луп до start-limit):
   [`deploy.md`](deploy.md) §4.2–4.3.
5. **Пользователи и каталоги** — `crawler-<сервис>` в группе `crawler`,
   рабочие каталоги с правами: [`deploy.md`](deploy.md) §4.4.
6. **Конфигурация** — `/etc/crawler/config.yaml` (обязательные правки
   относительно примера — DSN/пути/`render.remote_url`/`monitor.listen`/
   `http.user_agent`: [`deploy.md`](deploy.md) §4.5; справочник —
   [`configuration.md`](configuration.md)) и `crawler.env`.
7. **Юниты и запуск** — `systemctl enable --now crawler.target …`:
   [`deploy.md`](deploy.md) §4.6. Миграции применятся автоматически при
   старте (отдельного шага нет, §4.1 ниже).
8. **Мониторинг SMART** — `deploy/smartd/`: [`deploy.md`](deploy.md)
   §4.8, [`disks.md`](disks.md) §6.
9. **Проверка**: `systemctl list-units 'crawler-*'` (все active),
   `curl -fsS 127.0.0.1:9100…9108/healthz`,
   `/opt/crawler/bin/crawlerctl status`; шрифтовой стек —
   `fc-list | grep -q . && echo "fonts ok" || echo "FONTS MISSING"`
   (FONTS MISSING — шрифтов нет, рендер краш-лупом уйдёт в start-limit;
   [`deploy.md`](deploy.md) §4.3).
10. **Подать домены** — файлы в `/var/lib/crawler/incoming/`
    (контракт — [`incoming-files.md`](incoming-files.md)).
11. **Входящий фид NRD** (опционально) — cron-выгрузка списка новых
    доменов smet.cz в `incoming/add/` ежедневно в 20:33 UTC:
    [`deploy/nrd/`](../deploy/nrd/), [`deploy.md`](deploy.md) §4.9.

Раскладка памяти (MemoryMax по юнитам) и обоснование — [`deploy.md`](deploy.md) §2.

## 4. Обслуживание

### 4.1. Обновление ПО и миграции

**Dev.** `git pull` → `make up`: пересоберётся изменившееся, air дальше
следит за кодом; после смены зависимостей — `make mod-download`.
Миграции применяют сами сервисы при старте (рестарт контейнера).

**Бой.** Штатный путь — CI/CD: пуш/мерж в `master` с зелёным CI деплоится
автоматически (сборка → `/opt/crawler/bin` → рестарт → healthz). Откат
автоматический при провале любого шага после остановки набора (ТЗ §10.4):
провал установки — trap'ом install-скрипта, провал healthcheck — шагом
rollback конвейера (`deploy/deploy-rollback.sh`, восстановление
`bin.prev`); исключения — первый деплой без `bin.prev`, сбой остановки
набора и срыв связи ssh — тогда извещение прямо зовёт оператора
([`ci-cd.md`](ci-cd.md)). О каждом
деплое приходит извещение Telegram: ✅ успех; ⛔️ провал с шагом-виновником
и фактом отката (выполнен / выполнен, но не все юниты active / нечего
откатывать / не удался / неизвестен). «Требуется оператор» в тексте —
сигнал разбираться по [`ci-cd.md`](ci-cd.md) и `journalctl`. Ручной путь
(вне CI):

1. Собрать бинарники (машина с Go или `make shell` в dev-окружении):
   цикл `CGO_ENABLED=0 go build …` — [`deploy.md`](deploy.md) §4.2.
2. Установить в `/opt/crawler/bin` (скрипт атомарной замены —
   `deploy/deploy-install.sh`, проверка — `deploy/deploy-healthcheck.sh`).
3. `systemctl restart crawler.target` — **все юниты сразу**, не
   по одному: межпроцессный протокол пак-файлов хранилища (flock)
   рассчитан на одну версию ПО у писателей и компактификации
   ([`content.md`](content.md) «Закрытость для записи»).
4. Сервисы при старте применят новые миграции сами: advisory-блокировка
   сериализует одновременный старт, порядок включения не важен, рестарты
   идемпотентны ([`migrations.md`](migrations.md)).

**Контроль версии схемы** (что применено):

```sql
-- make psql / sudo -u postgres psql -d crawler
SELECT * FROM schema_migrations;   -- version, dirty = false
```

Откат схемы (`migrate-down`) — только вручную и осознанно: сервисы
умеют только up; даунгрейд ПО на откаченной схеме не поддерживается.
Восстановление после прерванной миграции (dirty) —
[`migrations.md`](migrations.md) «Восстановление после сбоя».

### 4.2. Логи

| Где | Команда |
|---|---|
| dev | `make logs SERVICE=<имя>`; `dc logs -f --tail=100 <сервис>` ([`docker.md`](docker.md) §2) |
| бой, сервисы | `journalctl -t crawler-<сервис> -f` (SyslogIdentifier; имена — `crawler-ingest`, `crawler-scheduler`, `crawler-dns-worker`, `crawler-http-worker`, `crawler-render-worker`, `crawler-analyzer`, `crawler-monitor`, `crawler-web`, `crawler-dashboard`) |
| бой, пакеты ОС | `journalctl -u postgresql -u redis-server -u unbound` |
| SMART | `journalctl -t smartd -p warning` ([`disks.md`](disks.md) §6) |

Формат — структурированный slog, каждая запись с атрибутом
`service=<имя>`; построчные отклонения ingest — отдельный поток
`service=rejected`. Уровень — `log.level` конфигурации
(`CRAWLER_LOG_LEVEL`, по умолчанию `info`).

### 4.3. Метрики и алерты

**Метрики.** Каждый сервис отдаёт `/livez`, `/healthz` и `/metrics` на
своём порту (таблица адресов — [`services.md`](services.md) «Метрики»;
бой — loopback). Ключевые метрики конвейера
([`services.md`](services.md), таблица «Метрики рабочей логики»):

- `crawler_stage_tasks_total{stage}` / `crawler_stage_seconds` —
  пропускная способность этапов dns/http/render/analyze;
- `crawler_queue_depth{queue}` — глубина очередей `q:*` (backlog групп потребления);
- `crawler_queue_dead_buried_total{queue}` / `crawler_redis_memory_ratio` —
  смерти задач (переводы в q:dead, T-154) и заполнение `maxmemory` Redis;
- `crawler_backlog_domains{state}` — pending/queued по БД;
- `crawler_domain_status_total{type}` — статусы доменов;
- `crawler_laravel_near_findings` — находки Laravel в серой зоне порога
  (материал калибровки правил, T-115);
- `crawler_laravel_appeared_total` / `crawler_laravel_lost_total` —
  появления/исчезновения Laravel-доменов (история детекции T-118);
- `crawler_service_up{service}` — живость (включая полную смерть
  процесса).

Prometheus: dev — профиль `monitoring` (UI на 127.0.0.1:9090); бой —
Prometheus оператора, конфиг джобов по образцу
`deploy/docker/prometheus/prometheus.yml` ([`deploy.md`](deploy.md) §5).

**CLI-сводка** — `crawlerctl status` (dev: `make status`): прогресс по
зонам (всего/проверено), длины очередей включая `q:dead`, скорость
(доменов/сутки, средняя с начала суток) и ETA завершения первого цикла;
`--json` — машинный вывод.

**Алерты** минимального набора ТЗ 5.9 оценивает сервис monitor циклом
`monitor.zone_stats_interval` (60 с): доля ошибок DNS, переполнение
очередей, остановка прогресса, нехватка места на диске, износ NVMe,
всплеск мусорных `content_hash` при очистке удалённых доменов (T-099),
OOM-убийства и крэш-рестарты юнитов crawler (T-145), неразобранный
`q:dead` и память Redis (T-154). Сигнал фронтовой
(на переходах): **Warn в лог/journald monitor** —

```bash
journalctl -t crawler-monitor -p warning      # бой
make logs SERVICE=monitor | grep -i warn      # dev
```

пороги и обоснование — [`services.md`](services.md) (разделы об
алертах), место на диске — `content.disk_free_percent`
([`content.md`](content.md) «Лимиты и переполнение диска»), SMART —
[`disks.md`](disks.md) §6.

**Мусорные `content_hash`** (`ingest_bad_hashes`): счётчик
`crawler_ingest_bad_hashes_total` сервиса ingest (удалённые очисткой
результаты с хэшем неверной длины) вырос на ≥ 10 за окно ≈15 минут —
программная ошибка писателя результатов (T-056: http-worker при записи
`subdomain_results` / render-worker при замене ссылки на DOM), а не
шум нагрузки: инвариант контракта — ровно 16 октет (ТЗ 6.1). Данные не
теряются и очистка не блокируется (poison pill обезврежен, ING-P2-4):
`refcount` по мусорной ссылке не менялся, строки удалены. Действия
оператора:

1. Оценить масштаб и период: лог ingest
   (`journalctl -t crawler-ingest | grep -i content_hash` / dev:
   `make logs SERVICE=ingest`) — каждый проход с аномалией пишет Warn с
   числом строк; темп счётчика —
   `rate(crawler_ingest_bad_hashes_total[15m])` в Prometheus.
2. Мусорные строки пишут http-worker (`Recorder`, T-058) и render-worker
   (замена `content_hash` на DOM): проверить их логи вокруг времени
   всплеска, вспомнить последние обновления ПО/деплой этих сервисов —
   регрессия писателя (разбор случаев — runbook §2: логи воркера по
   атрибуту `service` и времени).
3. Зафиксировать баг (репорт с логами и временем всплеска): строки уже
   удалены очисткой, SQL-проверке не подлежат — источник истины о
   масштабе лог Warn'ов и счётчик. Лечение — исправление писателя;
   перезапуск сервисов не требуется (конвейер и очистка продолжают
   работу).

Единичные значения ниже порога (реликты, посаженные до фикса ING-P2-4)
алертом не считаются. Источник алерта — зонд `/metrics` ingest
(`monitor.ingest_metrics_url`): ingest остановлен или метрики в
экспозиции нет — алерт «не измеряется» (Warn), сама смерть ingest видна
по `crawler_service_up`.

### 4.4. Регулярный контроль (чек-лист)

Ежедневно/по мере необходимости:

```bash
crawlerctl status                  # зоны, очереди, q:dead, ETA
crawlerctl near                    # серая зона Laravel-детекции (T-115)
crawlerctl check <домен>           # разбор спорного домена «на живую»
                                   # (T-119): read-only — без записи в БД
                                   # и очередях; для записи — recheck
# алерты и ошибки:
journalctl -t crawler-monitor -p warning --since -1d   # бой
journalctl -t crawler-nrd-fetch --since -1d            # доставка NRD-фида
                                                       # (deploy.md §4.9:
                                                       # OK ежедневно, СБОЙ — вручную)
systemctl --failed                 # юниты в failed (StartLimit)
systemctl show 'crawler-*.service' -p Id -p NRestarts   # OOM/рестарты юнитов (алерт unit_oom, T-145)
df -h /var/lib/crawler/content /var/lib/postgresql    # тома
```

- **`q:dead` не пуст или растёт** (алерт `queue_dead` бьёт на 3 прохода
  устойчивой глубины, T-154; темп смертей —
  `crawler_queue_dead_buried_total`) — разбрать:
  [`runbooks.md`](runbooks.md) §2.
- **Том контента близок к порогу** (`content.disk_free_percent`, 5%) —
  запись контента встала на паузу (алерт disk_free): освободить место
  (компактификация идёт сама), расширить том ([`disks.md`](disks.md)).
- **OOM/рестарты юнитов** — алерт `unit_oom` (T-145): один Warn на
  эпизод — `oom`/`oom_kill` (memory.events cgroup юнита) или `NRestarts`
  > 0. Сброс эпизода: `systemctl reset-failed` (или новый старт) обнуляет
  NRestarts, но счётчики `oom`/`oom_kill` живого юнита держатся до
  пересоздания cgroup — перезапуска юнита; деплой сбрасывает всё сам
  (stop → `reset-failed` → start в deploy-install.sh), вручную —
  `systemctl restart crawler-<сервис>`. Метрики —
  `crawler_unit_oom_events{unit,kind}` / `crawler_unit_restarts{unit}`
  ([`services.md`](services.md)); источник — `systemctl show` + чтение
  cgroup v2, в dev-контейнере алерт «не измеряется». Событие — симптом
  невыполнения бюджета памяти (http-worker — MemoryHigh/MemoryMax,
  [`deploy.md`](deploy.md) §2): разбирать по `journalctl -k --grep oom`
  и логу юнита.
- **Память**: `systemctl status crawler-<сервис>` показывает `Memory:`;
  потолок — `systemctl show -p MemoryMax` (http-worker — и мягкий
  `-p MemoryHigh`, T-145). Суточный прогон без OOM —
  критерий ТЗ 11.4 п. 7 (T-093): `journalctl -k --grep oom` пуст,
  рестарты — по `NRestarts` (бьёт алерт unit_oom, T-145).
- **Память Redis** — `crawler_redis_memory_ratio` (алерт `redis_memory`
  при ≥ 80 % `maxmemory`, T-154; при noeviction — предупреждение
  отказов записи): сверить `redis-cli INFO memory`, разбор —
  [`redis.md`](redis.md).
- **SMART/TBW** — `smartctl -H -A /dev/nvme0`, предупреждения smartd:
  [`disks.md`](disks.md) §6.
- **Отказоустойчивость**: падение PostgreSQL/Redis не останавливает
  воркеры (переподключение/`Warn`-циклы, ТЗ 3.4); потеря Redis
  восстанавливается lease-проходом за ≈ `scheduler.lease`
  ([`redis.md`](redis.md)).

### 4.5. Остановка и перезапуск

Graceful shutdown встроен в каркас (ТЗ 3.4): SIGTERM → доработка
текущей задачи или возврат в очередь (бюджет 10 с) → закрытие пула БД.

```bash
systemctl stop crawler.target      # весь набор (бой); точечно: systemctl stop crawler-http-worker
make down                          # dev (тома сохраняются)
```

Перезапуск любого сервиса безопасен: недоработанные задачи вернутся
повторной доставкой, домены — lease-проходом (ТЗ 11.4 п. 9).

## 5. Журнал эксплуатационных работ (боевой сервер)

Хронология внеплановых работ на проде: что менялось руками, почему и как
откатить. Штатные деплои CD здесь не фиксируются (их журнал — извещения
Telegram и Actions).

### 2026-10-22 — раскатка волны 20 (T-138…T-140) и стабилизация

- Деплой `be3a57f` через CD (18:06 MSK), все healthz ok.
- Раскатка T-139 по шагам (load-measurements §3): конфиг
  `/etc/crawler/config.yaml` дополнен `http.task_*` (шаг 128 → позже 256,
  итоговое 192, см. 23.10), юнит http-worker синхронизирован из репо,
  drop-in'ы `set-property` сняты.
- **Redis, временные меры T-093** (все сняты 23.10, см. ниже):
  `maxmemory` 512→600→640→900 MiB runtime и персистентно,
  cgroup `MemoryMax` 640 MiB→1 GiB; юнит `t092-trimhist` (XTRIM MINID
  старейшего pending раз в 2 мин, без потери работы) — костыль до
  код-фикса T-141.
- Морозка притока на время замеров: `systemctl stop crawler-scheduler
  [crawler-dns-worker]` и обратный запуск; q:dns-сток 1735 легаси-пачек
  (по ~237 КиБ) держал Redis у потолка — источник OOM-окна 16:00–18:15
  (пост-мортем — load-measurements §11.1).
- Замер шага 256 (12 мин): 47.6 дом/с, anon 834–873 MiB — критерий §3
  не достигнут; оставлен рабочий шаг 192 (отчёт §11).

### 2026-10-23 — волна 21 (T-141…T-143), разбор инцидентов, возврат бюджетов

- Деплой `4c5410f`: **первый прогон провалился по healthz http-worker**
  (стартовый пик памяти 256 слотов у потолка 900 MiB → GC-thrash;
  автосворач. откат сработал штатно). Повтор после `task_concurrency:
  256→192` через `gh workflow run deploy.yml` — success.
- Конфиг: `scheduler.batch_size: 5000→1000` (замер T-141: пачка
  1000 доменов ≈ 64 КиБ; «237 КиБ» была эпохой batch_size 5000),
  `render.contexts: 3→4` (T-142; бюджет Chromium ≈2 ГБ). Урок о явных
  значениях против дефолтов — deploy.md §5 «Обновление версий».
- T-149: `XTRIM q:dns MAXLEN 125` — срезаны 1754 легаси-пачки (617 МиБ;
  безопасно: источник истины PostgreSQL, lease 30 мин вернул бы
  непроверенное) → Redis 683→104 МиБ. Возвращены бюджеты:
  `maxmemory 512mb` (CONFIG SET + /etc/redis/redis.conf), cgroup
  redis `MemoryMax=640M`. Юнит `t092-trimhist` остановлен — заменён
  встроенным TrimLoop (T-141, цикл в scheduler, 1 мин).
- T-150: q:dead 5507 разобран (все — q:render, корень — OOM-окно Redis,
  не тарпиты; предметы `check_state=done`, ручной requeue не нужен),
  `XTRIM q:dead MAXLEN 0`; хвост PEL ~940 записей с RC=3 досыпает
  смерти сотнями — контроль XLEN и повторный XTRIM после стабилизации.
- T-151: zstd-словарь обучен и размещён: образцы
  `testdata/laravel-samples` → `/opt/crawler/samples`,
  `crawlerctl dict train /var/lib/crawler/content/dict.zst --samples
  /opt/crawler/samples` (ID `f2ddc163`, 5233 Б, оценка на образцах −32%).
  Писатели/readers подхватывают при следующем рестарте — контроль:
  строка лога о загрузке словаря у сервисов хранилища после деплоя.
- Локально (dev, не прод): дважды чинена гонка статических IP
  docker-сети (postgres→web, затем dashboard→testweb занимали чужие
  адреса при остановленных соседях) — порядок старта: остановить
  узурпатора → стартовать владельца IP → вернуть узурпатора.

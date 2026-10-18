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
| [`deploy.md`](deploy.md) | боевой деплой systemd: юниты, бюджет памяти | установка на сервер |
| [`disks.md`](disks.md) / [`os-tuning.md`](os-tuning.md) | тома и SMART, sysctl боевого сервера | подготовка сервера |

## 2. Сценарий: первичный запуск (dev)

Полная инструкция — [`dev-environment.md`](dev-environment.md) §1–3;
здесь — контрольные шаги и критерии «получилось».

1. `cp .env.example .env` — окружение (Linux, Docker ≥ 24, Compose v2;
   Go на хосте не нужен).
2. `make up` — инфраструктура + testbed + приложения; первый запуск
   5–15 минут (сборка образов).
3. `make ps` — дождаться `healthy` у всех сервисов (healthcheck —
   `/healthz` на порту метрик).
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
   [`deploy.md`](deploy.md) §4.1.
3. **Sysctl** — `deploy/sysctl/99-crawler.conf` в `/etc/sysctl.d`
   (порты/TIME_WAIT/conntrack/UDP-буферы):
   [`os-tuning.md`](os-tuning.md), `deploy/sysctl/README.md`.
4. **Бинарники** — статические сборки в `/opt/crawler/bin` и Chromium
   headless-shell в `/opt/crawler/chromium`: [`deploy.md`](deploy.md)
   §4.2–4.3.
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
   `curl -fsS 127.0.0.1:9100…9106/healthz`,
   `/opt/crawler/bin/crawlerctl status`.
10. **Подать домены** — файлы в `/var/lib/crawler/incoming/`
    (контракт — [`incoming-files.md`](incoming-files.md)).

Раскладка памяти (MemoryMax по юнитам) и обоснование — [`deploy.md`](deploy.md) §2.

## 4. Обслуживание

### 4.1. Обновление ПО и миграции

**Dev.** `git pull` → `make up`: пересоберётся изменившееся, air дальше
следит за кодом; после смены зависимостей — `make mod-download`.
Миграции применяют сами сервисы при старте (рестарт контейнера).

**Бой.**

1. Собрать бинарники (машина с Go или `make shell` в dev-окружении):
   цикл `CGO_ENABLED=0 go build …` — [`deploy.md`](deploy.md) §4.2.
2. Установить в `/opt/crawler/bin`.
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
| бой, сервисы | `journalctl -t crawler-<сервис> -f` (SyslogIdentifier; имена — `crawler-ingest`, `crawler-scheduler`, `crawler-dns-worker`, `crawler-http-worker`, `crawler-render-worker`, `crawler-analyzer`, `crawler-monitor`) |
| бой, пакеты ОС | `journalctl -u postgresql -u redis-server -u unbound` |
| SMART | `journalctl -t smartd -p warning` ([`disks.md`](disks.md) §6) |

Формат — структурированный slog, каждая запись с атрибутом
`service=<имя>`; построчные отклонения ingest — отдельный поток
`service=rejected`. Уровень — `log.level` конфигурации
(`CRAWLER_LOG_LEVEL`, по умолчанию `info`).

### 4.3. Метрики и алерты

**Метрики.** Каждый сервис отдаёт `/healthz` и `/metrics` на своём
порту (таблица адресов — [`services.md`](services.md) «Метрики»;
бой — loopback). Ключевые метрики конвейера
([`services.md`](services.md), таблица «Метрики рабочей логики»):

- `crawler_stage_tasks_total{stage}` / `crawler_stage_seconds` —
  пропускная способность этапов dns/http/render/analyze;
- `crawler_queue_depth{queue}` — глубина очередей `q:*` (backlog групп потребления);
- `crawler_backlog_domains{state}` — pending/queued по БД;
- `crawler_domain_status_total{type}` — статусы доменов;
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
очередей, остановка прогресса, нехватка места на диске, износ NVMe.
Сигнал фронтовой (на переходах): **Warn в лог/journald monitor** —

```bash
journalctl -t crawler-monitor -p warning      # бой
make logs SERVICE=monitor | grep -i warn      # dev
```

пороги и обоснование — [`services.md`](services.md) (разделы об
алертах), место на диске — `content.disk_free_percent`
([`content.md`](content.md) «Лимиты и переполнение диска»), SMART —
[`disks.md`](disks.md) §6.

### 4.4. Регулярный контроль (чек-лист)

Ежедневно/по мере необходимости:

```bash
crawlerctl status                  # зоны, очереди, q:dead, ETA
# алерты и ошибки:
journalctl -t crawler-monitor -p warning --since -1d   # бой
systemctl --failed                 # юниты в failed (StartLimit)
df -h /var/lib/crawler/content /var/lib/postgresql    # тома
```

- **`q:dead` не пуст или растёт** — разбрать:
  [`runbooks.md`](runbooks.md) §2.
- **Том контента близок к порогу** (`content.disk_free_percent`, 5%) —
  запись контента встала на паузу (алерт disk_free): освободить место
  (компактификация идёт сама), расширить том ([`disks.md`](disks.md)).
- **Память**: `systemctl status crawler-<сервис>` показывает `Memory:`;
  потолок — `systemctl show -p MemoryMax`. Суточный прогон без OOM —
  критерий ТЗ 11.4 п. 7 (T-093): `journalctl -k --grep oom` пуст,
  рестарты — по `NRestarts`.
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

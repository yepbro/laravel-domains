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
- `crawler_queue_depth{queue}` — глубина очередей `q:*` (backlog групп
  потребления; лаг ограничен длиной стрима — кламп фантома T-168);
- `crawler_queue_dead_buried_total{queue}` /
  `crawler_queue_dead_unidentified_total{queue}` /
  `crawler_redis_memory_ratio` — смерти задач (переводы в q:dead, T-154),
  карантины неидентифицируемых записей без `task_id` (T-260) и заполнение
  `maxmemory` Redis;
- `crawler_backlog_domains{state}` — pending/queued по БД;
- `crawler_domain_status_total{type}` — статусы доменов;
- `crawler_laravel_near_findings` — находки Laravel в серой зоне порога
  (материал калибровки правил, T-115);
- `crawler_laravel_appeared_total` / `crawler_laravel_lost_total` —
  появления/исчезновения Laravel-доменов (история детекции T-118);
- `crawler_web_checks_total{result}` / `crawler_web_check_errors_total{type}`
  / `crawler_web_check_seconds` — здоровье публичной формы проверки
  (web, T-221): исходы запросов, сбои по классам (рост `type="journal"`
  = потеря строк журнала `web_checks`, `type="non_ip"` = прокси не
  передаёт адрес посетителя), длительность;
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
`monitor.alerts_interval` (по умолчанию 5 мин — T-206; реконсиляция
счётчиков идёт отдельно раз в `monitor.zone_stats_interval`, с T-199 —
30 мин): доля ошибок
DNS, переполнение
очередей, остановка прогресса, нехватка места на диске (процент и
абсолютный минимум с эскалацией, T-162), износ NVMe,
всплеск мусорных `content_hash` при очистке удалённых доменов (T-099),
OOM-убийства и крэш-рестарты юнитов crawler (T-145; юниты движков
рендера — `crawler-*.service` — тем же алертом), неразобранный
`q:dead` и память Redis (T-154), троттлинг MemoryHigh, замороженная
очередь и PSI CPU (T-162), рост глубины очереди фолбэка рендера,
превышение бюджета доли фолбэка и рост browser-ошибок этапа рендера
(T-246/S-P2-6, пороги — §4.4), домены «статус повис» `new ∧ done`
старше суток (T-256, ТЗ-fsm ФТ-2.2 — потеря задач `q:http`). Сигнал фронтовой
(на переходах): **Warn в лог/journald monitor** и, при включённой
доставке, **сообщение Telegram** (ниже) —

```bash
journalctl -t crawler-monitor -p warning      # бой
make logs SERVICE=monitor | grep -i warn      # dev
```

пороги и обоснование — [`services.md`](services.md) (разделы об
алертах), место на диске — `content.disk_free_percent` и
`monitor.alerts.disk_free_*_bytes` ([`content.md`](content.md) «Лимиты
и переполнение диска», [`configuration.md`](configuration.md)), SMART —
[`disks.md`](disks.md) §6. Эффективные окна алертов —
[`services.md`](services.md) (цикл оценки алертов); T-206 вернул
прежнюю частоту алертов (5 мин) при 30-минутной реконсиляции.

**Доставка алертов в Telegram** (T-162): сработавшие и снятые алерты
приходят сообщениями в ту же выделенную группу, что и извещения CI/CD
(ТЗ §10.4) — «🚨 Алерт …» при срабатывании и «✅ Восстановлено …» при
снятии; дубли дебаунсятся (один эпизод = два сообщения), журнал monitor
не флудит. Включена по умолчанию (`monitor.alerts.telegram`), фактически
работает при двух условиях:

1. Секреты в `/etc/crawler/crawler.env` (EnvironmentFile юнита monitor,
   образец — `deploy/systemd/crawler.env.example`):

   ```
   TELEGRAM_BOT_TOKEN=<токен @BotFather>
   TELEGRAM_CHAT_ID=<id выделенной группы>
   ```

   **обязательны оба**: нет любого — доставка выключена (запись в журнале
   monitor при старте), алерты остаются Warn в journald — как шаг CI
   молчит без секретов. Значения живут в секретах репозитория GitHub
   (`TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` — те же, что у извещений
   CI/CD) и оттуда не читаются (write-only), поэтому доставка на сервер —
   либо ops-workflow **`ops-telegram-env`** (Actions → Run workflow:
   SSH-ключом деплоя дописывает/заменяет оба ключа в env-файле, сохраняет
   права root:crawler 640 и перезапускает monitor — скрипт
   `deploy/telegram-env.sh`), либо вручную редактором на сервере;
2. Скрипт отправки на хосте: `/opt/crawler/bin/telegram-notify.sh` —
   **едет в деплой-архиве вместе с бинарниками** (deploy.yml копирует
   `deploy/telegram-notify.sh` в архив, install распаковывает его в
   `/opt/crawler/bin` и chmod 0755): отдельной установки не требует и
   переживает деплой. Ручное копирование не нужно (историческая
   установка «один раз scp» стиралась первым же деплоем — install
   пересоздаёт каталог из архива).

   скрипту нужны `bash`, `curl` и `jq` на сервере (`jq` — список пакетов
   [`deploy.md`](deploy.md) §4.1; без него отправка падает exit 127);
   путь переопределяется `monitor.alerts.telegram_script`. Проверка без
   отправки: `TELEGRAM_BOT_TOKEN=x TELEGRAM_CHAT_ID=y /opt/crawler/bin/telegram-notify.sh --message "тест" --dry-run`;
3. Сеть до `api.telegram.org`: боевой сервер (российский хостинг) не
   проходит к API Telegram — TCP на `api.telegram.org:443` дропается
   (блокировка РКН; замер 24.10: github.com → 200, ya.ru → 302,
   telegram → таймаут по IPv4 и IPv6, DNS корректен). Симптом — Warn
   в журнале monitor каждые 10 минут: `curl: (28) Failed to connect
   to api.telegram.org … Timeout was reached (HTTP 000)`. Решение —
   HTTP-прокси на доступном извне VPS (внешний хост оператора
   `104.253.1.37`): `tinyproxy`, порт 3128, только `CONNECT 443`,
   `Allow` только IP боевого сервера; подключение — строкой
   `https_proxy=http://104.253.1.37:3128` в `/etc/crawler/crawler.env`
   (curl скрипта берёт переменную из окружения юнита автоматически).
   Установка/диагностика/откат прокси — журнал работ §5 (24.10) и
   `/root/README.crawler-telegram-proxy.txt` на самом VPS. Без прокси
   алерты и отчёты продолжают писаться в journald — краулер не
   страдает, тихо недоступна только доставка.

После правок `crawler.env` — `systemctl restart crawler-monitor`.

Той же доставкой отправляются ещё два типа сообщений monitor: отчёт
нагрузки 📊 (T-173, раз в `monitor.load_report_interval`) и извещения о
медленных запросах PostgreSQL 🐌 (T-177, только при
`monitor.slow_query_telegram` — по умолчанию выключена): операторы,
выполняемые дольше `monitor.slow_query_min_duration` (3 с), — по одному
сообщению на каждый новый (дебаунс pid+query_start), с меткой сервиса
и pid; без флага цикл ограничен Warn в журнале monitor и метриками,
полный
лог медленных операторов — журнал сервера PostgreSQL
(`log_min_duration_statement = 3s`, [db.md](db.md) §«Мониторинг»). Для
текстов чужих сессий нужен `GRANT pg_read_all_stats` роли `crawler`
([deploy.md](deploy.md) §4.1) — без гранта в сообщении пометка «текст
недоступен».

Разбор кластера блокировок 25.10 (конвой `domains`, T-192) — как читать
журнал медленных при похожем инциденте. Сигнатура конвоя: точечные
PK-операторы write-пути (запись результата, публикация бита, возврат
lease) с аномальными длительностями при идеальных планах (Index Scan по
`domains_default_pkey`, rows 1 — время уходит не в скан, а в ожидание
строчного лока) и одновременным освобождением группы жертв в одну
секунду — подпись одного держателя: как только его транзакция
закрывается, вся очередь проходит мгновенно. Кто держатель, журнал
медленных не называет — имя даёт `log_lock_waits = on` (T-192): при
ожидании дольше `deadlock_timeout` (1 с) сервер пишет в журнал
`process N still waiting for ShareLock on transaction X …` — pid,
транзакция и связанный оператор держателя. До T-192 параметр был off —
держатель ≈37 мин инцидента 25.10 не восстановим задним числом; класс
закрыт потолком `lock_timeout = 30 с` у write-пулов (dns-worker,
http-worker, scheduler — [db.md](db.md) §«AfterConnect-хуки»): конвой
обрезается откатом транзакции и повтором доставки (at-least-once)
вместо немого стояния конвейера.

Строка «🧮 Топ CPU» отчёта нагрузки — доля ядра за окно (1.0я = одно
ядро целиком): дельта тиков utime+stime имени к дельте одного ядра.
Имя, чьи процессы рестартовали за окно (сумма тиков имени упала —
25.10: postgres 18 → 16 процессов печатался «310613997334638.2я»,
андерфлоу беззнакового вычитания), пропускается на одно окно — честное
отсутствие вместо мусора (T-189); новое имя считается с полной суммой
тиков — они начаты внутри окна.

Строка «📬 Очереди» отчёта нагрузки (T-188) — бэклог групп потребления:
та же цифра, что `crawlerctl status` и метрика `crawler_queue_depth`
(общий расчёт `queue.GroupDepth` с гейтом backpressure), читается
`q:render 27529 (−1200, 2.0/с)` как «непрочитанный остаток + pending —
27 529, за окно дренаж 1 200, доставки идут 2/с»: дельта ±N — рост или
дренаж реальной очереди, темп — доставки (подтверждённая история
стрима до обрезки XTRIM на ней не сказывается — консистентность не
зависит от работы TrimLoop); стрим без групп и `q:dead` — инвентарный
XLEN. До T-188 строка печатала сырой XLEN — расхождение 25.10
(q:render ×5 при остановленном морозкой TrimLoop, фантомный рост
q:analyze при lag 0) — см. [redis.md](redis.md) §3.

**Мусорные `content_hash`** (`ingest_bad_hashes`): счётчик
`crawler_ingest_bad_hashes_total` сервиса ingest (удалённые очисткой
результаты с хэшем неверной длины) вырос на ≥ 10 за окно 15 проходов
алертов (≈75 мин на дефолтных 5 мин, T-206; на 30-мин цикле T-199
растягивалось до 7,5 ч) —
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

**Фолбэк рендера** (`render_fallback_depth` / `render_fallback_budget` /
`render_browser_errors`, T-246/S-P2-6): алерты наблюдаемости выбора
движка рендера ([`render.md`](render.md)).

- `render_fallback_depth` — глубина `q:render-fallback` (backlog группы
  воркера B; на дашборде — строка фолбэка панели «Движки рендера», в
  метриках — `crawler_queue_depth{queue="q:render-fallback"}` сборки B)
  достигла эффективного порога стрима **7 500** записей и (растёт 2
  прохода алертов подряд (10 мин) ИЛИ держится над порогом 2 прохода
  подряд без роста — стационарная глубина: остановленный B, упор в
  предел MAXLEN стрима 15 000, когда XADD вытесняет старейшие
  недоставленные записи и глубина перестаёт расти, или эпизод,
  переживший рестарт монитора; S-P2-1) — chromium-воркер B не успевает
  дорабатывать приток (темп `render.fallback.worker_rate` ниже потока
  публикаций, вплоть до остановленного B: глубину monitor читает из
  Redis напрямую, поэтому смерть воркера алертом видна). Порог — по
  образцу q:render из конфигурации:
  `queue.StreamWatermark(scheduler.queue_high_watermark, redis.maxlen,
  q:render-fallback)` — настроенные 100 тыс. клампятся половиной
  предела MAXLEN стрима (15 000 → 7 500; явный заниженный watermark
  даёт меньшие числа, как у рабочих стримов). Сработавший алерт
  держится до дренажа ниже 90 % порога (6 750).
- `render_fallback_budget` — счётчик событий приостановки
  `crawler_render_fallback_suspended_total{reason="budget"}` (воркер A)
  вырос: доля публикаций фолбэка за окно бюджета ушла за потолок
  `render.fallback.share_percent` — основной движок деградирует,
  fallback-рейт за экономическим потолком. Задачи не теряются
  (кандидаты финализируются штатно, ФТ-9). Событие держит алерт окном
  6 проходов (≈30 мин), снимается старением. Источник — зонд `/metrics`
  рендер-воркера (`monitor.render_metrics_url`); пустой адрес или
  недоступная экспозиция — «не измеряется» (Warn при ошибке зонда).
  Серия `suspended{reason="budget"}` предсоздана нулём с T-250 (S-P1-4):
  зонд читает 0 с первого старта воркера (и 0 для старой сборки без
  события — «серии нет» ≠ «не измеряется»), первое событие 0→1 алерт
  видит и не теряет, в том числе первым после рестарта воркера.
- `render_browser_errors` — счётчик ошибок класса браузера этапа
  `crawler_stage_errors_total{stage="render",type="browser"}` (тот же
  зонд `/metrics` рендер-воркера, S-P2-6) растёт 2 прохода алертов
  подряд (10 мин): движок-кандидат (moli/lightpanda) завис или
  недоступен — health-гейт уже приостановил фолбэк-публикации
  (`suspended{reason="gate"}` — счётчик и Warn-лог воркера, алерт —
  про первопричину), браузер-класс ждёт в redelivery `q:render` в
  пределах окна ретраев. Разовая browser-ошибка (штатный перезапуск
  сессии) и рестарт воркера (счётчик процессный) — тихо. Семейство
  ленивое: у воркера без browser-ошибок серии нет — алерт читает 0;
  экспозиция чужая (нет маркера `crawler_stage_tasks_total{stage=
  "render"}`) — «не измеряется» без Warn: проверьте адрес
  `monitor.render_metrics_url`.

Падение юнита движка рендера (`crawler-chromium.service` сегодня,
moli/lightpanda по ТЗ рендера §4.7 — юниты `crawler-*.service`)
отдельного алерта не имеет: OOM/крэш-рестарты ловит существующий
`unit_oom` (T-145), зависание/недоступность CDP движка — алерт
`render_browser_errors` (рост browser-ошибок этапа, S-P2-6) и
health-гейт фолбэка (`suspended{reason="gate"}`).

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
  устойчивой глубины, T-154: ≈15 мин на дефолтных 5 мин — T-206; темп смертей —
  `crawler_queue_dead_buried_total`, поток неидентифицируемых записей —
  `crawler_queue_dead_unidentified_total`, T-260) — разбрать:
  [`runbooks.md`](runbooks.md) §2.
- **Замороженная очередь** (алерт `queue_frozen`, T-162:
  last-delivered-id группы не движется ≥ 15 мин при глубине > 0 —
  потребитель не читает стрим, сигнатура яда T-159; неподвижность
  оценивается проходом цикла алертов — латентность ещё до периода:
  на дефолтных 5 мин, T-206, заморозка замечается через ≈15–20 мин):
  лог воркера
  стрима из детали (`journalctl -t crawler-http-worker` и т.п.),
  перезапуск воркера; записи, валящие потребителя, уходят в `q:dead`
  после рестартов — разбирать по runbooks §2.
- **Рендер-долг растёт** (алерт `render_debt`, T-175: флаги
  `render_pending` + лаг `q:render` выше 50 тыс. и растут ≥ 2 проходов
  алертов подряд — приток SPA обгоняет дренаж; сработавший алерт
  держится до дренажа ниже 45 тыс. — стагнация роста не будит, но видна
  строкой «Рендер-долг» отчёта нагрузки и gauge
  `crawler_render_pending_domains`): сверить темпы отчёта нагрузки
  (дренаж `q:render`, приток) и `render.contexts` — при исчерпании
  рендера поднять контексты/второй Chromium (deploy.md §2.2), временно
  заморозить приток планировщика (журнал работ §5); счётчик флагов ведут
  переходы бита — ручные операции с битом правят счётчик (runbooks §6).
- **Очередь фолбэка рендера растёт** (алерт `render_fallback_depth`,
  T-246: глубина `q:render-fallback` ≥ 7 500 и растёт ≥ 2 проходов
  алертов, ИЛИ держится ≥ 7 500 без роста 2 прохода — стационарная
  глубина: остановленный B или упор в предел MAXLEN (S-P2-1); снятие —
  дренаж ниже 6 750): сверить темп потребления
  `crawler_render_fallback_consumed_
  total` (воркер B жив? `systemctl status crawler-render-fallback-
  worker`, метрика — порт 9120) против потока публикаций
  `crawler_render_fallback_published_total`; темп потолком
  `render.fallback.worker_rate` (окно 1 мин) — поднять потолок или
  завести второй воркер B по образцу render-worker@ (deploy.md §2.2);
  растущий поток публикаций при здоровом B — сигнал деградации
  основного движка (смотреть `render_fallback_budget` и причины
  финализаций `crawler_render_task_finalized_total{reason}`);
  записи, потерянные обрезкой MAXLEN недоставленным, возвращает
  поздний переиздатель T-259 (http-worker, раз в минуту): его метрика
  `crawler_render_late_republish_total` после инцидента растёт
  (дренаж потерь) и спадает до нуля — зависшая ненулевая при пустой
  `q:render` — повод смотреть журнал http-worker (render.md
  §«Поздний переиздатель»).
- **Бюджет доли фолбэка превышен** (алерт `render_fallback_budget`,
  T-246: событие приостановки `crawler_render_fallback_suspended_total
  {reason="budget"}` — доля публикаций за окно бюджета ушла за потолок
  `render.fallback.share_percent`; снимается старением ≈30 мин):
  основной движок деградирует — fallback-рейт и успешность по движкам —
  панель «Движки рендера» дашборда и метрики этапа; решить: откат
  движка на chromium (ТЗ рендера §6.4, профиль воркера — правка
  `render.engine` + рестарт) или пересмотр потолка доли; задачи при
  приостановке не теряются (финализация штатная с маркером).
- **Browser-ошибки рендера растут** (алерт `render_browser_errors`,
  S-P2-6: `crawler_stage_errors_total{stage="render",type="browser"}`
  растёт ≥ 2 проходов алертов — движок-кандидат завис/недоступен;
  снимается остановкой роста): движок рендера чинить — статус
  health-гейта (`suspended{reason="gate"}` в `/metrics` рендер-воркера,
  Warn-логи), юнит движка (`systemctl status crawler-chromium.service`
  и т.п.); не чинится быстро — откат на chromium (ТЗ рендера §6.4:
  `render.engine` + `render.remote_url`, рестарт воркера A; воркер B
  останется на chromium — §2.3); задачи при живом гейте не теряются
  (redelivery q:render, после исчерпания — штатная финализация).
- **Домены «статус повис»** (алерт `domains_new_done_stale`, T-256:
  строка `crawler_counters['new_done_stale']` ненулевая в 2 НОВЫХ
  снапшотах реконсиляции подряд — серия растёт только при смене
  `updated_at` строки, повторные чтения одного снапшота (алерт ходит
  каждые 5 мин, источник — раз в 30) наблюдением не считаются, ≈1 ч
  устойчивого значения; домены `status = new ∧ check_state = done` со
  `last_checked_at` старше суток; уровень — gauge
  `crawler_domains_new_done_stale`): DNS-этап записал «есть адреса», а
  HTTP-итог не пришёл — потерянные задачи `q:http` (спецификация
  статусов — `internal/model/enums.go`). Когорту лечит компенсатор
  T-270 (ранний recheck: часовой проход планировщика возвращает
  new∧done старше 24 ч в `pending` сам, счётчик
  `crawler_scheduler_stale_requeued_total`, runbooks.md §2.10) —
  действий обычно не требуется; алерт остаётся сигналом о НОВЫХ
  потерях: сверить причину (рестарт Redis — параллельный алерт
  `redis_restart`; OOM-окно — журнал http-worker) и следить, что
  компенсатор дренирует когорту (счётчик растёт, gauge падает до нуля
  в пределах часа после реконсиляции). Не дренирует — разбор по
  runbooks.md §2.10.
- **Троттлинг MemoryHigh** (алерт `memory_high`, T-162: счётчик
  memory.events high юнита растёт ≥ 2 проходов подряд — устойчивый
  reclaim под мягким потолком, предвестник фаталов T-157): проверить
  `systemctl status crawler-http-worker` (Memory: у потолка
  MemoryHigh=850M) и лог юнита; повторяющийся троттлинг — повод
  пересмотреть бюджет ([`deploy.md`](deploy.md) §2), метрика —
  `crawler_unit_memory_high_events`.
- **Голод CPU** (алерт `psi_cpu`, T-162: PSI some avg300 > 90 %
  устойчиво 30 мин — тонут все этапы разом): `cat /proc/pressure/cpu`,
  топ потребителей (`ps aux --sort=-%cpu`), сверить с конкурентами на
  сервере (агрегаты postgres, компактификация); метрика —
  `crawler_psi_cpu_some_percent`.
- **Том контента близок к порогу** (`content.disk_free_percent`, 5%;
  абсолютные минимумы T-162 — `monitor.alerts.disk_free_min_bytes`
  15 GiB Warn / `disk_free_crit_bytes` 10 GiB эскалация) —
  запись контента встала на паузу (алерты disk_free/disk_free_min):
  освободить место (компактификация идёт сама), расширить том
  ([`disks.md`](disks.md)).
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
  `-p MemoryHigh`, T-145). Суточный прогон без OOM — критерий ТЗ 11.4
  п. 7 (T-093, **выполнен** 24.10: `journalctl -k --since "2026-10-23
  09:00" --grep oom` пуст — окно в `--since` обязательно, без него
  видны записи инцидентов до окна; пики в бюджетах MemoryMax —
  [`load-measurements.md`](load-measurements.md) §13); текущий
  контроль: `journalctl -k --grep oom` пуст,
  рестарты — по `NRestarts` (бьёт алерт unit_oom, T-145).
- **Память Redis** — `crawler_redis_memory_ratio` (алерт `redis_memory`
  при ≥ 80 % `maxmemory`, T-154; при noeviction — предупреждение
  отказов записи): сверить `redis-cli INFO memory`, разбор —
  [`redis.md`](redis.md).
- **Рестарт Redis** — алерт `redis_restart` (T-270: uptime из
  `INFO server` < 15 мин): недоставленные записи стримов потеряны —
  q:dns вернёт lease, потерянные q:http при записанном done —
  компенсатор T-270 (`new_done_stale` дренируется в пределах часа,
  runbooks.md §2.10); сверить причину перезапуска (юнит/OOM/хост),
  разбор — [`redis.md`](redis.md).
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

Таймер ресайкла chromium (`crawler-chromium-recycle.timer`, T-208) входит
в набор (deploy.md §2.2, T-215): `PartOf=crawler.target` в обоих юнитах
пары гасит и сервис, и таймер вместе с набором — очередной тик (раз в
2 ч) не поднимет остановленный chromium; `WantedBy=crawler.target`
возвращает таймер при старте набора. Отдельного гашения при остановке
набора не требуется. Точечная остановка одного `crawler-chromium.service`
таймер не гасит — ближайший тик (≤2 ч) вернёт браузер; при работающем
наборе это штатно.

Перезапуск любого сервиса безопасен: недоработанные задачи вернутся
повторной доставкой, домены — lease-проходом (ТЗ 11.4 п. 9).

### 4.6. Спокойное окно: остановка scheduler для дренажа retention

Временная остановка фронта (`crawler-scheduler`), чтобы тяжёлые циклы
http-worker — retention контента, компактификация паков, поздний
переиздатель рендера — доработали бэклог без конкуренции с полным
фронтом за CPU/IO/память PG. Прецеденты: 2026-10-24 (дренаж q:render),
2026-11-10…11 (дренаж retention после хотфикса T-273; журнал — §5).

**Критерии запуска** (достаточно одного):

- retention стоит: растёт `crawler_content_retention_errors_total`
  (серия WARN «проход retention контента» с err=), `freed_bytes_total`
  не растёт, диск/контент растут при стоящей очистке;
- красная зона системы при полном фронте: swap > ~3 ГиБ, серийные
  OOM-киллы PG, available < ~1.2 ГиБ (фронт не критично потерять на
  часы: доля суточной выработки мала против риска потери диска).

**Важно перед остановкой:**

- retention/компактификация живут в http-worker — остановка scheduler
  их НЕ останавливает (морозка 10-24 к притоку из БД тоже применима
  лишь частично — см. журнал: бит `render_pending` публикует сам
  http-worker);
- хвост очередей дорабатывается ещё ~2–3 ч (q:dns пачки × 1000
  доменов): load спадает не мгновенно, полное спокойствие — после
  доработки хвоста; дренаж суток бэклога — окно 12–24 ч (11.11:
  первый проход ~60 мин впритык + ночной дренаж ~13 ч);
- **деплой поднимет scheduler сам** (install стартует crawler.target;
  прецеденты 10-24 13:07 и 10-11 14:58), а **ребут сервера — тем более**
  (все enabled-юниты стартуют на загрузке; прецедент 11-11 11:03 —
  внешний power-cycle виртуалки оборвал морозку) — окно, которое должно
  пережить деплой или ребут, возобновлять остановкой повторно.

```bash
systemctl stop crawler-scheduler   # юнит остаётся enabled
```

**Метрики успеха во время окна:**

- `crawler_content_retention_errors_total` не растёт, WARN-серия
  прекратилась; в `pg_stat_activity` живой цикл пачек (SELECT по
  subdomain_results → commit);
- `contents` n_live_tup падает (`pg_stat_user_tables`), старейший пак
  молодеет, `du /var/lib/crawler/content` уменьшается; ежечасный INFO
  «проход компактификации паков» с freed_bytes > 0 — диск вниз;
- по завершении catch-up-прохода: `retention_pass_seconds_count` > 0,
  `rows/contents/freed_bytes_total` — пачковый прирост. **Нули
  счётчиков при живом длинном проходе — норма**: инкремент по итогу
  прохода (T-273), прогресс смотреть по contents/пакам/компактификации;
- после освобождения гейта render-late: `crawler_render_late_errors_total`
  не растёт.

**Возврат фронта:**

```bash
systemctl start crawler-scheduler
```

Предпочтительно после `pass_seconds_count` > 0 (проход завершён) и/или
при нормальном диске и устойчивом прогрессе (курсор дренажа персистентен
в процессе http-worker — таймаут отдельной пачки не обнуляет дренаж,
T-273). Контроль 1–2 ч после запуска: load вернётся к 20+ (норма
рендера — [`load-measurements.md`](load-measurements.md)), OOM-киллов
быть не должно, retention-проходы — штатные минуты, диск не растёт.

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

### 2026-10-24 — доставка Telegram через прокси (разблокировка T-162/T-173)

- После деплоя T-173 отчёты нагрузки генерировались каждые 10 минут,
  но доставка молчала: `curl: (28) Failed to connect to
  api.telegram.org:443 — Timeout` (HTTP 000) на каждой отправке.
  Упавший ops-ран `ops-telegram-env` (06:58, код 3 «строка включения
  не найдена») — ложная тревога: он стартовал до деплоя фикса
  `61b3484` (скрипта ещё не было в `/opt/crawler/bin`), секреты при
  этом записаны корректно; после деплоя 07:17 доставка включилась
  сама и упёрлась в сеть.
- Диагноз: с боевого IP `api.telegram.org` недоступен (дроп TCP),
  остальной интернет работает (github → 200, ya.ru → 302); CI-извещения
  приходили, т.к. их шлёт раннер GitHub — это маскировало проблему.
- Решение: на внешнем VPS оператора `104.253.1.37` (vpn, Ubuntu 24.04)
  установлен `tinyproxy 1.11.1` — порт 3128 (свободен: Amnezia-стек
  слушает 53/443/500/4500/47386/34116/41289/35826, mtproto-proxy —
  8443), только `CONNECT 443`, `Allow` только 155.212.174.88 и
  localhost, `enable --now`. Конфиг `/etc/tinyproxy/tinyproxy.conf`,
  памятка — `/root/README.crawler-telegram-proxy.txt` на VPS.
- На проде: `https_proxy=http://104.253.1.37:3128` дописан в
  `/etc/crawler/crawler.env` (переживает деплои и ops-раны — те
  меняют только `TELEGRAM_*` ключи), `systemctl restart
  crawler-monitor`. Проверка 09:57 MSK: CONNECT в логе прокси,
  ошибок отправки нет, отчёты ходят в группу.
- Откат: убрать `https_proxy` из `crawler.env` + restart monitor;
  на VPS — `systemctl disable --now tinyproxy && apt purge tinyproxy`.

### 2026-10-24 — морозка притока: остановка scheduler для дренажа q:render

- Причина: бэклог q:render ~119 тыс. записей (118 681 на момент
  остановки) — приток сравнялся с дренажём, лаг у watermark 100К
  не таял (паттерн config.go T-142). Решение — заморозить приток,
  дать очереди рассосаться (прецедент морозки 2026-10-22).
- Действие 10:26:37 MSK: `systemctl stop crawler-scheduler` — чистый
  graceful shutdown (текущая пачка доработана, пул БД закрыт,
  exit 0). Юнит остался enabled (ребут вернёт — сознательно);
  конвейер (dns/http/render/analyzer/monitor) не трогали.
- Контроль через 2 ч: q:dns (сток 122 пачки у watermark 125) и
  q:http дренированы до нуля (lag 0, pending 0), докатившаяся волна
  переработана http-этапом; живой остаток q:render ≈100 тыс.
  (lag 94 765 + pending 5 222) при нулевом притоке — чистый
  монотонный дренаж ~29–34 записи/с (T-092), ETA ~1 ч; q:analyze
  поспевает за рендером (lag 0).
- Нюанс остановленного scheduler (все циклы — в одном сервисе,
  cmd/scheduler/main.go): TrimLoop не подрезает подтверждённую
  историю — XLEN стримов растёт (q:http 41 тыс., q:analyze 81 тыс.
  на момент проверки), потолки MAXLEN T-025 держат бюджет; LeaseWatch
  стоит — зависшие queued вернёт первый lease-проход после старта.
  На паузе XLEN — не показатель глубины, смотреть лаги групп
  (`XINFO GROUPS` / `crawler_queue_depth` воркеров).
- Возврат: `systemctl start crawler-scheduler` после q:render → 0 —
  EnqueueLoop возьмёт накопленный pending (гейт T-026 ограничит
  q:dns 125 пачками), TrimLoop подрежет накопленную историю за проход.
- Продолжение (13:0x MSK): деплой-ран `40083436999` (12:48 MSK,
  success) перезапустил весь набор — healthcheck требует все 10 юнитов
  active и поднял остановленный scheduler (морозку не различает).
  Повторная остановка 13:07 MSK. Попутно замерен фактический темп
  дренажа: `crawler_stage_tasks_total{stage="render"}` ≈ **0.7 задачи/с**
  (не 29–34/с из T-092 §4) — голова q:render оказалась волной злых
  .рф-доменов (рендер-тарпиты 15с×3 попытки, ERR_CERT_*,
  ERR_CONNECTION_REFUSED — записи финализируются без рендера, но
  выжигают десятки секунд на 4 контекстах). При таком составе ~100 тыс.
  остатка — не часы, а ~1.5–2 суток дренажа; оценка по средней стр/с
  к бэклогу однородно-плохого состава неприменима. Урок: перед морозкой
  оценивать состав головы очереди (лог WARN рендера), а не только
  глубину.
- Эксперимент на проде без деплоя (правки `/etc/crawler/config.yaml`,
  бэкапы `config.yaml.bak-20261024-{contexts8,toa2}` рядом; деплой
  конфиги сервера не трогает — ci-cd.md, но возник дрейф с репо):
  14:22 `render.contexts: 8→4` (4 ядра, load 16.5 — CPU-голод),
  ~14:29 `render.timeout_attempts: 2→1` (тарпит финализируется с
  первого таймаута, без 10-мин claim-цикла T-148); после каждой правки
  `systemctl restart crawler-render-worker`. Замеры по эпохам
  (`crawler_stage_tasks_total` за 2–5 мин): 8ctx+toa2 — ~63–66
  зад/мин, load 16.5; 4ctx+toa2 — 65/мин, load 12.6 (темп не потерян,
  CPU полегчал); 4ctx+toa1 — 52/мин, load 14.8 — счётчик считает
  доставки, падение это исчезновение повторов, не деградация
  (pending 5.1k→5.0k дренажется). Решение: обе правки оставлены.
- **Корень найден (главный вывод дня): дренаж q:render невозможен —
  приток идёт не из очередей.** При пустых q:dns/q:http
  `entries-added` q:render рос на ~100/мин: http-worker каждую минуту
  выполняет «проход доработки рендера» (renderq.go, T-066) — публикует
  в q:render строки с битом `render_pending` из БД, минуя scheduler.
  Морозка scheduler этот путь не останавливает. В БД ждёт
  **394 833 домена** с битом (xn--p1ai 23 912, ru 18 359, baby 11 842,
  wiki 9 394, org.cn 7 207, …) — при ~100 зап/мин это 4–6 суток
  непрерывного притока. Лаг ~95–100 тыс. — равновесие притока (~100/мин)
  с ёмкостью рендера (~55–65/мин) у watermark 100К: очередь не
  «зависла», это буфер давления из БД (паттерн тот же, что в
  config.go комментарий T-142 «лаг не таял с 22.10»).
- Уточнение к предыдущему пункту: 29–34 из T-092 §4 — это зад/**мин**,
  не /с (опечатка выше); рендер никогда не был быстрым. «~40 дом/с» —
  темп dns/http-этапов (T-139: 47.6 дом/с); рендерская ёмкость —
  29–66 зад/мин во всех замерах. Отставание копилось в БД неделями и
  вскрылось теперь проходом доработки.
- Что дальше: морозка scheduler на q:render не влияет — снимать можно
  в любой момент (верх конвейера чист); реальный фикс ёмкости — второй
  Chromium + второй render-воркер на отдельном железе (deploy.md §2.2),
  на 4 ядрах потолок достигнут. Кандидат в кодовые задачи: cert-хосты
  получают бит `render_pending` заново каждым суточным recheck-эпизодом
  (T-159 сознательно «не навсегда») — для сотен тысяч злых хостов это
  вечный генератор рендер-работы, просится cooldown безнадёжных классов.

### 2026-10-24 — деплой T-177 (медленные запросы PG): конфиг, грант, проверка

- Деплой `103171b`–`e85bc61` прошёл штатно (ci 11м47с → deploy 5м51с,
  healthz зелёные, извещение Telegram доставлено — прокси работает).
- PostgreSQL: `/etc/postgresql/16/main/crawler-postgresql.conf` заменён
  версией из репо (перед заменой сверен md5 со старой репочной —
  серверных правок не было; бэкап `.bak-t177` рядом), `systemctl
  reload` — `SHOW log_min_duration_statement` = 3s без рестарта;
  `GRANT pg_read_all_stats TO crawler` — тексты чужих сессий в
  pg_stat_activity открыты.
- Проверка (12:50–12:55 MSK): цикл monitor поднялся («цикл медленных
  запросов» interval=10s порог=3s), gauge `crawler_pg_slow_queries` и
  счётчик `{app}` в /metrics; 11 событий за 5 минут, 0 ошибок
  Telegram-доставки. Реальные находки сразу: LATERAL-скан
  RequeueRenderPending (~10 с, scheduler), агрегаты dashboard/crawlerctl
  status (5–12 с), пачки http-worker — рендер-долг этапа 25 виден в
  Telegram без ручного SQL (цель T-175 частично закрыта сигналом).
- Наблюдение оператору: на текущем голоде CPU (PSI ~80–90 %) штатные
  агрегаты регулярно превышают 3 с — темп сообщений ~2/мин. Если
  тонет — поднять `monitor.slow_query_min_duration` (например, до
  10–15 с; коридор до 1h) или выключить цикл интервалом 0; полный
  след остаётся в журнале сервера независимо.
- Откат: вернуть `.bak-t177` + reload; `REVOKE pg_read_all_stats FROM
  crawler`; бинарники — деплой предыдущей версии (bin.prev).

### 2026-10-25 — конвой блокировок domains (T-192): разбор, конфиг в репо, нужен reload

- Инцидент 10:22–12:44 UTC: транзакция-держатель строчных локов `domains`
  ~37 мин (волна 2 — пять жертв на одной строке `(560, 25398110)`,
  освобождение всей очереди в одну секунду), волна 1 — держатель
  разрозненных строк ≈16 мин; точечные write-операторы стояли до 39 мин
  (записи №2–№4/№6/№11 журнала медленных), конвейер замер ≈55 мин
  (q:dns у watermark). Операторских сессий не было; имя держателя
  пост-фактум не восстановимо — `log_lock_waits = off`. Полный разбор —
  TZ T-192 (`tasks/TZ_slow_lock_convoy.md`), как читать журнал — §4.3.
- Конфиг: `deploy/postgres/postgresql.conf` и dev-конфиг в репо получили
  `log_lock_waits = on` (reload-параметр). **Осталось оператору:**
  заменить `/etc/postgresql/16/main/crawler-postgresql.conf` версией из
  репо (по образцу записи 24.10: бэкап-копия рядом, сверка md5), затем
  `systemctl reload postgresql` — рестарт не нужен; проверить
  `sudo -u postgres psql -c 'SHOW log_lock_waits'` → on. При следующем
  конвое журнал пишет «process N still waiting for ShareLock on
  transaction X …» — держатель по имени (pid, транзакция).
- Код (очередной деплой): write-пулы dns-worker/http-worker/scheduler
  получают AfterConnect-хук `db.LockTimeout(30s)` — конвой обрезается
  SQLSTATE 55P03 с откатом транзакции и повтором доставки вместо
  получасового ожидания; контроль — журнал медленных T-177 (ожидания
  вне держателя не выше 30 с).

### 2026-10-25 — операционная часть T-192 (log_lock_waits) и деплой волны T-191…T-190

- Операционная часть T-192 применена до деплоя (по образцу записи 24.10):
  серверный md5 `/etc/postgresql/16/main/crawler-postgresql.conf` сверен с
  репо-версией до T-192 (совпал — серверных правок не было), бэкап
  `.bak-t192` рядом; после замены md5 = репо-HEAD, `systemctl reload
  postgresql` (без рестарта): `SHOW log_lock_waits` = **on**,
  `log_min_duration_statement` = 3s (T-177) не тронут.
- Деплой волны `e9e3c4d..3bffc62` (T-191 retry-SEQ+guard, T-192
  lock_timeout-хуки, T-193 тик переанализа на время-курсоре, T-190
  тесты -race + CI-шаг) прошёл штатно: ci 13м14с (включая новый шаг
  «Тесты с детектором гонок») → deploy 1м53с, `crawler_build_info`
  revision=3bffc62, 11 юнитов active (включая render-worker@9109),
  healthz 9100–9106 зелёные, NRestarts=0 у всех.
- Контроль после (первые минуты): retry-цикл T-191 поднялся штатно,
  ошибок guard-строки `status_temp_error` нет; первый тик переанализа
  T-193 — selected=3 published=3 (свежий срез по курсору вместо
  полного скана); error/fatal в dns/http воркерах — 0.
- Наблюдение (сутки): записи `retrySelectSQL` исчезают из журнала
  медленных (T-191); при повторении конвоя журнал пишет «process N
  still waiting for ShareLock on transaction X …» (держатель по
  имени), ожидания точечных операторов вне держателя ≤ 30 с
  (обрезание 55P03 — откат транзакции, повтор доставки);
  `reanalyzeSelectSQL`-тик уходит из журнала, дренаж при смене правил
  остаётся (осознанно полный, T-193).
- Откат: конфиг — вернуть `.bak-t192` + reload; бинарники — деплой
  предыдущей версии (bin.prev) штатным конвейером.

### 2026-10-26 — разбор окна медленных запросов после 3bffc62; реконсиляция 30 м, cron NRD 05:33 UTC (T-199/T-205, операционная часть)

- Разбор окна 18:16 UTC 25.10 → 03:05 UTC 26.10 (лог
  `tmp/pg_slow_after_3bffc62.log`, вне git): 1053 оператора / 25 видов /
  ≈4,9 ч времени БД. Конвой блокировок не повторился (459 ожиданий, все
  ≤1,1 с, все acquired — T-192 работает); фиксы T-191/T-193
  подтвердились (retrySelect 150 → 10 событий, reanalyze-тик 18–25 →
  5–12 с, renderRequeue 137 → 1). По итогам поставлен этап 31
  (T-199…T-205, tasks/tasks.md); ТЗ разбора lease и retention —
  `tasks/TZ_slow_lease_return.md`, `tasks/TZ_slow_retention_residual.md`.
- T-199 (операционно, до деплоя дефолтов): `/etc/crawler/config.yaml`
  `monitor.zone_stats_interval: 5m → 30m` (бэкап
  `config.yaml.bak-20261026-zonestats30m` рядом), `systemctl restart
  crawler-monitor` — журнал: «цикл реконсиляции счётчиков конвейера
  interval=30m0s», healthz ok. Решение оператора по ТЗИ §7.1
  (разредить реконсиляцию после приработки волны). Следствие: цикл
  алертов monitor теперь 30 мин (свежесть алертов/дашборда зон;
  порог остановки прогресса AlertStallFactor × интервал = 2,5 ч).
- T-205 (операционно): `/etc/cron.d/crawler-nrd` 23:33 → 08:33 MSK
  (05:33 UTC — вне вечернего голода CPU и полуночного шторма
  перепроверки; день источника закрыт >9 ч). Нюанс: бэкап-копия в
  `/etc/cron.d` подхватилась systemd-cron как второй cron-джоб
  (`cron-crawler-nrdbak-…-0.timer` на 23:33) — убрана в
  `/root/crawler-nrd.bak-20261026`; урок: бэкапы `/etc/cron.d` хранить
  вне каталога. Проверка: один таймер, следующий прогон 08:33 MSK
  26.10 (день 2026-10-25 — непрерывность суточного фида сохранена).
- Откат: конфиг — вернуть `5m` + restart monitor; cron — вернуть
  строку `33 23 * * *` и комментарий (копия в /root).

### 2026-11-02 — T-215: пара recycle-юнитов chromium введена в crawler.target (PartOf/WantedBy)

- На боевом сервере в `crawler-chromium-recycle.{service,timer}` добавлен
  `PartOf=crawler.target`, таймер переведён с `WantedBy=timers.target`
  на `WantedBy=crawler.target` (`reenable`: симлинк переехал из
  `timers.target.wants/` в `crawler.target.wants/`), `daemon-reload`;
  юниты синхронизированы в комплект `deploy/systemd/` (deploy.md §2.2).
  Причина: после `systemctl stop crawler.target` очередной тик таймера
  (раз в 2 ч) запускал остановленный chromium. Бэкап прежних юнитов —
  `/root/t215-backup-20261102/`.
- Живой стоп-тест набора не проводился: идёт дренаж `q:render`
  (150 тыс. записей), scheduler намеренно остановлен с 30.10 —
  `start crawler.target` поднял бы его поверх дренажа. Связки проверены
  статически (`systemctl show -p PartOf,WantedBy`,
  `list-dependencies --reverse`: оба юнита под crawler.target) и
  семантика PartOf→timer подтверждена пробными юнитами в
  `/run/systemd/system` (stop цели гасит таймер, start возвращает;
  systemd 255). Провести короткий живой тест (stop → таймер и chromium
  inactive → start → всё вернулось) после завершения дренажа `q:render`
  и возврата scheduler в работу.
- Откат: `cp /root/t215-backup-20261102/{crawler-chromium-recycle.service,crawler-chromium-recycle.timer} /etc/systemd/system/`,
  `daemon-reload`, `reenable --now` таймера (вернётся в timers.target.wants/;
  `--now` обязателен: без него остановленный таймер `start crawler.target`
  не поднимет — WantedBy после отката снова timers.target, а он активен
  с загрузки), контроль — `systemctl is-active crawler-chromium-recycle.timer`.

### 2026-11-10…11 — регрессия T-271 (петля retention), хотфикс T-273 и «спокойное окно» дренажа

- **Инцидент**: с деплоя T-271 (09.11 14:13) оба тяжёлых селекта
  http-worker — retention и поздний переиздатель — в вечной петле
  обречённого catch-up: клиентский таймаут (600/60 с) убивал первый
  проход с нулевого курсора ДО коммита, курсор не двигался, каждый
  повтор шёл тем же сканом через чистый префикс. Retention мёртв 21 ч
  (70+ неудачных проходов), 21 cgroup-OOM-килл PG за ночь 09–10.11.
  Расследование (прод-отчёты 2026-11-09/10, задачи T-271/T-273):
  корень — таймаут меньше стоимости первого прохода + закрепление
  планов T-271 развернуло join render-late от domains.
- **Хотфикс T-273** (SelectTimeout 600→3600 с; render-late без
  закреплений + 300 с; reset тикера на каждую ошибку; метрики
  `*_errors_total`; тесты catch-up/backoff): деплой 10.11 14:58,
  healthcheck зелёный.
- **Подводный камень**: деплой поднял остановленный в 08:00 scheduler
  (install стартует crawler.target) — фронт и первый catch-up-скан
  совместились; под полной нагрузкой часа не хватило — проходы 1–3
  упёрлись в 3600 с (`retention_errors_total` = 3), курсор остался
  нулевым.
- **Действие 16:29:23 MSK**: `systemctl stop crawler-scheduler` — окно
  дренажа (хвост фронта дорабатывался до ~19:29). Проход 4 (18:43):
  первая выборка уложилась впритык (~60 мин на спокойной системе),
  далее дренаж пачками по 10 тыс. всю ночь без единой ошибки.
- **Итог к 08:04 11.11**: диск 74→53 % (свободно 36 Г), контент
  21→5.3 Г (паков 40→12, старейший молодел на сутки), contents live
  1.25 млн→262 тыс. (−~1 млн), компактификация freeing 1.5–2 Г/ч
  (~13 Г за ночь), киллов PG 0, WARN 0 за 12+ ч. Цена: фронт стоял
  ~15.5 ч (~0.5 млн проверок ≈ 1.2 % остатка цикла).
- **Уроки**: (а) оценка «30–60 мин» первого прохода — нижняя граница;
  под нагрузкой/с недоработанным хвостом — вдвое-втрое больше: потолок
  3600 с впритык (follow-up — 7200 с конфиг-ключом или частичный
  индекс + персистентный курсор); (б) счётчики retention агрегируются
  по итогу прохода — нули при живом дренаже норма, прогресс смотреть
  по contents/пакам/компактификации; (в) любой деплой поднимает
  scheduler — окно, которое должно пережить деплой, возобновлять
  вручную; (г) при живом (ненулевом) курсоре таймаут пачки дренаж не
  обнуляет — фронт можно возвращать до полного завершения прохода.
- Runbook операции — §4.6; снимки — `prod-reports/2026-11-09-204415.md`
  … `prod-reports/2026-11-10-210400.md`.

### 2026-11-11 — внешний power-cycle виртуалки в 11:03 (без оператора и деплоя)

- Диагностика по журналам: в 11:03:17 гость начал чистую остановку всех
  target'ов (multi-user, crawler, таймеры) → 11:03:23
  `poweroff.target`/`systemd-poweroff` → загрузка 11:03:44 (uptime -s).
  Инициатора внутри гостя нет: ни systemctl-команд, ни sudo, ни SSH-сессий;
  unattended-upgrades `Automatic-Reboot` закомментирован (его
  «Unattended Upgrades Shutdown»-юнит — следствие остановки, не причина);
  деплоя не было (бинари от 10.11 14:57, deploy-воркфлоу не запускался).
  Вывод: гипервизорный рестарт — панель/API хостинга или инфраструктура
  провайдера (fvds.ru).
- Последствия для конвейера: все юнити стартовали на загрузке — включая
  остановленный 10.11 16:29 scheduler (морозка дренажа завершилась
  естественно за 19 мин ДО ребута — повезло с таймингом); http-worker
  начал retention catch-up с нулевого курсора под разгоняющимся фронтом
  (риск «потолок 3600 с впритык» — T-274); PG/Redis перезапущены,
  process-метрики и pg_stat_statements обнулены.
- Уроки: (а) ребут сервера эквивалентен деплою по эффекту «поднял всё и
  оборвал любые окна» — учитывать в runbook §4.6; (б) при расследовании
  рестартов набора проверять `uptime -s`/`who -b` и хвост `journalctl
  -b -1` (poweroff vs reboot vs crash) до выдвижения гипотез; (в)
  внешние power-cycle не контролируем — ещё один аргумент за
  персистентный курсор дренажа (T-274).

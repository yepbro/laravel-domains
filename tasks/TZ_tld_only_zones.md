# Техническое задание: зоны только верхнего уровня (TLD-only)

Версия: 1.0 (дополнение к основному ТЗ [`TZ_domain_crawler.md`](./TZ_domain_crawler.md);
**заменяет семантику ТЗ 5.2 п. 5** «зона — публичный суффикс по списку
publicsuffix». Основное ТЗ не редактируется — управляет настоящее
дополнение, по прецеденту [`TZ_incremental_counters.md`](./TZ_incremental_counters.md)).

---

## 1. Цель и общее описание

Зона домена в системе — **последняя метка имени (TLD)**, а не публичный
суффикс по Mozilla PSL. `example.5g.in` → зона `in`, label `example.5g`.
Существующие зоны-публичные-суффиксы (`5g.in`, `co.uk`, `net.in`, …)
сворачиваются в свои TLD переносом данных; новые не возникают.

### 1.1. Сегодняшнее состояние (что заменяем)

Зону определяет `internal/idn` (`idn.go`, `Normalize`) вызовом
`publicsuffix.PublicSuffix` из `golang.org/x/net/publicsuffix` — встроенный
срез Mozilla PSL. PSL — список для изоляции cookies, куда **реестры сами
добавляют вторые уровни**: только у `.in` (реестр NIXI) их ~44 — `5g.in`,
`6g.in`, `ai.in`, `co.in`, `net.in`, `org.in`, …

Последствия:

| # | Проблема | Где |
|---|---|---|
| 1 | **Полный дамп `.in` через `incoming/in/` невозможен**: у `example.5g.in` суффикс `5g.in` ≠ зона пути `in`, `domainLabel` отвечает `wrong_zone` на каждой строке, файл падает по `ingest.max_error_ratio`. Дамп приходится вручную раскладывать по каталогам `incoming/5g.in/`, `incoming/co.in/`, … | `internal/ingest/validate.go`, `domainLabel` |
| 2 | Каждая 2LD-зона — строка `zones` и потенциально свой набор партиций (4 таблицы × `hash_parts`): тысячи микрозон-«мусора», которых нет в срезе IANA | `zones`, `create_zone_partitions` |
| 3 | Набор зон дрейфует с обновлением зависимости: PSL вшит в `golang.org/x/net` (сейчас v0.61.0), реестры добавляют суффиксы — новые «зоны» появляются вне контроля оператора | `go.mod` |
| 4 | Справочник `zones` расходится с срезом IANA: `crawlerctl zones sync` (T-125) грузит только TLD, `zones --deactivate-missing` не осмыслен, пока в таблице 2LD | `cmd/crawlerctl/zonessync.go` |

### 1.2. Целевое состояние

1. `idn.Normalize`: зона — **последняя метка** ASCII-формы; label — всё
   до неё (составной label — норма, контракт уже допускал составные метки).
2. Справочник `zones` = TLD (+ исключения-тестовые зоны из allowlist,
   §5). `zones sync` — канонический источник справочника.
3. Каталоги ingest: `incoming/<tld>/add|delete/`. Многоуровневые каталоги —
   только из allowlist `ingest.allowed_zone_paths` (тестовые зоны testbed).
4. Данные существующих 2LD-зон переносятся консольной командой
   **`crawlerctl zones collapse`** (§7) в окне обслуживания по runbook §8.
5. Зависимость от `golang.org/x/net/publicsuffix` удалена (`idna` остаётся).

### 1.3. Ключевые решения (обоснования — §12)

| Решение | Суть |
|---|---|
| Голые публичные суффиксы в строках (`co.in` в дампе `.in`) | **Принимаются как мусор** — становятся доменами (`in`, `co`). Фильтрация требует списка суффиксов — возврат PSL, от которого уходим (§12.6) |
| Селективное схлопывание («`*.in` свернуть, `co.uk` оставить») | Отклонено: две семантики зоны, вечный конфиг-патч за реестрами (§12.3) |
| PSL только ICANN-секция | Не решает задачу: `5g.in`/`co.in` — ICANN-секция (§12.2) |
| Перенос данных — CLI, а не файл миграции | Миграции при старте сервисов — только схема, «чистый SQL без кода» (стиль проекта); тяжёлый перенос строк — CLI с батчами, прецедент `zones sync` (§12.4) |

---

## 2. Термины

| Термин | Значение |
|---|---|
| ТЗ | Основное ТЗ проекта (`TZ_domain_crawler.md`), если не указано иное |
| ТЗТ | Настоящее ТЗ |
| TLD | Последняя метка доменного имени (срез IANA `tlds-alpha-by-domain.txt`) |
| 2LD-зона | Зона в `zones` из нескольких меток, совпадающая с публичным суффиксом PSL (`5g.in`, `co.uk`) — та, что сворачивается |
| Префикс зоны | 2LD-зона без TLD-хвоста: у `5g.in` — `5g`, у `city.kawasaki.jp` — `city.kawasaki` |
| Сворачивание (collapse) | Перенос строк 2LD-зоны в TLD-зону: `zone_id` → TLD, `label` → `label || '.' || префикс` |
| Allowlist каталогов зон | `ingest.allowed_zone_paths` — многоуровневые имена зон, которым разрешено быть каталогами `incoming/<зона>/` (тестовые зоны testbed) |

---

## 3. Ограничения и рамки

### 3.1. Рамки (что не входит)

1. **Фильтрация голых суффиксов не вводится** (решение §1.3): `co.in`
   в дампе становится доменом `(in, co)` и живёт общей судьбой (проверки,
   статусы). Объём — единицы строк на дамп.
2. **Гранулярность 2LD не сохраняется**: алерты `zone_stats`
   (доля ошибок DNS по зоне), веса `zone_weights`, приоритеты
   `requeue_deprioritized_zones` становятся per-TLD. Сломанный реестр
   `co.in` размажется по зоне `in`.
3. Схема БД не меняется: партиционирование LIST по `zone_id`, ключи
   `(zone_id, …)`, `UNIQUE (zone_id, label)` — как есть. Новых
   SQL-миграций нет.
4. Семантика `zones enable|disable`, `zones sync`, идемпотентность
   ingest (T-097), дедуп файлов (T-113) — без изменений.
5. Down-перенос (TLD → 2LD) не проектируется: миграция необратима
   (§12.5), откат — восстановление из бэкапа (§8.5).

### 3.2. Технические ограничения

1. Новых зависимостей нет; `go.mod` не меняется: из `golang.org/x/net`
   уходит использование только `publicsuffix`, `idna` остаётся.
2. Перенос данных выполняет CLI-команда `crawlerctl zones collapse`
   (батчи, свой темп), а не авто-миграция при старте сервисов
   («миграции — чистый SQL», ТЗИ инкрементальных счётчиков §3.2).
3. PostgreSQL 16: перенос строк между партициями — штатный row movement
   через `UPDATE` ключа партиционирования.
4. Окно обслуживания обязательно: во время переноса сервисы остановлены
   (§8) — иначе старый код пересоздаёт 2LD-зоны, новый пишет дубли до
   переноса, сообщения очередей несут устаревший `zone_id`.

---

## 4. Контракт нормализации `internal/idn`

### 4.1. Правило отделения зоны

`Normalize` после `normalizeASCII` (пп. 1–4 конвейера — без изменений):

```
i     := LastIndexByte(ascii, '.')
zone  := ascii[i+1:]        // последняя метка — TLD
label := ascii[:i]          // всё до неё; может быть составной
```

- `i < 0` (точек нет, имя само TLD) → `ErrNoLabel` — как сегодня для `com`.
- `isNumericZone(zone)` → `ErrNumericZone` — как сегодня: последняя метка
  из одних цифр отсекает IPv4-литералы (`1.2.3.4` → зона `4`) и даты
  выгрузок (`18.06.1999` → зона `1999`). Функция не меняется
  (цикл по меткам одной метки — тот же результат).
- `zone == ""` недостижим (normalizeASCII гарантирует непустой ASCII
  без пустых меток) — существующая страховочная ветка сохраняется.
- Вызов `publicsuffix.PublicSuffix` и импорт `golang.org/x/net/publicsuffix`
  удаляются.

`NormalizeZone` (имя зоны каталога) и `HostToASCII` — без изменений.

### 4.2. Примеры (до → после)

| Вход | Сегодня (PSL) | После (TLD-only) |
|---|---|---|
| `example.com` | зона `com`, label `example` | без изменений |
| `example.5g.in` | зона `5g.in`, label `example` | зона `in`, label `example.5g` |
| `example.co.uk` | зона `co.uk`, label `example` | зона `uk`, label `example.co` |
| `example.city.kawasaki.jp` | зона `kawasaki.jp` | зона `jp`, label `example.city.kawasaki` |
| `example.github.io` | зона `github.io` (частный суффикс) | зона `io`, label `example.github` |
| `магазин.рф` | зона `xn--p1ai`, label `xn--80adddb…` | без изменений |
| `example.crawl.test` | зона `test`, label `example.crawl` (правило `*`) | без изменений (теперь всегда) |
| `github.io` (голый суффикс) | `ErrNoLabel` | **валиден**: зона `io`, label `github` |
| `co.in` (голый суффикс) | `ErrNoLabel` | **валиден**: зона `in`, label `co` (решение §12.6) |
| `com` | `ErrNoLabel` | `ErrNoLabel` (без изменений) |
| `1.2.3.4`, `18.06.1999` | `ErrNumericZone` | `ErrNumericZone` (без изменений) |

### 4.3. Изменения в пакете

| Место | Изменение |
|---|---|
| `Normalize` | формула §4.1 вместо `publicsuffix.PublicSuffix` |
| докстринг пакета (строки 12–13) | «отделение зоны по последней метке (TLD), п. 5 в редакции ТЗТ» |
| `Result.Zone` (докстринг) | «TLD — последняя метка; составные зоны PSL не выделяются» |
| `Result.Label` (докстринг) | «составной label — норма для зон с 2LD-регистрацией» |
| `ErrNumericZone`, `ErrNoLabel` (докстринг) | формулировки без отсылки к PSL-правилу `*` |
| импорт `golang.org/x/net/publicsuffix` | удалить |

`IsIDN` семантики не меняется: `HasXNLabel(label)` по составному label
даёт тот же результат, что раньше по метке домена (любая A-метка слева
от зоны даёт бит).

---

## 5. Ingest: каталоги зон и валидация

### 5.1. Каталоги зон

- `incoming/<tld>/add|delete/` — рабочий путь; полный дамп TLD
  (`incoming/in/` со строками `example.5g.in`) принимается целиком.
- Зона-less файлы (`add|delete/<файл>`) — механика без изменений:
  зона строки выводится из имени (`idn.Result.Zone`), теперь это TLD.
- **Многоуровневый каталог** (`incoming/<зона с точкой>/`) — только если
  зона входит в `ingest.allowed_zone_paths` (§6). Иначе `NewValidator`
  возвращает ошибку уровня файла, watcher переносит файл в `failed/`
  с причиной (семантика T-021), строки не разбираются.
- Назначение allowlist — тестовые зоны testbed (`crawl.test`), которым
  ТLD-семантика запрещает быть «зоной пути». Продуктовый конфиг — пуст.

### 5.2. `domainLabel`

Логика структуры не меняется, меняются входные данные (`res.Zone` — TLD):

1. `res.Zone == zone` (путь — TLD имени) → label `res.Label`
   (составной — норма): `example.5g.in` в `incoming/in/` → `example.5g`.
2. `res.ASCII == zone` → `ReasonNoLabel` (само имя — зона пути;
   для TLD-путей раньше ловит `idn.Normalize`, ветка — страховка).
3. Имя оканчивается на `.zone` **и** `zone` оканчивается на `.`+TLD
   (путь глубже TLD — allowlisted тестовая зона) → label без хвоста:
   `example.crawl.test` в `incoming/crawl.test/` → label `example`.
   Иначе — `ReasonWrongZone`.

Ветка 3 для allowlisted зон — та же, что сегодня обслуживала не-PSL
зоны путей: код `domainLabel` не меняется, проверка allowlist стоит
раньше (в `NewValidator`).

### 5.3. Ошибки

Новые причины строк не вводятся. Изменяется покрытие `wrong_zone`:
- `example.co.uk` в `incoming/uk/`: сегодня reject → после accept.
- `example.5g.in` в `incoming/in/`: сегодня reject → после accept.

---

## 6. Конфигурация

### 6.1. Новое поле

```yaml
ingest:
  # Многоуровневые зоны, которым разрешено быть каталогами
  # incoming/<зона>/ (тестовые зоны; пример — crawl.test в testbed).
  # Продуктовое значение — пусто: рабочие каталоги только TLD.
  allowed_zone_paths: []
```

- `Ingest.AllowedZonePaths []string` (`yaml:"allowed_zone_paths"`),
  дефолт `nil` (пусто).
- Валидация: каждая запись — валидное имя зоны по `idn.NormalizeZone`,
  нижний регистр punycode, **минимум одна точка** (TLD-зоне в allowlist
  делать нечего). Нормализация и дедуп — при загрузке конфига
  (стиль `ZoneWeights`, config.go `Validate`).
- Сообщение об ошибке валидации указывает пример (`crawl.test`).

### 6.2. Сопутствующие правки текстов

- `config.go`, валидация `scheduler.zone_weights` (строка ~1109) и
  `render.requeue_deprioritized_zones` (~1302): убрать из текстов
  «составные — co.uk» — ключи теперь TLD (`in`, `uk`, `xn--p1ai`).
  Сам regex `zoneNameRe` не меняется (allowlist-имена с точками
  валидируются отдельно).
- `config.example.yaml`: комментарий к `zone_weights` — имена TLD;
  новый блок §6.1.

---

## 7. CLI: `crawlerctl zones collapse` (миграция данных)

### 7.1. Семантика

```
crawlerctl zones collapse              # dry-run: инвентаризация и план, ничего не меняет
crawlerctl zones collapse --apply      # выполнение переноса
crawlerctl zones collapse --apply --skip-reconcile   # без реконсиляции счётчиков в конце
crawlerctl zones collapse --force      # игнорировать предупреждение о check_state = 1
```

- Команда группы `zones` (прецедент — `zones sync`, T-125), DSN из
  конфигурации, allowlist — из того же конфига (его зоны **не
  сворачиваются** — `crawl.test` живёт как зона).
- **Идемпотентна и возобновляема**: повторный запуск продолжает
  с места остановки — переносятся только строки, ещё лежащие в 2LD-зоне
  (батч §7.3 адресует `zone_id = <2LD>`; уже перенесённые строки под
  предикат не попадают). Отсутствие 2LD-зон — no-op.
- Предусловие: в затрагиваемых зонах нет доменов `check_state = 1`
  (в очереди). Есть — отказ с перечнем зон (или `--force` после разбора
  оператором, §8.2).

### 7.2. Алгоритм (по каждой 2LD-зоне Z → TLD T)

Инвентаризация (список для dry-run и цикла):

```sql
SELECT z.id, z.name, z.domains_total,
       (SELECT count(*) FROM domains d WHERE d.zone_id = z.id) AS domains_now
FROM zones z
WHERE z.name LIKE '%.%'
  AND z.name <> ALL($1)      -- allowlist из конфига
ORDER BY z.name;
```

Для каждой зоны `Z` (id `zid`, name `zname`): `T` = последняя метка
`zname` (Go: `LastIndexByte`), `prefix` = `zname` без `.{T}`
(`5g.in` → `T=in`, `prefix=5g`; `city.kawasaki.jp` → `T=jp`,
`prefix=city.kawasaki`).

**Шаг 1 — зона T.** Найти `zones` по имени; нет — создать (unicode-форма
для XN-зон — `idn.ToUnicode`, ASCII — NULL; `is_active = true`, стиль
`zones sync`):

```sql
SELECT id FROM zones WHERE name = $t;
INSERT INTO zones (name, name_unicode, is_active)
VALUES ($t, NULLIF($unicode, ''), true) RETURNING id;
```

**Шаг 2 — партиции T** (до переноса, чтобы строки ложились сразу в
выделенные партиции; идемпотентно — существующие пропускаются NOTICE):

```sql
CALL create_zone_partitions($tid);
```

**Шаг 3 — множество переносимых id** (temp-таблица на сессию команды):

```sql
CREATE TEMP TABLE moving_ids AS SELECT id FROM domains WHERE zone_id = $zid;
CREATE INDEX ON moving_ids (id);
```

**Шаг 4 — перенос батчами** (§7.3): по каждой таблице — `UPDATE` с
row movement (PG16 переносит строки в партиции T автоматически):

```sql
UPDATE domains
SET zone_id = $tid, label = label || '.' || $prefix
WHERE zone_id = $zid AND id >= $lo AND id < $hi;

UPDATE subdomain_results SET zone_id = $tid
WHERE zone_id = $zid AND domain_id >= $lo AND domain_id < $hi;

UPDATE laravel_domains   SET zone_id = $tid
WHERE zone_id = $zid AND domain_id >= $lo AND domain_id < $hi;

UPDATE laravel_findings  SET zone_id = $tid
WHERE zone_id = $zid AND domain_id >= $lo AND domain_id < $hi;
```

`laravel_history` (без партиций, объём на порядки меньше) — одним
оператором на зону:

```sql
UPDATE laravel_history SET zone_id = $tid WHERE zone_id = $zid;
```

**Шаг 5 — дедуп-журнал `ingest_files`** (перепривязка к T сохраняет
дедуп точной повторной доставки, T-113; конфликт ключа
`(zone_id, action, sha256)` возможен, если то же содержимое уже
доставлялось в T — дубль удаляется, остаётся строка-владелец):

```sql
DELETE FROM ingest_files a
USING ingest_files b
WHERE a.zone_id = $zid AND b.zone_id = $tid
  AND a.action IS NOT DISTINCT FROM b.action
  AND a.sha256 = b.sha256;

UPDATE ingest_files SET zone_id = $tid WHERE zone_id = $zid;
```

**Шаг 6 — партиции Z дропнуть** (выборка из каталога по границе
`FOR VALUES IN (zid)` — тот же приём, что в `create_zone_partitions`;
`DROP TABLE` партиции-родителя зоны уносит её hash-листья):

```sql
SELECT p.relname AS parent, c.relname AS part
FROM pg_inherits i
JOIN pg_class c        ON c.oid = i.inhrelid
JOIN pg_class p        ON p.oid = i.inhparent
JOIN pg_namespace n    ON n.oid = p.relnamespace
WHERE n.nspname = current_schema()
  AND p.relname IN ('domains','subdomain_results','laravel_domains','laravel_findings')
  AND pg_get_expr(c.relpartbound, c.oid) IN (
      format('FOR VALUES IN (%s)', $zid), format('FOR VALUES IN (%L)', $zid::text));
-- DROP TABLE <part> по каждой строке результата
```

**Шаг 7 — строка зоны:**

```sql
DELETE FROM zones WHERE id = $zid;
```

**Шаг 8 — счётчики.** Колонки `zones` (`domains_total`, `domains_checked`,
`domains_pending`, `domains_queued`) не мержатся вручную: после всех зон
команда вызывает общую реконсиляцию счётчиков (тот же агрегат, что
`monitor` `ZoneStatsOnce` / `crawlerctl counters`, T-178) — она
авторитетно пересчитает и T, и глобальные кумулятивы. `--skip-reconcile`
оставляет это первому проходу monitor после старта.

`DROP TABLE moving_ids` — в конце команды.

### 7.3. Батчинг

- Go перебирает `moving_ids` по возрастанию `id` окнами по
  `batch = 50000` id (константа, не конфиг): `lo`/`hi` — границы окна.
- Каждый батч — **отдельная транзакция** (4 `UPDATE` шага 4 одного
  диапазона вместе: домены и их дети переезжают атомарно на батч).
- Нарушение `UNIQUE (zone_id, label)` в батче невозможно (доказательство
  §12.5); если аномальные данные всё же дают конфликт — транзакция батча
  откатывается, команда останавливается с ошибкой и именем зоны
  (естественная защита, отдельный pre-check не нужен).
- Между батчами — пауза-троттлинг не вводится (окно обслуживания,
  конкурентной записи нет); WAL/bloat закрываются `VACUUM (ANALYZE)`
  затронутых таблиц в конце (шаг post, §8.4).

### 7.4. Отчёт

Dry-run и `--apply` печатают таблицу (стиль `zones sync`):

```
зона        TLD   доменов  результатов  находок  история  файлов  партиции
5g.in       in       12345       456789    12345     1234      12  есть
co.in       in      456789      1234567   456789    45678      3  default
…           …           …           …        …        …       …  …
итого: 2LD-зон 44, доменов 1 234 567 → зона in
```

После `--apply` — сводка перенесённого по зонам и итог реконсиляции.

### 7.5. Пост-проверки (команда, всегда)

```sql
-- 2LD-зон вне allowlist не осталось
SELECT count(*) FROM zones WHERE name LIKE '%.%' AND name <> ALL($1);            -- 0

-- сиротских zone_id в доменных таблицах нет
SELECT count(*) FROM domains d
WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.id = d.zone_id);                 -- 0
-- (аналогично subdomain_results, laravel_domains, laravel_findings, laravel_history)

-- счётчики зон = агрегат (после реконсиляции)
```

Плюс спот-чек одного перенесённого имени: `domain_info('<домен>.<Z>)`
возвращает зону `T`, label с доклеенным префиксом.

---

## 8. Runbook окна обслуживания

Порядок обязателен: смешение старого/нового кода с наполовину
перенесёнными данными даёт дубли и пересоздание зон.

### 8.1. Подготовка

1. Выкатить сборку с волнами 1–4 (код + collapse) **без запуска**
   (образы собраны, стек остановлен — обновление не стартует сервисы
   до команды оператора).
2. Бэкап БД (`pg_dump` или снапшок тома) — единственный путь отката (§8.5).
3. Зафиксировать инвентаризацию: `crawlerctl zones collapse` (dry-run).

### 8.2. Тишина

4. Остановить источники работы: ingest (watcher) и scheduler — новые
   задачи не публикуются.
5. Дождаться опустошения стримов `q:dns`, `q:http`, `q:analyze`,
   `q:render` (воркеры добивают хвост; контроль — `crawlerctl status`
   / `redis-cli XLEN`). Сообщения несут `zone_id` (queue/message.go) —
   задачи удалённых зон, выполненные после переноса, записали бы
   результаты с несуществующим `zone_id` в DEFAULT-партиции.
6. Остановить воркеров, analyzer, monitor, render-worker, web.
   Экстренный вариант (хвост не дренируется): `DEL` стримов и перевод
   зависших задач в pending — `UPDATE domains SET check_state = 0,
   queued_at = NULL WHERE check_state = 1`; идемпотентность конвейера
   (T-097) гарантирует корректность повторной выборки.

### 8.3. Перенос

7. `crawlerctl zones collapse --apply` (предусловие §7.1 проверит
   `check_state = 1`; при `--force` из шага 6 — осознанно).
8. Пост-проверки §7.5, спот-чек `domain_info('example.5g.in')` → зона
   `in`, label `example.5g`.

### 8.4. Финал

9. `VACUUM (ANALYZE) domains, subdomain_results, laravel_domains,
   laravel_findings;` (bloat после row movement).
10. Стек со сборкой шага 1 — сервисы стартуют на новой схеме данных;
    monitor первой реконсиляцией подтверждает счётчики.
11. Почистить каталоги `var/incoming/<2LD>/` (если складывали дампы
    по старой схеме) — с allowlist §5.1 файл оттуда упадёт в `failed/`,
    но чистота лучше; при желании — `crawlerctl zones sync`.

### 8.5. Откат

Восстановление бэкапа шага 2 целиком. Частичный откат (вернуть одну
зону) невозможен: отличить перенесённый составной label от исконного
(`foo.bar.in` всегда жил в зоне `in` с label `foo.bar`) нельзя (§12.5).

---

## 9. Тесты

| Слой | Что проверяется |
|---|---|
| `internal/idn` юнит + корпус (T-010) | таблица §4.2 целиком: `co.uk`/`5g.in`/`github.io`/`kawasaki.jp` → TLD-зоны и составные label; голые `github.io`/`co.in` валидны; `com` — `ErrNoLabel`; `1.2.3.4`/даты — `ErrNumericZone`; `IsIDN` на составных label |
| `internal/ingest` | `incoming/in/` + `example.5g.in` → accept, label `example.5g` (бывший `wrong_zone`); мультилейбл-каталог без allowlist — ошибка файла; с allowlist `crawl.test` — accept по ветке 3 §5.2; `wrong_zone` на чужом TLD; зона-less файл со смешанными TLD |
| `internal/config` | `allowed_zone_paths`: пусто по умолчанию, валидные/невалидные записи (без точки, не-зона), дедуп; тексты ошибок `zone_weights`/`requeue_deprioritized_zones` |
| `crawlerctl zones collapse` интеграционный (`test/integration`) | на сидированной схеме: 2LD-зона с доменами/результатами/находками/history/ingest_files (+партиции и без) → `--apply` переносит всё, доклеивает label, дропает партиции, удаляет зону, перепривязывает ingest_files (включая конфликт дубля); повторный запуск — no-op; прерванный запуск (после N батчей) — продолжается; allowlist-зона не тронута; пост-проверки §7.5 зелёные |
| testbed | конфиг testbed с `allowed_zone_paths: [crawl.test]`; e2e-сценарии ingest→check в зоне `crawl.test` проходят |

Общие критерии: `make test`, `make lint`, `make test-integration` — зелёные.

---

## 10. Документация

| Файл | Изменение |
|---|---|
| `docs/schema.md` | §«Обработка имени» (`domain_info`): зона — TLD, «самый длинный суффикс из `zones`» остаётся верным (справочник — TLD + allowlist); пример `site.co.uk` → зона `uk` |
| `docs/incoming-files.md` | каталоги `incoming/<tld>/`, allowlist, запрет прочих многоуровневых каталогов |
| `docs/configuration.md` + `config.example.yaml` | `ingest.allowed_zone_paths`; комментарии `zone_weights`/`requeue_deprioritized_zones` — имена TLD |
| `docs/runbooks.md` | runbook §8 (окно сворачивания зон) |
| `tasks.md` | указатель добавлен: этап 30, T-194…T-198 |

---

## 11. Этапы реализации

Задачи заведены в [`tasks.md`](./tasks.md) этапом 30: T-194…T-198,
одна задача на волну.

| Волна | Задача | Содержимое | Критерий приёмки |
|---|---|---|---|
| 1 | T-194 | `internal/idn`: TLD-отделение, удаление `publicsuffix`, докстринги; корпус/юнит-тесты §9 | `make test` зелёный; таблица §4.2 зафиксирована тестами |
| 2 | T-195 | `internal/ingest` + `internal/config`: allowlist каталогов, тексты; тесты §9 | `make test` зелёный |
| 3 | T-196 | `crawlerctl zones collapse` + интеграционный тест §9 | `make test-integration` зелёный; dry-run на dev-стеке читаем |
| 4 | T-197 | Документация §10, testbed-конфиг | ревью доков; testbed-сценарии проходят |
| 5 | T-198 | Окно обслуживания по §8 на продуктивных данных | пост-проверки §7.5 зелёные; дамп `.in` принимается через `incoming/in/` одним файлом |

Волны 1–4 — один PR/выпуск (код в репозитории до окна, но сервисы
продолжают старую сборку до шага §8.1.1 — переключение атомарно).
Допустимо последовательное тестирование на dev-стеке с переингестом
тестовых данных вместо переноса.

---

## 12. Решения и альтернативы

### 12.1. PSL → последняя метка

PSL решает задачу изоляции cookies (кто может ставить cookie на `*.in`),
а не задачу «административной корзины» зоны в краулере. Свойства TLD-only:
зона детерминирована именем (никаких внешних списков), стабильна
(TLD-множество меняется единицами в год), совпадает со срезом IANA —
каноном `zones sync`. Цена — гранулярность 2LD (§3.1.2).

### 12.2. ICANN-секция PSL — не решение

`5g.in`, `co.in` и пр. находятся в ICANN-секции (поданы реестром NIXI);
фильтрация PRIVATE-секции убрала бы только `github.io`-подобные. Задачу
не решает, зависимость сохраняет.

### 12.3. Селективное схлопывание отклонено

«Свернуть `*.in`, оставить `co.uk`» требует конфига-перечня и его
сопровождения за реестрами; даёт две семантики зоны (статистика, веса,
ingest) и возвращается к исходной проблеме в урезанном виде. TLD-only —
одна простая инвариантная семантика.

### 12.4. Перенос данных — CLI, не миграция при старте

Миграции проекта — чистый SQL-схема, применяются автоматически на старте
всех сервисов (docs/migrations.md). Перенос миллионов строк — часы,
батчи, отчёт, dry-run, предусловия — это CLI-команда обслуживания
(прецедент `zones sync`, T-125), выполняемая в окне тишины; авто-миграция
такого объёма в одной неявной транзакции стартующего сервиса — риск
долгих транзакций и каскадных рестартов.

### 12.5. Необратимость и отсутствие коллизий

- **Коллизий `UNIQUE (zone_id, label)` нет**: пара `(T, label||префикс)`
  биективно соответствует полному имени, а при старом коде имя
  `example.5g.in` попадало **только** в зону `5g.in` — пары `(in,
  example.5g)` в БД существовать не могло. `domains.id` — глобальная
  последовательность (`domain_id_seq`), коллизий PK между зонами нет.
- **Обратный перенос невозможен**: перенесённый label `example.5g` неотличим
  от исконного составного `foo.bar` (домен `foo.bar.in` всегда жил в
  зоне `in`). Поэтому down-механизма нет, откат — бэкап (§8.5).

### 12.6. Голые суффиксы — мусор без фильтра

Отличить `co.in` (суффикс) от `nic.in` (домен) можно только списком
суффиксов — это возврат PSL/ручного перечня. Объём шума: ~44 строки на
дамп `.in` из миллионов; строки реально существуют в DNS, проходят
проверки, портят статистику на доли процента. Точечный список
`bare_suffixes` можно добавить позже, если когда-нибудь понадобится.

### 12.7. Составной label — не новинка

Контракт составных меток существовал с T-010 (не-PSL зоны:
`example.crawl.test` → label `example.crawl`; `магазин.рф.co.uk` в зоне
`co.uk`). TLD-only лишь делает составные label регулярным случаем;
ни схема, ни индексы, ни идемпотентность ingest не меняются.

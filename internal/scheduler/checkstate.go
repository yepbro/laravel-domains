// Автомат check_state домена (T-255, ТЗ tasks/TZ-fsm.md §3 ФТ-1): единая
// точка правды о состояниях и переходах колонки domains.check_state
// (smallint: 0 pending, 1 queued, 2 done — model.CheckState, ТЗ 6.1).
// Формализация не переписывает переходы на FSM-фреймворк: они остаются
// SQL-операторами с охранными предикатами (проверенная модель проекта,
// ТЗ-fsm §1); эта спецификация — таблица решений, с которой обязаны
// сверяться все писатели колонки (перечислены ниже). Писатели воркеров
// ссылаются на неё одной строкой у своих операторов; поведение смены
// status (ТЗ 6.2) — отдельный автомат (ТЗ-fsm §3 ФТ-2), здесь только
// побочные действия, связанные с переходами check_state.
//
// # Состояния
//
//	pending (0)  ждёт выборки диспетчером (частичный индекс
//	             domains_pending_idx); queued_at = NULL — аренды нет
//	             (инвариант И2)
//	queued (1)   взят в работу, действует аренда: воркер обязан записать
//	             результат за scheduler.lease (ТЗ 5.3); queued_at = момент
//	             постановки — инвариант T-028 «queued ⇒ queued_at NOT NULL»
//	             (И1); NULL-отметка — нарушенный инвариант, вне основного
//	             lease-прохода навсегда (сравнение по возрасту NULL
//	             отсеивает) — терминальная страховка ФТ-1.2 (leasenull.go)
//	done (2)     эпизод проверки завершён (DNS/HTTP, ТЗ 7); queued_at =
//	             NULL (И3); рендер и анализ дополняют результат
//	             асинхронно, состояния не меняя
//
//	status = deleted — НЕ состояние check_state: delete-файл (T-019)
//	меняет только статус, check_state остаётся каким был — удалённая
//	строка выпадает из всех проходов охранными предикатами
//	status <> deleted; возврат в цикл — только воскрешение (Р9).
//
// # Переходы: состояние × событие × охраны × цель × побочные действия
//
// Все охраны вычисляются на момент БЛОКИРОВКИ строки (FOR UPDATE либо
// охраняемый UPDATE) — гонка между выборкой и записью не переводит строку,
// чья судьба изменилась; RETURNING отдаёт только фактически переведённые
// строки (идемпотентность повторных доставок и проходов, at-least-once,
// ТЗ 3.4). Дельты счётчиков зон (T-178) ведут те же операторы переходов
// по старому состоянию строки; авторитет — реконсиляция monitor.
//
//	| № | Исходное | Событие | Охраны | Цель | Побочные действия | Писатель |
//	|---|----------|---------|--------|------|-------------------|----------|
//	| Р1 | pending | постановка планировщика | check_state = pending, status <> deleted | queued | queued_at = now() (сервер БД); снятие бита http_published (новый эпизод с чистой пометкой); зоны pending −n / queued +n; публикация q:dns по RETURNING (перевод ДО публикации: сбой публикации оставляет queued без задачи — вернёт lease Р5) | enqueue.go EnqueueBatch (T-028) |
//	| Р2 | queued | запись результата DNS-эпизода, терминальная ветка (not_delegated / delegated_no_address / temp_error) | status <> deleted, check_state <> done | done | queued_at = NULL (аренда завершена); last_checked_at = now(); статус-итог; temp_attempts = 0 (кроме temp_error — сохраняет); снятие http_published; зоны queued −n / checked +n; корзина retry_mature +1 при temp_error-итоге | dnsworker/result.go resultUpdateStatusSQL (T-036+) |
//	| Р3 | queued | запись результата DNS-эпизода, ветка «есть адреса» (итог за HTTP-этапом) | те же | done | queued_at = NULL; last_checked_at = now(); temp_attempts = 0; снятие http_published; зоны queued −n / checked +n | dnsworker/result.go resultUpdateKeepStatusSQL |
//	| Р4 | pending | запись результата DNS/HTTP-эпизода (lease вернул посреди эпизода, дубль-доставка) | те же (HTTP: status <> deleted БЕЗ охраны check_state — осознанная асимметрия: повторная доставка обязана переиздать q:analyze, фильтр done отбросил бы публикацию навсегда) | done | queued_at = NULL; last_checked_at = now(); снятие http_published у DNS-ветки (HTTP-ветка бит не трогает); зоны pending −n / checked +n (предчтение старого состояния FOR UPDATE, T-178) | dnsworker/result.go, httpworker/record.go recordDomainSQL (T-058) |
//	| Р5 | queued | истечение аренды (lease-возврат) | check_state = queued, status <> deleted, queued_at < now() − scheduler.lease (NULL отсеивается сравнением — возраста нет) | pending | queued_at = NULL; зоны queued −n / pending +n; перевыберет диспетчер — повторная доставка безопасна (ТЗ 3.4) | lease.go LeaseOnce (T-029) |
//	| Р6 | queued | терминальная страховка NULL-инварианта (нарушен И1: queued без queued_at — вечное зависание вне Р5) | check_state = queued, status <> deleted, queued_at IS NULL | pending | queued_at = NULL (уже NULL); зоны queued −n / pending +n; счётчик crawler_scheduler_lease_null_returned_total (ненулевой стационар — сигнал оператору, docs/runbooks.md §2.9) | leasenull.go LeaseNullOnce (T-255, ФТ-1.2) |
//	| Р7 | done | суточная возрастная перепроверка | check_state = done, status <> deleted, status <> invalid (ядовитые строки не циклируются, T-109), last_checked_at < now() − scheduler.recheck_interval | pending | queued_at = NULL; статус-итог сохраняется (перепишет новая проверка); зоны checked −n / pending +n | recheck.go RecheckOnce (T-030) |
//	| Р8 | done | повтор temp_error: попытки не исчерпаны | status = temp_error, check_state = done, last_checked_at старше temp_error_retry_after, temp_attempts < N | pending | temp_attempts + 1; зоны checked −n / pending +n; декремент корзин retry_mature (T-202/T-267); исчерпание попыток — переход СТАТУСА (delegated_no_address, temp_attempts → 0), check_state остаётся done | retry.go RetryOnce (T-031) |
//	| Р9 | done | воскрешение add-файлом | status = deleted | pending | status = new; queued_at = NULL; зоны total +n / pending +n (домен был вне базового множества счётчиков); flags/dns_mask/last_checked_at сохраняются | ingest/apply_add.go reviveAddSQL (T-018) |
//	| Р10 | — | вставка нового домена add-файлом | ON CONFLICT DO NOTHING (повторная партия — no-op) | pending | status = new (DEFAULT схемы); зоны total +n / pending +n | ingest/apply_add.go insertAddSQL (T-018) |
//	| Р11 | done, queued, pending | ручной recheck оператора | status <> deleted (найденные по имени; pending → pending — идемпотентный no-op; queued → pending равносилен ручному возврату lease) | pending | queued_at = NULL; зоны по старому состоянию: done → checked −n / pending +n, queued → queued −n / pending +n, pending → без дельт | cmd/crawlerctl/recheck.go recheckUpdateSQL (T-084) |
//	| Р12 | done | массовый recheck субдомена (справочник целиком или зона) | check_state = done, status <> deleted, (опционально zone_id) — порога возраста НЕТ (перепроверить надо все готовые) | pending | queued_at = NULL; зоны checked −n / pending +n | cmd/crawlerctl/manage.go subdomainRecheck*SQL (T-084) |
//	| Р13 | queued, pending | диагноз ядовитой строки (пустое имя — проверка невозможна детерминированно) | status <> deleted, check_state <> done | done (status = invalid) | queued_at = NULL; temp_attempts = 0; снятие http_published; зоны по старому состоянию; invalid не возвращается перепроверкой Р7 — выход один: ручной разбор | dnsworker/result.go resultInvalidSQL (T-109) |
//	| Р14 | любое | delete-файл | строка не deleted | check_state НЕ меняется (status = deleted) | удаляется из цикла охранными предикатами всех проходов; результаты чистит T-020; счётчики зон total −n и колонка старого состояния −n | ingest/apply_delete.go (T-019) |
//
// Не-переходы (наблюдения, check_state не меняют):
//
//   - фильтр актуальности пачки q:dns (dnsworker/result.go resultPendingSQL):
//     SELECT check_state <> done ∧ status <> deleted — отсев повторной
//     доставкой уже зафиксированных эпизодов;
//   - пометка http_published (dnsworker/result.go resultMarkPublishedSQL,
//     T-108): бит flags, не переход — спецификация бита отдельная (там же,
//     у resultMarkPublishedSQL; ТЗ-fsm §3 ФТ-3, T-257); снятие бита —
//     побочное действие Р1–Р4, Р13 (в Р4 — только DNS-ветка: операторы
//     dnsworker пишут done и из pending; HTTP-ветка recordDomainSQL бит
//     не трогает — сохраняет прочие биты, кроме dead_host_cutoff);
//   - диспетчер DispatchBatch (dispatch.go, T-027): читает pending,
//     состояние не меняет (переводит следующий шаг Р1).
//
// # Инварианты
//
//	И1  queued ⇒ queued_at IS NOT NULL (T-028). Нарушение — баг писателя
//	    или ручной SQL: NULL отсеивается сравнением lease-прохода Р5,
//	    строка зависает в queued навсегда. Терминальная страховка —
//	    ремонтный проход Р6 того же цикла LeaseWatch (ФТ-1.2, leasenull.go).
//	И2  pending ⇒ queued_at IS NULL: у pending нет аренды — каждый писатель
//	    перевода в pending сбрасывает отметку (Р5–Р12).
//	И3  done ⇒ queued_at IS NULL: запись результата завершает аренду (Р2–Р4,
//	    Р13); ненулевая отметка остаётся только у queued-строк.
//	И4  единственный переход в queued — постановка планировщика Р1: любой
//	    возврат строки в работу идёт через pending (диспетчер выбирает
//	    только pending) — свежий эпизод всегда проходит очистку бита
//	    http_published и ставит новую отметку аренды.
//	И5  status = deleted исключён охранными предикатами всех переходов,
//	    кроме Р9 (воскрешение) и Р14 (само удаление).
//	И6  переходы идемпотентны: охраны перепроверяют состояние на момент
//	    блокировки строки, счёты ведутся по RETURNING фактически
//	    переведённых (повторные доставки и проходы безопасны, ТЗ 3.4).

package scheduler

-- T-178: инкрементальные счётчики состояний конвейера (ТЗИ
-- tasks/TZ_incremental_counters.md): полные проходы 47 млн строк domains
-- уходят из путей чтения — счётчики поддерживаются дельтами в транзакциях
-- переходов, точность восстанавливает периодическая реконсиляция monitor
-- (совмещённый проход ZoneStatsOnce, ТЗИ §7).
--
-- 1) zones.domains_pending / domains_queued — живое распределение
--    состояний проверки по зонам (ТЗИ §4.1). Базовое множество всех
--    колонок зон унифицировано — status <> deleted (как у реконсиляции
--    T-078): удалённый домен не является ни работой, ни прогрессом.
--    Читатели: crawlerctl status / дашборд (без JOIN domains, ТЗИ §8.1),
--    BacklogLoop (sum по справочнику, ТЗИ §8.4), выборка диспетчера
--    (фильтр domains_pending > 0 — задача T-183). Пишут: дельты
--    переходов (scheduler/ingest/httpworker, ТЗИ §6) и реконсиляция.
--
-- 2) crawler_counters.updated_at — момент последней записи строки:
--    инкременты анализатора (кумулятивы истории, ТЗИ §6.5) и
--    реконсиляция пишут имя/значение/момент; возраст строк показывает
--    crawlerctl counters list (ТЗИ §9). Таблица создана миграцией 000015
--    (T-175, render_pending): новые строки (status_*, кумулятивы
--    appeared/lost) кладутся в неё же — «не плодят новых таблиц»
--    (решение T-175, подтверждено ТЗИ T-178: таблица pipeline_counters
--    не заводится, единая точка — crawler_counters).
--
-- Наполнение — первый проход реконсиляции при старте monitor (прецедент
-- 000012/domains_laravel, ТЗИ §5): между миграцией и проходом (секунды)
-- читатели видят нули; строки crawler_counters создаёт реконсиляция
-- upsert'ом. Отдельного backfill-SQL нет — дубль текста агрегата
-- миграция/код был бы источником расхождений.
--
-- Откат (см. down): (1) обязан сопровождаться откатом сервисов на
-- версии до T-178 — код переходов пишет дельты в отсутствующие колонки;
-- (2) DROP COLUMN трогает только новые колонки зон, crawler_counters не
-- удаляется (владелец — T-175): откат 000015 по-прежнему отдельный.

ALTER TABLE zones
    ADD COLUMN domains_pending bigint NOT NULL DEFAULT 0,
    ADD COLUMN domains_queued  bigint NOT NULL DEFAULT 0;

ALTER TABLE crawler_counters
    ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now();

COMMENT ON COLUMN zones.domains_pending IS 'Живые домены зоны в check_state = pending (status <> deleted): дельты переходов + реконсиляция monitor, T-178';
COMMENT ON COLUMN zones.domains_queued IS 'Живые домены зоны в check_state = queued (status <> deleted): дельты переходов + реконсиляция monitor, T-178';
COMMENT ON COLUMN crawler_counters.updated_at IS 'Момент последней записи строки счётчика (инкременты/реконсиляция, T-178)';

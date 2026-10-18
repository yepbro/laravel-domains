-- T-118: откат истории детекции appeared/lost.
--
-- Журнал append-only без ссылок со стороны рабочих таблиц: удаление не
-- теряет данных конвейера. IF EXISTS — повторный запуск по чистой схеме
-- не падает (полный откат golang-migrate после частичного).

DROP TABLE IF EXISTS laravel_history CASCADE;

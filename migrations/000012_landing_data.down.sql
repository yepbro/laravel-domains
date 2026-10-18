-- T-120: откат данных лендинга.
--
-- web_checks — лог без ссылок со стороны рабочих таблиц, удаляется
-- целиком; индекс и колонка зон снимаются. IF [NOT] EXISTS — повторный
-- запуск по чистой схеме не падает (полный откат после частичного).

DROP TABLE IF EXISTS web_checks CASCADE;
DROP INDEX IF EXISTS laravel_domains_updated_at_idx;
ALTER TABLE zones DROP COLUMN IF EXISTS domains_laravel;

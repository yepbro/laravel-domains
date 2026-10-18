-- T-113: откат к глобальному уникальному ключу ingest_files.sha256.
--
-- Восстанавливает ограничение уникальности 000001 (глобально по
-- содержимому) и удаляет служебный ingest_files_sha256_idx. Откат
-- требует, чтобы журнал не содержал строк с одинаковым sha256 в разных
-- (zone_id, action) — созданных контрактом T-113 (undo-строки delete,
-- повторные доставки в других зонах): при наличии таких ADD CONSTRAINT
-- упадёт, конфликтующие строки разрешаются вручную до отката.
-- Ограничение возвращается исходной формой 000001 (ADD CONSTRAINT …
-- UNIQUE), а не голым индексом: имя и уникальность те же, pg_dump-вид
-- схемы совпадает с досхемным.

DROP INDEX IF EXISTS ingest_files_sha256_idx;

DROP INDEX IF EXISTS ingest_files_zone_id_action_sha256_key;

ALTER TABLE ingest_files ADD CONSTRAINT ingest_files_sha256_key UNIQUE (sha256);

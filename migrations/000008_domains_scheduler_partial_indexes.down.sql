-- T-104: откат частичных индексов фоновых проходов планировщика.
--
-- DROP INDEX на партиционированном родителе каскадно удаляет дочерние
-- индексы всех партиций; данные не затрагиваются. Сами проходы
-- (lease/recheck) продолжат работать seq scan'ом — как до миграции.

DROP INDEX IF EXISTS domains_recheck_idx;
DROP INDEX IF EXISTS domains_lease_idx;

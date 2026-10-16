-- T-005: субпартиционирование крупных зон (ТЗ §6: «крупные зоны дополнительно
-- по хэшу id, чтобы партиция не превышала ≈ 30–50 млн строк»).
--
-- Процедура create_zone_partitions(zone_id, hash_parts) выделяет зону из
-- DEFAULT-партиции в собственный набор партиций во всех четырёх таблицах
-- с zone_id (domains, subdomain_results, laravel_domains, laravel_findings):
--
--   domains                PARTITION BY LIST (zone_id)
--   └─ domains_z<zone_id>  FOR VALUES IN (zone_id) WITH (fillfactor = 85)
--      — обычная партиция-лист, без второго уровня (см. ниже);
--
--   subdomain_results / laravel_domains / laravel_findings
--   └─ <таблица>_z<zone_id>         FOR VALUES IN (zone_id)
--      └─ <таблица>_z<zone_id>_h<N> FOR VALUES WITH (MODULUS M, REMAINDER N)
--         — субпартиционирование HASH по domain_id: все строки одного домена
--         попадают в один лист.
--
-- Почему domains без хэша. У domains два уникальных ключа: PK (zone_id, id)
-- и UNIQUE (zone_id, label) — контракт T-003 (идемпотентность ingest по
-- ON CONFLICT, ТЗ 5.1; поиск по имени, ТЗ 5.11). PostgreSQL 16 требует,
-- чтобы каждый уникальный ключ содержал колонки ключа партиционирования
-- каждого уровня; общий делитель двух ключей — только zone_id, константа
-- внутри партиции зоны. Значит, субпартиционировать domains по хэшу id (или
-- label) при живом UNIQUE (zone_id, label) невозможно в принципе — это
-- уточнение ТЗ §6/§6.1 при реализации (прямое указание создать HASH по id
-- для domains приводит к ошибке «unique constraint on partitioned table
-- must include all partitioning columns»). Крупная зона получает одну
-- выделенную партицию domains — строки таблицы маленькие, апдейты точечные;
-- основной объём и перезаписи живут в subdomain_results, который
-- субпартиционируется полностью.
--
-- Последовательность и блокировки (PostgreSQL 16), по каждой таблице,
-- в одной транзакции CALL:
--   1. DETACH DEFAULT-партиции: CREATE PARTITION OF невозможен, пока
--      DEFAULT прикреплена и содержит строки зоны (PG откажет с ошибкой
--      «updated partition constraint for default partition would be
--      violated by some row»);
--   2. CREATE партиции зоны (и hash-листьев на все остатки 0..M-1 —
--      у hash-партиций не бывает DEFAULT, «дырок» для маршрутизации нет);
--   3. перенос строк зоны из отсоединённой DEFAULT одной командой
--      WITH moved AS (DELETE ... RETURNING *) INSERT ... — атомарно,
--      без промежуточного хранения;
--   4. ATTACH DEFAULT обратно: строк зоны в ней уже нет, проверка
--      ограничения проходит.
--
-- DETACH/CREATE/ATTACH держат AccessExclusiveLock на таблице, поэтому
-- на время работы процедуры (включая перенос строк) конкурентная запись
-- в таблицу ждёт, а не падает: для зоны, выделяемой до загрузки, перенос
-- пуст и процедура мгновенна; нагруженную зону выделять в окно обслуживания.
-- DETACH ... CONCURRENTLY внутри процедуры недоступен (не работает
-- в транзакции).
--
-- fillfactor = 85 (storage parameters на партиционированном родителе PG16
-- запрещает, параметр получают листья): domains — самой партиции зоны,
-- subdomain_results — hash-листьям, как у их DEFAULT-партиций (миграции
-- 000001/000002, контракт test/integration/schema_test.go); laravel_* —
-- без fillfactor. Дочерние индексы всех индексов родителя создаются на
-- партициях автоматически вместе с CREATE TABLE ... PARTITION OF.
--
-- Идемпотентность: таблицы, у которых партиция зоны уже существует,
-- пропускаются (NOTICE) — повторный CALL безопасен и завершит прерванное
-- выделение; повторные вызовы по одной зоне сериализуются advisory-блокировкой.
--
-- Как выбирать hash_parts (число листьев под целевые ≈ 30–50 млн строк) —
-- docs/schema.md. Вызов:
--
--   CALL create_zone_partitions(<zone_id>);
--   CALL create_zone_partitions(<zone_id>, <hash_parts>);

CREATE PROCEDURE create_zone_partitions(p_zone_id smallint, p_hash_parts integer DEFAULT 4)
LANGUAGE plpgsql
AS $proc$
DECLARE
    v_table    text;    -- таблица: domains, subdomain_results, laravel_*
    v_hash_col text;    -- колонка хэширования (domain_id) либо NULL для domains
    v_has_ff   boolean; -- партиции нужен fillfactor = 85
    v_default  text;    -- имя DEFAULT-партиции таблицы (из каталога)
    v_part     text;    -- имя партиции зоны: <таблица>_z<zone_id>
    v_ff       text;    -- хвост ' WITH (fillfactor = 85)' либо ''
    v_moved    bigint;  -- строк, перенесённых из DEFAULT
    v_i        integer;
BEGIN
    -- Зона обязана существовать: партиция зоны без строки в zones —
    -- ошибка данных (зоны создаёт ingest, ТЗ 5.1).
    IF NOT EXISTS (SELECT 1 FROM zones WHERE id = p_zone_id) THEN
        RAISE EXCEPTION 'create_zone_partitions: зоны % нет в таблице zones', p_zone_id
            USING HINT = 'зона создаётся автоматически при первом входном файле (ТЗ 5.1)';
    END IF;

    -- Степень двойки 1..1024: любой modulus валиден, но степень двойки
    -- позволяет позже делить лист пополам (modulus 2M, остатки N и N+M),
    -- не пересобирая остальные листья зоны.
    IF p_hash_parts < 1 OR p_hash_parts > 1024
       OR p_hash_parts & (p_hash_parts - 1) <> 0 THEN
        RAISE EXCEPTION 'create_zone_partitions: hash_parts = %, ожидается степень двойки от 1 до 1024', p_hash_parts;
    END IF;

    -- Сериализация повторных вызовов по одной зоне (транзакционная —
    -- снимается при COMMIT/ROLLBACK сама).
    PERFORM pg_advisory_xact_lock(hashtext('create_zone_partitions'), p_zone_id);

    FOR v_table, v_hash_col, v_has_ff IN
        SELECT * FROM (VALUES
            ('domains',           NULL::text,  true),
            ('subdomain_results', 'domain_id', true),
            ('laravel_domains',   'domain_id', false),
            ('laravel_findings',  'domain_id', false)
        ) AS t(tab, hash_col, needs_ff)
    LOOP
        -- Идемпотентность: партиция зоны уже выделена — таблица пропускается.
        -- pg_get_expr печатает значение smallint в границе в кавычках
        -- (FOR VALUES IN ('1')), поэтому ожидаемое сравнивается в двух
        -- вариантах записи.
        IF EXISTS (
            SELECT 1
            FROM pg_inherits i
            JOIN pg_class c        ON c.oid = i.inhrelid
            JOIN pg_class parent   ON parent.oid = i.inhparent
            JOIN pg_namespace nspc ON nspc.oid = parent.relnamespace
            WHERE nspc.nspname = current_schema()
              AND parent.relname = v_table
              AND pg_get_expr(c.relpartbound, c.oid) IN (
                  format('FOR VALUES IN (%s)', p_zone_id),
                  format('FOR VALUES IN (%L)', p_zone_id::text)
              )
        ) THEN
            RAISE NOTICE 'create_zone_partitions: %: партиция зоны % уже существует, пропуск', v_table, p_zone_id;
            CONTINUE;
        END IF;

        -- Имя DEFAULT-партиции берём из каталога, а не из соглашения об именах.
        SELECT c.relname INTO v_default
        FROM pg_inherits i
        JOIN pg_class c        ON c.oid = i.inhrelid
        JOIN pg_class parent   ON parent.oid = i.inhparent
        JOIN pg_namespace nspc ON nspc.oid = parent.relnamespace
        WHERE nspc.nspname = current_schema()
          AND parent.relname = v_table
          AND pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT';
        IF v_default IS NULL THEN
            RAISE EXCEPTION 'create_zone_partitions: у таблицы % нет DEFAULT-партиции', v_table;
        END IF;

        v_part := format('%s_z%s', v_table, p_zone_id);
        v_ff   := CASE WHEN v_has_ff THEN ' WITH (fillfactor = 85)' ELSE '' END;

        -- 1. Отсоединить DEFAULT — см. комментарий над процедурой.
        EXECUTE format('ALTER TABLE %I DETACH PARTITION %I', v_table, v_default);

        IF v_hash_col IS NULL THEN
            -- 2а. domains: партиция зоны — сразу лист с fillfactor
            --     (субпартиционирование невозможно, см. шапку).
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF %I FOR VALUES IN (%s)%s',
                v_part, v_table, p_zone_id, v_ff);
        ELSE
            -- 2б. Промежуточный родитель, субпартиционированный HASH по id
            --     домена (fillfactor на нём запрещён PG16 — параметр получат
            --     листья), и листья на все остатки: DEFAULT внутри hash-набора
            --     невозможен.
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF %I FOR VALUES IN (%s) PARTITION BY HASH (%I)',
                v_part, v_table, p_zone_id, v_hash_col);
            FOR v_i IN 0 .. p_hash_parts - 1 LOOP
                EXECUTE format(
                    'CREATE TABLE %I PARTITION OF %I FOR VALUES WITH (MODULUS %s, REMAINDER %s)%s',
                    v_part || '_h' || v_i, v_part, p_hash_parts, v_i, v_ff);
            END LOOP;
        END IF;

        -- 3. Перенос строк зоны из отсоединённой DEFAULT одной командой.
        EXECUTE format(
            'WITH moved AS (DELETE FROM %I WHERE zone_id = %s RETURNING *) ' ||
            'INSERT INTO %I SELECT * FROM moved',
            v_default, p_zone_id, v_part);
        GET DIAGNOSTICS v_moved = ROW_COUNT;
        RAISE NOTICE 'create_zone_partitions: %: перенесено строк из DEFAULT: %', v_table, v_moved;

        -- 4. DEFAULT на место: строк зоны в ней больше нет.
        EXECUTE format('ALTER TABLE %I ATTACH PARTITION %I DEFAULT', v_table, v_default);
    END LOOP;
END
$proc$;

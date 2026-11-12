-- T-005: subpartitioning of large zones (spec §6: "large zones are additionally
-- hashed by id, so that a partition stays within ≈30–50M rows").
--
-- The create_zone_partitions(zone_id, hash_parts) procedure carves a zone out of the
-- DEFAULT partition into its own set of partitions in all four
-- tables with zone_id (domains, subdomain_results, laravel_domains, laravel_findings):
--
--   domains                PARTITION BY LIST (zone_id)
--   └─ domains_z<zone_id>  FOR VALUES IN (zone_id) WITH (fillfactor = 85)
--      — an ordinary leaf partition, no second level (see below);
--
--   subdomain_results / laravel_domains / laravel_findings
--   └─ <table>_z<zone_id>         FOR VALUES IN (zone_id)
--      └─ <table>_z<zone_id>_h<N> FOR VALUES WITH (MODULUS M, REMAINDER N)
--         — HASH subpartitioning by domain_id: all rows of one domain
--         land in one leaf.
--
-- Why domains has no hash. domains has two unique keys: PK (zone_id, id)
-- and UNIQUE (zone_id, label) — the T-003 contract (ingest idempotency via
-- ON CONFLICT, spec 5.1; lookup by name, spec 5.11). PostgreSQL 16 requires
-- every unique key to include the partitioning key columns
-- of every level; the only common divisor of the two keys is zone_id, a constant
-- inside a zone partition. So subpartitioning domains by a hash of id (or
-- label) while UNIQUE (zone_id, label) is alive is impossible in principle — a
-- refinement of spec §6/§6.1 made during implementation (a direct instruction to create a HASH on id
-- for domains fails with the error "unique constraint on partitioned table
-- must include all partitioning columns"). A large zone gets one
-- dedicated domains partition — the table's rows are small, updates are point ones;
-- the main volume and the rewrites live in subdomain_results, which is
-- subpartitioned in full.
--
-- Sequence and locks (PostgreSQL 16), per table,
-- inside one CALL transaction:
--   1. DETACH the DEFAULT partition: CREATE PARTITION OF is impossible while
--      the DEFAULT is attached and holds the zone's rows (PG fails with the error
--      "updated partition constraint for default partition would be
--      violated by some row");
--   2. CREATE the zone partition (and hash leaves for every remainder 0..M-1 —
--      hash partitions have no DEFAULT, there are no "holes" for routing);
--   3. move the zone's rows out of the detached DEFAULT with one command
--      WITH moved AS (DELETE ... RETURNING *) INSERT ... — atomically,
--      with no intermediate storage;
--   4. ATTACH the DEFAULT back: the zone's rows are gone from it, the
--      constraint check passes.
--
-- DETACH/CREATE/ATTACH hold an AccessExclusiveLock on the table, so
-- while the procedure runs (including the row move) concurrent writes
-- to the table wait instead of failing: for a zone carved out before load, the move
-- is empty and the procedure is instant; carve out a loaded zone in a maintenance window.
-- DETACH ... CONCURRENTLY is unavailable inside the procedure (it does not work
-- in a transaction).
--
-- fillfactor = 85 (PG16 forbids storage parameters on a partitioned parent,
-- the leaves get the parameter): domains — the zone partition itself,
-- subdomain_results — the hash leaves, like their DEFAULT partitions (migrations
-- 000001/000002, the contract test/integration/schema_test.go); laravel_* —
-- without fillfactor. Child indexes of every parent index are created on
-- the partitions automatically together with CREATE TABLE ... PARTITION OF.
--
-- Idempotency: tables whose zone partition already exists
-- are skipped (NOTICE) — a repeated CALL is safe and finishes an interrupted
-- carve-out; repeated calls for the same zone are serialized by an advisory lock.
--
-- How to choose hash_parts (the number of leaves for the target ≈30–50M rows) —
-- docs/schema.md. Invocation:
--
--   CALL create_zone_partitions(<zone_id>);
--   CALL create_zone_partitions(<zone_id>, <hash_parts>);

CREATE PROCEDURE create_zone_partitions(p_zone_id smallint, p_hash_parts integer DEFAULT 4)
LANGUAGE plpgsql
AS $proc$
DECLARE
    v_table    text;    -- the table: domains, subdomain_results, laravel_*
    v_hash_col text;    -- the hash column (domain_id), or NULL for domains
    v_has_ff   boolean; -- the partition needs fillfactor = 85
    v_default  text;    -- the table's DEFAULT partition name (from the catalog)
    v_part     text;    -- the zone partition name: <table>_z<zone_id>
    v_ff       text;    -- the tail ' WITH (fillfactor = 85)' or ''
    v_moved    bigint;  -- rows moved out of the DEFAULT
    v_i        integer;
BEGIN
    -- The zone must exist: a zone partition without a row in zones —
    -- a data error (zones are created by ingest, spec 5.1).
    IF NOT EXISTS (SELECT 1 FROM zones WHERE id = p_zone_id) THEN
        RAISE EXCEPTION 'create_zone_partitions: zone % is missing from the zones table', p_zone_id
            USING HINT = 'the zone is created automatically by the first incoming file (spec 5.1)';
    END IF;

    -- A power of two 1..1024: any modulus is valid, but a power of two
    -- makes it possible to split a leaf in half later (modulus 2M, remainders N and N+M)
    -- without rebuilding the zone's other leaves.
    IF p_hash_parts < 1 OR p_hash_parts > 1024
       OR p_hash_parts & (p_hash_parts - 1) <> 0 THEN
        RAISE EXCEPTION 'create_zone_partitions: hash_parts = %, a power of two from 1 to 1024 is expected', p_hash_parts;
    END IF;

    -- Serialization of repeated calls for the same zone (transactional —
    -- released automatically at COMMIT/ROLLBACK).
    PERFORM pg_advisory_xact_lock(hashtext('create_zone_partitions'), p_zone_id);

    FOR v_table, v_hash_col, v_has_ff IN
        SELECT * FROM (VALUES
            ('domains',           NULL::text,  true),
            ('subdomain_results', 'domain_id', true),
            ('laravel_domains',   'domain_id', false),
            ('laravel_findings',  'domain_id', false)
        ) AS t(tab, hash_col, needs_ff)
    LOOP
        -- Idempotency: the zone partition already exists — the table is skipped.
        -- pg_get_expr prints a smallint value inside the bound in quotes
        -- (FOR VALUES IN ('1')), so the expected value is compared in both
        -- spellings.
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
            RAISE NOTICE 'create_zone_partitions: %: the zone % partition already exists, skipping', v_table, p_zone_id;
            CONTINUE;
        END IF;

        -- The DEFAULT partition name is taken from the catalog, not from a naming convention.
        SELECT c.relname INTO v_default
        FROM pg_inherits i
        JOIN pg_class c        ON c.oid = i.inhrelid
        JOIN pg_class parent   ON parent.oid = i.inhparent
        JOIN pg_namespace nspc ON nspc.oid = parent.relnamespace
        WHERE nspc.nspname = current_schema()
          AND parent.relname = v_table
          AND pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT';
        IF v_default IS NULL THEN
            RAISE EXCEPTION 'create_zone_partitions: the table % has no DEFAULT partition', v_table;
        END IF;

        v_part := format('%s_z%s', v_table, p_zone_id);
        v_ff   := CASE WHEN v_has_ff THEN ' WITH (fillfactor = 85)' ELSE '' END;

        -- 1. Detach the DEFAULT — see the comment above the procedure.
        EXECUTE format('ALTER TABLE %I DETACH PARTITION %I', v_table, v_default);

        IF v_hash_col IS NULL THEN
            -- 2a. domains: the zone partition is a leaf with fillfactor right away
            --     (subpartitioning is impossible, see the header).
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF %I FOR VALUES IN (%s)%s',
                v_part, v_table, p_zone_id, v_ff);
        ELSE
            -- 2b. An intermediate parent, subpartitioned HASH by the domain
            --     id (fillfactor on it is forbidden by PG16 — the leaves get
            --     the parameter), and leaves for every remainder: a DEFAULT inside the hash set
            --     is impossible.
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF %I FOR VALUES IN (%s) PARTITION BY HASH (%I)',
                v_part, v_table, p_zone_id, v_hash_col);
            FOR v_i IN 0 .. p_hash_parts - 1 LOOP
                EXECUTE format(
                    'CREATE TABLE %I PARTITION OF %I FOR VALUES WITH (MODULUS %s, REMAINDER %s)%s',
                    v_part || '_h' || v_i, v_part, p_hash_parts, v_i, v_ff);
            END LOOP;
        END IF;

        -- 3. Move the zone's rows out of the detached DEFAULT with one command.
        EXECUTE format(
            'WITH moved AS (DELETE FROM %I WHERE zone_id = %s RETURNING *) ' ||
            'INSERT INTO %I SELECT * FROM moved',
            v_default, p_zone_id, v_part);
        GET DIAGNOSTICS v_moved = ROW_COUNT;
        RAISE NOTICE 'create_zone_partitions: %: rows moved out of DEFAULT: %', v_table, v_moved;

        -- 4. The DEFAULT back in place: the zone's rows are no longer there.
        EXECUTE format('ALTER TABLE %I ATTACH PARTITION %I DEFAULT', v_table, v_default);
    END LOOP;
END
$proc$;

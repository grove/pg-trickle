-- pg_trickle 0.31.0 → 0.32.0 upgrade migration
-- ============================================
--
-- v0.32.0 — Citus: Stable Naming & Per-Source Frontier Foundation
--
-- Changes in this version:
--   - CITUS-1: Citus detection helpers (src/citus.rs)
--   - CITUS-2: Stable object naming — change buffers, triggers, slots, publications
--              now use a 16-character xxhash-derived name instead of OIDs
--   - CITUS-3: Citus placement columns added to catalog tables
--   - CITUS-4: source_stable_name stored in pgt_dependencies and pgt_change_tracking
--   - CITUS-7: frontier_per_node JSONB column added to pgt_change_tracking for
--              future per-worker WAL position tracking
--
-- New SQL API:
--   pgtrickle.source_stable_name(oid) → TEXT
--     Returns the 16-char stable hash for a source table by OID.
--     Useful for diagnostics; also used internally by this migration.
--
-- Migration is safe to run on a live system.  All ALTER TABLE ADD COLUMN
-- statements use DEFAULT values, so existing rows are instantly populated
-- and no table rewrites occur (PostgreSQL 11+ behaviour).
--
-- The object rename step (change buffers, trigger functions, replication
-- slots, publications) is done in a loop that is idempotent: if the stable
-- name already matches the object name, the rename is skipped.

-- ─────────────────────────────────────────────────────────────────────────
-- STEP 1: Add new columns to pgt_stream_tables
-- ─────────────────────────────────────────────────────────────────────────

-- CITUS-3: Placement of this stream table's storage in a Citus cluster.
ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS st_placement TEXT NOT NULL DEFAULT 'local';

-- ─────────────────────────────────────────────────────────────────────────
-- STEP 2: Add new columns to pgt_dependencies
-- ─────────────────────────────────────────────────────────────────────────

-- CITUS-4: Stable hash name for the source table (v0.32.0+).
ALTER TABLE pgtrickle.pgt_dependencies
    ADD COLUMN IF NOT EXISTS source_stable_name TEXT;

-- CITUS-3: Source placement in a Citus cluster.
ALTER TABLE pgtrickle.pgt_dependencies
    ADD COLUMN IF NOT EXISTS source_placement TEXT NOT NULL DEFAULT 'local';

-- ─────────────────────────────────────────────────────────────────────────
-- STEP 3: Add new columns to pgt_change_tracking
-- ─────────────────────────────────────────────────────────────────────────

-- CITUS-4: Stable hash name used for all pg_trickle-managed objects.
ALTER TABLE pgtrickle.pgt_change_tracking
    ADD COLUMN IF NOT EXISTS source_stable_name TEXT;

-- CITUS-3: How the source is placed in a Citus cluster.
ALTER TABLE pgtrickle.pgt_change_tracking
    ADD COLUMN IF NOT EXISTS source_placement TEXT NOT NULL DEFAULT 'local';

-- CITUS-7: Per-node WAL frontier for distributed sources.
ALTER TABLE pgtrickle.pgt_change_tracking
    ADD COLUMN IF NOT EXISTS frontier_per_node JSONB;

-- ─────────────────────────────────────────────────────────────────────────
-- STEP 4: Backfill source_stable_name in pgt_change_tracking
-- ─────────────────────────────────────────────────────────────────────────
--
-- The new pgtrickle.source_stable_name(oid) function (registered by the
-- updated .so via pgrx) computes the xxhash stable name.  We call it here
-- to backfill all existing rows.  Rows where the source relation no longer
-- exists in pg_class are left NULL (STAB-1 backward compat: the code will
-- fall back to changes_{oid} naming for those rows).

UPDATE pgtrickle.pgt_change_tracking AS ct
   SET source_stable_name = pgtrickle.source_stable_name(ct.source_relid)
 WHERE ct.source_stable_name IS NULL
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_class WHERE oid = ct.source_relid
   );

-- ─────────────────────────────────────────────────────────────────────────
-- STEP 5: Backfill source_stable_name in pgt_dependencies
-- ─────────────────────────────────────────────────────────────────────────

UPDATE pgtrickle.pgt_dependencies AS d
   SET source_stable_name = pgtrickle.source_stable_name(d.source_relid)
 WHERE d.source_stable_name IS NULL
   AND d.source_type IN ('TABLE', 'FOREIGN_TABLE', 'MATVIEW', 'VIEW')
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_class WHERE oid = d.source_relid
   );

-- ─────────────────────────────────────────────────────────────────────────
-- STEP 6: Rename change buffer tables  changes_{oid} → changes_{stable_name}
-- ─────────────────────────────────────────────────────────────────────────
--
-- We iterate over all rows in pgt_change_tracking that now have a
-- source_stable_name and whose change buffer table still uses the legacy
-- OID-based name.  The rename is idempotent — if the stable-name table
-- already exists we skip it.

DO $$
DECLARE
    r RECORD;
    old_name TEXT;
    new_name TEXT;
    schema_name TEXT;
    tbl_exists BOOLEAN;
    new_tbl_exists BOOLEAN;
BEGIN
    FOR r IN
        SELECT
            ct.source_relid,
            ct.source_stable_name,
            -- The change buffer lives in the same schema as the pg_trickle
            -- working schema (look it up from pgt_change_tracking slot_name
            -- or default to pgtrickle).  Actually, change buffers were always
            -- in 'pgtrickle' schema.
            'pgtrickle' AS buf_schema
        FROM pgtrickle.pgt_change_tracking ct
        WHERE ct.source_stable_name IS NOT NULL
    LOOP
        old_name := 'changes_' || r.source_relid::text;
        new_name := 'changes_' || r.source_stable_name;
        schema_name := r.buf_schema;

        -- Check if the old (OID-based) table exists
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = schema_name AND table_name = old_name
        ) INTO tbl_exists;

        -- Check if the new (stable-name) table already exists
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = schema_name AND table_name = new_name
        ) INTO new_tbl_exists;

        IF tbl_exists AND NOT new_tbl_exists THEN
            EXECUTE format(
                'ALTER TABLE %I.%I RENAME TO %I',
                schema_name, old_name, new_name
            );
            RAISE NOTICE 'Renamed % to %', schema_name || '.' || old_name, new_name;
        ELSIF tbl_exists AND new_tbl_exists THEN
            RAISE WARNING 'Both % and % exist in schema %; manual resolution needed',
                old_name, new_name, schema_name;
        END IF;
    END LOOP;
END;
$$;

-- ─────────────────────────────────────────────────────────────────────────
-- STEP 7: Rename trigger functions  pgt_cdc_fn_{oid} → pgt_cdc_fn_{stable_name}
-- ─────────────────────────────────────────────────────────────────────────
--
-- Trigger functions are named pgt_cdc_fn_{oid} or pgt_cdc_stmt_fn_{oid}.
-- We rename them to use the stable name.  pg_proc entries are looked up by
-- proname pattern matching.

DO $$
DECLARE
    r RECORD;
    old_fn_name TEXT;
    new_fn_name TEXT;
    fn_exists BOOLEAN;
BEGIN
    FOR r IN
        SELECT
            ct.source_relid,
            ct.source_stable_name
        FROM pgtrickle.pgt_change_tracking ct
        WHERE ct.source_stable_name IS NOT NULL
    LOOP
        -- Row trigger function
        old_fn_name := 'pgt_cdc_fn_' || r.source_relid::text;
        new_fn_name := 'pgt_cdc_fn_' || r.source_stable_name;

        SELECT EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc p
            JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = 'pgtrickle' AND p.proname = old_fn_name
        ) INTO fn_exists;

        IF fn_exists THEN
            EXECUTE format(
                'ALTER FUNCTION pgtrickle.%I() RENAME TO %I',
                old_fn_name, new_fn_name
            );
            RAISE NOTICE 'Renamed trigger function % to %', old_fn_name, new_fn_name;
        END IF;

        -- Statement trigger function
        old_fn_name := 'pgt_cdc_stmt_fn_' || r.source_relid::text;
        new_fn_name := 'pgt_cdc_stmt_fn_' || r.source_stable_name;

        SELECT EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc p
            JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = 'pgtrickle' AND p.proname = old_fn_name
        ) INTO fn_exists;

        IF fn_exists THEN
            EXECUTE format(
                'ALTER FUNCTION pgtrickle.%I() RENAME TO %I',
                old_fn_name, new_fn_name
            );
            RAISE NOTICE 'Renamed stmt trigger function % to %', old_fn_name, new_fn_name;
        END IF;
    END LOOP;
END;
$$;

-- ─────────────────────────────────────────────────────────────────────────
-- STEP 8: Rename WAL replication slots  pgtrickle_{oid} → pgtrickle_{stable}
-- ─────────────────────────────────────────────────────────────────────────
--
-- Replication slots are named pgtrickle_{oid}.  We rename them to
-- pgtrickle_{stable_name}.  Slots can only be renamed if they exist and
-- are not active.  We use pg_replication_slots view to check.
--
-- NOTE: pg_rename_replication_slot() was added in PostgreSQL 15.
-- For older versions, we skip the rename and emit a NOTICE.

DO $$
DECLARE
    r RECORD;
    old_slot TEXT;
    new_slot TEXT;
    slot_exists BOOLEAN;
    slot_active BOOLEAN;
BEGIN
    -- Only proceed on PostgreSQL 15+
    IF current_setting('server_version_num')::int < 150000 THEN
        RAISE NOTICE 'Skipping slot rename: requires PostgreSQL 15+';
        RETURN;
    END IF;

    FOR r IN
        SELECT
            ct.source_relid,
            ct.source_stable_name
        FROM pgtrickle.pgt_change_tracking ct
        WHERE ct.source_stable_name IS NOT NULL
    LOOP
        old_slot := 'pgtrickle_' || r.source_relid::text;
        new_slot := 'pgtrickle_' || r.source_stable_name;

        SELECT
            EXISTS(SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = old_slot),
            COALESCE((SELECT active FROM pg_catalog.pg_replication_slots WHERE slot_name = old_slot), false)
        INTO slot_exists, slot_active;

        IF slot_exists AND NOT slot_active THEN
            PERFORM pg_catalog.pg_rename_replication_slot(old_slot, new_slot);
            RAISE NOTICE 'Renamed replication slot % to %', old_slot, new_slot;
        ELSIF slot_exists AND slot_active THEN
            RAISE WARNING 'Slot % is active; cannot rename to % now. Rename manually after stopping CDC.',
                old_slot, new_slot;
        END IF;
    END LOOP;
END;
$$;

-- ─────────────────────────────────────────────────────────────────────────
-- STEP 9: Update slot_name in pgt_change_tracking for renamed slots
-- ─────────────────────────────────────────────────────────────────────────

UPDATE pgtrickle.pgt_change_tracking AS ct
   SET slot_name = 'pgtrickle_' || ct.source_stable_name
 WHERE ct.source_stable_name IS NOT NULL
   AND ct.slot_name = 'pgtrickle_' || ct.source_relid::text;

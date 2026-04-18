-- pg_trickle 0.20.0 → 0.21.0 upgrade migration
-- ============================================
--
-- ARCH-2: Add refresh_reason column to pgt_refresh_history.
--   Stores a machine-readable tag when the executor takes a non-default path.
--   Currently emitted values:
--     'recursive_cte_fallback' — non-monotone recursive CTE forced recomputation
--   NULL means the default execution path was taken.
--
-- OP-3: Add pause_all() / resume_all() helpers.
-- OP-4: Add refresh_if_stale(name, max_age) helper.
-- OP-5: Add stream_table_definition(name) helper.

-- ── ARCH-2: refresh_reason column ──────────────────────────────────────────

ALTER TABLE pgtrickle.pgt_refresh_history
    ADD COLUMN IF NOT EXISTS refresh_reason text DEFAULT NULL;

COMMENT ON COLUMN pgtrickle.pgt_refresh_history.refresh_reason IS
    'Machine-readable tag explaining why a non-default refresh strategy was '
    'selected. NULL = default path. Example value: ''recursive_cte_fallback''.';

-- ── OP-3: pause_all() / resume_all() ───────────────────────────────────────

CREATE OR REPLACE FUNCTION pgtrickle."pause_all"()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE pgtrickle.pgt_stream_tables
       SET status = 'PAUSED'
     WHERE status = 'ACTIVE';
    RAISE NOTICE 'pg_trickle: all stream tables paused.';
END;
$$;

COMMENT ON FUNCTION pgtrickle."pause_all"() IS
    'Pause automatic refreshes for every ACTIVE stream table. '
    'Use pgtrickle.resume_all() to re-activate them.';

CREATE OR REPLACE FUNCTION pgtrickle."resume_all"()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE pgtrickle.pgt_stream_tables
       SET status = 'ACTIVE'
     WHERE status = 'PAUSED';
    RAISE NOTICE 'pg_trickle: all paused stream tables resumed.';
END;
$$;

COMMENT ON FUNCTION pgtrickle."resume_all"() IS
    'Re-activate all stream tables that were paused with pgtrickle.pause_all().';

-- ── OP-4: refresh_if_stale(name, max_age) ──────────────────────────────────

CREATE OR REPLACE FUNCTION pgtrickle."refresh_if_stale"(
    p_name   text,
    p_max_age interval DEFAULT '5 minutes'::interval
)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    v_last_end timestamp with time zone;
    v_refreshed boolean := false;
BEGIN
    SELECT MAX(end_time)
      INTO v_last_end
      FROM pgtrickle.pgt_refresh_history h
      JOIN pgtrickle.pgt_stream_tables   s USING (pgt_id)
     WHERE s.pgt_name = p_name
       AND h.status = 'COMPLETED';

    IF v_last_end IS NULL OR (now() - v_last_end) > p_max_age THEN
        PERFORM pgtrickle.refresh_stream_table(p_name);
        v_refreshed := true;
    END IF;

    RETURN v_refreshed;
END;
$$;

COMMENT ON FUNCTION pgtrickle."refresh_if_stale"(text, interval) IS
    'Refresh the named stream table only when the most recent completed '
    'refresh is older than max_age.  Returns TRUE when a refresh was '
    'triggered, FALSE when the table was fresh enough.';

-- ── OP-5: stream_table_definition(name) ─────────────────────────────────────
-- Ergonomic alias for pgtrickle.export_definition() — same implementation,
-- easier to discover under the "definition" terminology.

CREATE OR REPLACE FUNCTION pgtrickle."stream_table_definition"(
    p_name text
)
RETURNS text
LANGUAGE sql
STABLE
AS $$
    SELECT pgtrickle.export_definition(p_name);
$$;

COMMENT ON FUNCTION pgtrickle."stream_table_definition"(text) IS
    'Return the CREATE STREAM TABLE DDL for the named stream table. '
    'Equivalent to pgtrickle.export_definition(name) — provided as a '
    'more discoverable alias.';

-- ── OPS-1: Shadow/canary mode for alter_stream_table ────────────────────────
--
-- Adds `pgtrickle.canary_begin(name, new_query)`, `pgtrickle.canary_diff(name)`,
-- and `pgtrickle.canary_promote(name)` functions.
--
-- Usage:
--   1. SELECT pgtrickle.canary_begin('myschema.orders_mv', 'SELECT …');
--      → Creates __pgt_canary_orders_mv alongside the live table and starts
--        populating it using the new query.
--   2. SELECT * FROM pgtrickle.canary_diff('myschema.orders_mv');
--      → Returns rows present in only one of live / canary.
--   3. SELECT pgtrickle.canary_promote('myschema.orders_mv');
--      → Swaps canary into production via ALTER STREAM TABLE, drops __pgt_canary_*.
--
-- The canary table is a regular stream table named __pgt_canary_<original_name>
-- in the same schema.  It is never visible via SELECT * (column exclusions apply)
-- and is explicitly excluded from dog-feeding analytics.

CREATE OR REPLACE FUNCTION pgtrickle."canary_begin"(
    p_name      text,
    p_new_query text
)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema  text;
    v_table   text;
    v_canary  text;
    v_dot     int;
BEGIN
    v_dot    := strpos(p_name, '.');
    IF v_dot > 0 THEN
        v_schema := substr(p_name, 1, v_dot - 1);
        v_table  := substr(p_name, v_dot + 1);
    ELSE
        v_schema := current_schema();
        v_table  := p_name;
    END IF;

    v_canary := '__pgt_canary_' || v_table;

    -- Drop any stale canary table from a previous run.
    BEGIN
        PERFORM pgtrickle.drop_stream_table(v_schema || '.' || v_canary);
    EXCEPTION WHEN OTHERS THEN
        NULL;  -- ignore if it does not exist
    END;

    -- Create the canary stream table with the new query.
    PERFORM pgtrickle.create_stream_table(
        v_schema || '.' || v_canary,
        p_new_query
    );

    RETURN format(
        'Canary stream table %I.%I created. Run pgtrickle.canary_diff(%L) to compare.',
        v_schema, v_canary, p_name
    );
END;
$$;

COMMENT ON FUNCTION pgtrickle."canary_begin"(text, text) IS
    'Start a shadow/canary test for the named stream table. '
    'Creates __pgt_canary_<name> with p_new_query and starts refreshing it. '
    'Use canary_diff(name) to inspect differences and canary_promote(name) to '
    'swap canary into production.';

CREATE OR REPLACE FUNCTION pgtrickle."canary_diff"(
    p_name text
)
RETURNS TABLE(
    row_source text,
    diff_row   text
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema  text;
    v_table   text;
    v_canary  text;
    v_dot     int;
    v_sql     text;
BEGIN
    v_dot    := strpos(p_name, '.');
    IF v_dot > 0 THEN
        v_schema := substr(p_name, 1, v_dot - 1);
        v_table  := substr(p_name, v_dot + 1);
    ELSE
        v_schema := current_schema();
        v_table  := p_name;
    END IF;

    v_canary := '__pgt_canary_' || v_table;

    -- Return rows in live-only vs canary-only using EXCEPT (symmetric difference).
    v_sql := format(
        '(SELECT %L AS row_source, t::text AS diff_row FROM %I.%I t EXCEPT
          SELECT %L, c::text FROM %I.%I c)
         UNION ALL
         (SELECT %L, c::text FROM %I.%I c EXCEPT
          SELECT %L, t::text FROM %I.%I t)',
        'live_only',   v_schema, v_table,
        'canary_only', v_schema, v_canary,
        'canary_only', v_schema, v_canary,
        'live_only',   v_schema, v_table
    );
    RETURN QUERY EXECUTE v_sql;
END;
$$;

COMMENT ON FUNCTION pgtrickle."canary_diff"(text) IS
    'Compare the live stream table with its canary counterpart. '
    'Returns rows that exist in only one of the two tables. '
    'An empty result set indicates the new query produces the same output.';

CREATE OR REPLACE FUNCTION pgtrickle."canary_promote"(
    p_name text
)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema    text;
    v_table     text;
    v_canary    text;
    v_dot       int;
    v_new_query text;
BEGIN
    v_dot    := strpos(p_name, '.');
    IF v_dot > 0 THEN
        v_schema := substr(p_name, 1, v_dot - 1);
        v_table  := substr(p_name, v_dot + 1);
    ELSE
        v_schema := current_schema();
        v_table  := p_name;
    END IF;

    v_canary := '__pgt_canary_' || v_table;

    -- Read the defining query from the canary table.
    SELECT defining_query
      INTO v_new_query
      FROM pgtrickle.pgt_stream_tables
     WHERE pgt_schema = v_schema
       AND pgt_name   = v_canary;

    IF v_new_query IS NULL THEN
        RAISE EXCEPTION 'No canary found for %. Run pgtrickle.canary_begin() first.', p_name;
    END IF;

    -- Promote: alter the live table to use the new query, then drop the canary.
    PERFORM pgtrickle.alter_stream_table(v_schema || '.' || v_table, query => v_new_query);

    BEGIN
        PERFORM pgtrickle.drop_stream_table(v_schema || '.' || v_canary);
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    RETURN format(
        'Canary promoted: %I.%I now uses the canary query. Canary table dropped.',
        v_schema, v_table
    );
END;
$$;

COMMENT ON FUNCTION pgtrickle."canary_promote"(text) IS
    'Promote the canary stream table to production. '
    'Calls ALTER STREAM TABLE with the canary query, then drops the canary table. '
    'Run pgtrickle.canary_diff(name) first to confirm the result set matches.';

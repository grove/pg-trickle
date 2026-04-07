-- pg_trickle Post-Install Verification Script
-- =============================================
-- Run with: psql -f scripts/verify_install.sql
--
-- This script verifies that pg_trickle is correctly installed and configured.
-- Every check prints PASS or FAIL. A fully working installation should show
-- all PASS lines and zero FAIL lines.
--
-- Prerequisites: connect to a database where you want to use pg_trickle.

\set ON_ERROR_STOP off
\pset tuples_only on
\pset format unaligned

-- 1. Check that pg_trickle is in shared_preload_libraries
SELECT CASE
    WHEN current_setting('shared_preload_libraries') ILIKE '%pg_trickle%'
    THEN 'PASS: pg_trickle is in shared_preload_libraries'
    ELSE 'FAIL: pg_trickle is NOT in shared_preload_libraries — add it to postgresql.conf and restart'
END;

-- 2. Check extension can be created / already exists
DO $$
BEGIN
    CREATE EXTENSION IF NOT EXISTS pg_trickle;
    RAISE NOTICE 'PASS: pg_trickle extension is installed (version %)',
        (SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle');
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'FAIL: Cannot create pg_trickle extension — %', SQLERRM;
END;
$$;

-- 3. Check extension version
SELECT 'PASS: pg_trickle version = ' || pgtrickle.version();

-- 4. Check that the pgtrickle schema exists
SELECT CASE
    WHEN EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'pgtrickle')
    THEN 'PASS: pgtrickle schema exists'
    ELSE 'FAIL: pgtrickle schema does not exist'
END;

-- 5. Check that the pgtrickle_changes schema exists
SELECT CASE
    WHEN EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'pgtrickle_changes')
    THEN 'PASS: pgtrickle_changes schema exists'
    ELSE 'FAIL: pgtrickle_changes schema does not exist'
END;

-- 6. Check that the background scheduler is running
SELECT CASE
    WHEN EXISTS (
        SELECT 1 FROM pg_stat_activity
        WHERE backend_type = 'pg_trickle scheduler'
           OR application_name LIKE 'pg_trickle%'
    )
    THEN 'PASS: pg_trickle scheduler is running'
    ELSE 'FAIL: pg_trickle scheduler is NOT running — check pg_trickle.enabled GUC and max_worker_processes'
END;

-- 7. Check max_worker_processes is adequate
SELECT CASE
    WHEN current_setting('max_worker_processes')::int >= 16
    THEN 'PASS: max_worker_processes = ' || current_setting('max_worker_processes') || ' (>= 16)'
    ELSE 'FAIL: max_worker_processes = ' || current_setting('max_worker_processes') || ' — recommend >= 16 (32+ for production)'
END;

-- 8. Check pg_trickle.enabled GUC
SELECT CASE
    WHEN current_setting('pg_trickle.enabled', true) = 'on'
    THEN 'PASS: pg_trickle.enabled = on'
    WHEN current_setting('pg_trickle.enabled', true) IS NULL
    THEN 'FAIL: pg_trickle.enabled GUC not found — is shared_preload_libraries configured?'
    ELSE 'FAIL: pg_trickle.enabled = ' || current_setting('pg_trickle.enabled', true) || ' — set to on'
END;

-- 9. Create a test table, stream table, verify refresh, then clean up
DO $$
DECLARE
    v_count bigint;
BEGIN
    -- Create test source table
    CREATE TABLE IF NOT EXISTS _pgt_verify_src (id serial PRIMARY KEY, val int);
    INSERT INTO _pgt_verify_src (val) VALUES (1), (2), (3);

    -- Create a test stream table
    PERFORM pgtrickle.create_stream_table(
        name     => '_pgt_verify_st',
        query    => 'SELECT count(*) AS total FROM _pgt_verify_src',
        schedule => '1h'
    );

    -- Force a manual refresh
    PERFORM pgtrickle.refresh_stream_table('_pgt_verify_st');

    -- Verify the stream table has data
    EXECUTE 'SELECT total FROM _pgt_verify_st' INTO v_count;
    IF v_count = 3 THEN
        RAISE NOTICE 'PASS: Test stream table created, refreshed, and returned correct result (count=3)';
    ELSE
        RAISE NOTICE 'FAIL: Test stream table returned unexpected count=%', v_count;
    END IF;

    -- Clean up
    PERFORM pgtrickle.drop_stream_table('_pgt_verify_st');
    DROP TABLE IF EXISTS _pgt_verify_src;
    RAISE NOTICE 'PASS: Cleanup complete — test objects removed';

EXCEPTION WHEN OTHERS THEN
    -- Clean up on failure
    BEGIN
        PERFORM pgtrickle.drop_stream_table('_pgt_verify_st');
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
    DROP TABLE IF EXISTS _pgt_verify_src;
    RAISE NOTICE 'FAIL: End-to-end test failed — %', SQLERRM;
END;
$$;

-- Summary
SELECT '';
SELECT '=== Verification complete. Check for any FAIL lines above. ===';

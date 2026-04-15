-- pg_trickle 0.19.0 → 0.20.0 upgrade migration
-- ============================================
--
-- PERF-1: Add index on pgt_refresh_history(pgt_id, start_time) for dog-feeding queries.
-- DF-G3: Add 'DOG_FEED' to initiated_by CHECK constraint.

-- ── PERF-1: Index on (pgt_id, start_time) ──────────────────────────────────
-- Required by all five dog-feeding stream tables which filter on
-- start_time > now() - interval '1 hour' grouped by pgt_id.
CREATE INDEX IF NOT EXISTS idx_hist_pgt_start
    ON pgtrickle.pgt_refresh_history (pgt_id, start_time);

-- ── DF-G3: Extend initiated_by CHECK to include 'DOG_FEED' ────────────────
-- The auto-apply worker logs threshold changes with initiated_by = 'DOG_FEED'.
DO $$
BEGIN
    -- Drop existing CHECK constraint on initiated_by column.
    EXECUTE (
        SELECT format('ALTER TABLE pgtrickle.pgt_refresh_history DROP CONSTRAINT %I',
                       conname)
        FROM pg_constraint
        WHERE conrelid = 'pgtrickle.pgt_refresh_history'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) ILIKE '%initiated_by%'
        LIMIT 1
    );
EXCEPTION WHEN undefined_table OR undefined_object THEN
    NULL;
END $$;

ALTER TABLE pgtrickle.pgt_refresh_history
    ADD CONSTRAINT pgt_refresh_history_initiated_by_check
    CHECK (initiated_by IN ('SCHEDULER', 'MANUAL', 'INITIAL', 'DOG_FEED'));
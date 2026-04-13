-- pg_trickle 0.18.0 → 0.19.0 upgrade migration
-- ============================================
--
-- DB-1: Fix duplicate 'DIFFERENTIAL' in pgt_refresh_history.action CHECK.
-- DB-2: Add ON DELETE CASCADE FK on pgt_refresh_history.pgt_id.
-- DB-3: Create pgt_schema_version tracking table.
-- DB-4: Rename pgtrickle_refresh NOTIFY channel → pg_trickle_refresh.
--        (Channel rename is in Rust code; this script documents the change.)

-- ── DB-1: Fix duplicate 'DIFFERENTIAL' in CHECK constraint ─────────────────
-- The archive schema (≤ 0.11.0) had 'DIFFERENTIAL' listed twice in the
-- pgt_refresh_history.action CHECK constraint.  Drop and re-create it.
DO $$
BEGIN
    -- Drop existing CHECK constraint on action column (name varies).
    EXECUTE (
        SELECT format('ALTER TABLE pgtrickle.pgt_refresh_history DROP CONSTRAINT %I',
                       conname)
        FROM pg_constraint
        WHERE conrelid = 'pgtrickle.pgt_refresh_history'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) ILIKE '%action%'
        LIMIT 1
    );
EXCEPTION WHEN undefined_table OR undefined_object THEN
    -- Table or constraint doesn't exist yet — fresh install, nothing to fix.
    NULL;
END $$;

ALTER TABLE pgtrickle.pgt_refresh_history
    ADD CONSTRAINT pgt_refresh_history_action_check
    CHECK (action IN ('NO_DATA', 'FULL', 'DIFFERENTIAL', 'REINITIALIZE', 'SKIP'));

-- ── DB-2: Add ON DELETE CASCADE FK on pgt_refresh_history.pgt_id ───────────
-- Guard: only add if the FK doesn't already exist.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'pgtrickle.pgt_refresh_history'::regclass
          AND contype = 'f'
          AND pg_get_constraintdef(oid) ILIKE '%pgt_stream_tables%'
    ) THEN
        ALTER TABLE pgtrickle.pgt_refresh_history
            ADD CONSTRAINT fk_pgt_refresh_history_pgt_id
            FOREIGN KEY (pgt_id)
            REFERENCES pgtrickle.pgt_stream_tables(pgt_id)
            ON DELETE CASCADE;
    END IF;
END $$;

-- ── DB-3: Create pgt_schema_version tracking table ─────────────────────────
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_schema_version (
    version     TEXT PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    description TEXT
);

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES ('0.19.0', 'Production gap closure & distribution')
ON CONFLICT (version) DO NOTHING;

-- ── DB-4: NOTIFY channel rename ────────────────────────────────────────────
-- The Rust code now emits NOTIFY pg_trickle_refresh (was pgtrickle_refresh).
-- No SQL migration needed; this comment documents the breaking change.
-- Applications using LISTEN pgtrickle_refresh must switch to
-- LISTEN pg_trickle_refresh.

-- ── CORR-1: delete_insert merge strategy removal ──────────────────────────
-- The 'delete_insert' value for pg_trickle.merge_strategy GUC is no longer
-- accepted.  If it was set in postgresql.conf, the extension now logs a
-- WARNING and falls back to 'auto'.  No SQL migration needed.

-- ── PERF-4: Catalog indexes for scheduler hot path ────────────────────────
CREATE INDEX IF NOT EXISTS idx_pgt_relid
    ON pgtrickle.pgt_stream_tables (pgt_relid);
CREATE INDEX IF NOT EXISTS idx_deps_pgt_id
    ON pgtrickle.pgt_dependencies (pgt_id);

-- pg_trickle 0.21.0 → 0.22.0 upgrade migration
-- ============================================
--
-- v0.22.0 — Production Scalability & Downstream Integration
--
-- CDC-PUB: Downstream CDC publication columns + SQL functions
-- PAR-2: max_parallel_workers GUC (config-only, no schema change)
-- PRED: Predictive cost model (Rust-side logic, no schema change)
-- SLA: freshness_deadline_ms column + set_stream_table_sla() function

-- ── Performance indexes (added in v0.22.0 base DDL) ────────────────────────

CREATE INDEX IF NOT EXISTS idx_pgt_relid ON pgtrickle.pgt_stream_tables (pgt_relid);
CREATE INDEX IF NOT EXISTS idx_deps_pgt_id ON pgtrickle.pgt_dependencies (pgt_id);

-- ── Schema version tracking table ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS pgtrickle.pgt_schema_version (
    version     TEXT PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    description TEXT
);
INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES ('0.22.0', 'Downstream CDC publication, parallel refresh cap, predictive cost model, SLA tiers')
ON CONFLICT (version) DO NOTHING;

-- ── FK constraint on pgt_refresh_history.pgt_id ────────────────────────────

ALTER TABLE pgtrickle.pgt_refresh_history
    DROP CONSTRAINT IF EXISTS pgt_refresh_history_pgt_id_fkey;
ALTER TABLE pgtrickle.pgt_refresh_history
    ADD CONSTRAINT pgt_refresh_history_pgt_id_fkey
    FOREIGN KEY (pgt_id)
    REFERENCES pgtrickle.pgt_stream_tables(pgt_id)
    ON DELETE CASCADE;

-- ── CDC-PUB-1/3: downstream_publication_name column ────────────────────────

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS downstream_publication_name text DEFAULT NULL;

COMMENT ON COLUMN pgtrickle.pgt_stream_tables.downstream_publication_name IS
    'Name of the downstream logical replication publication created by '
    'pgtrickle.stream_table_to_publication(). NULL when no publication exists.';

-- ── SLA-1: freshness_deadline_ms column ────────────────────────────────────

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS freshness_deadline_ms bigint DEFAULT NULL;

COMMENT ON COLUMN pgtrickle.pgt_stream_tables.freshness_deadline_ms IS
    'SLA freshness deadline in milliseconds. Set via '
    'pgtrickle.set_stream_table_sla(name, interval). The scheduler uses this '
    'to auto-assign the appropriate refresh tier (SLA-2/SLA-3).';

-- ── CDC-PUB-1: stream_table_to_publication() SQL function ──────────────────

CREATE OR REPLACE FUNCTION pgtrickle."stream_table_to_publication"(
    "name" TEXT
) RETURNS void
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'stream_table_to_publication_wrapper';

-- ── CDC-PUB-2: drop_stream_table_publication() SQL function ────────────────

CREATE OR REPLACE FUNCTION pgtrickle."drop_stream_table_publication"(
    "name" TEXT
) RETURNS void
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'drop_stream_table_publication_wrapper';

-- ── SLA-1: set_stream_table_sla() SQL function ─────────────────────────────

CREATE OR REPLACE FUNCTION pgtrickle."set_stream_table_sla"(
    "name" TEXT,
    "sla" interval
) RETURNS void
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'set_stream_table_sla_wrapper';


-- pg_trickle 0.21.0 → 0.22.0 upgrade migration
-- ============================================
--
-- v0.22.0 — Production Scalability & Downstream Integration
--
-- CDC-PUB: Downstream CDC publication columns
-- PAR-2: max_parallel_workers GUC (config-only, no schema change)
-- PRED: Predictive cost model (Rust-side logic, no schema change)
-- SLA: freshness_deadline_ms column for SLA-driven tier auto-assignment

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

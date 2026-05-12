-- pg_trickle 0.58.0 -> 0.59.0 upgrade migration
--
-- v0.59.0 — Performance & Observability
--
-- PERF-2: Add defining_query_hash column to cache per-stream-table SQL
--         hashes in the catalog, eliminating re-hashing on every refresh.
--         Existing rows receive hash=0; the Rust layer falls back to a one-time
--         recompute on first access and writes the correct value on next ALTER.
--
-- All other v0.59.0 changes (PERF-1,3-7; OBS-1-6) are pure Rust and require
-- no SQL schema migration.

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS defining_query_hash BIGINT NOT NULL DEFAULT 0;

COMMENT ON COLUMN pgtrickle.pgt_stream_tables.defining_query_hash IS
    'Hash of the defining_query string, cached in the catalog to avoid '
    'recomputing it on every refresh cycle (0 = not yet computed).';

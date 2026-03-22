-- pg_trickle 0.9.0 -> 0.10.0 upgrade script
--
-- PB2: Add pooler_compatibility_mode column to pgt_stream_tables.
-- When true, prepared statements and NOTIFY emissions are suppressed
-- for this stream table, enabling PgBouncer transaction-mode compatibility.

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS pooler_compatibility_mode BOOLEAN NOT NULL DEFAULT FALSE;

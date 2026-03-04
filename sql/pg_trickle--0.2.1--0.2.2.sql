-- pg_trickle 0.2.1 → 0.2.2 upgrade script
--
-- EC-16: Add function_hashes column to pgt_stream_tables.
-- Stores the last-seen md5(prosrc) hash for each function referenced in
-- the defining query (functions_used array).  On each differential refresh
-- cycle the scheduler recomputes these hashes and, when any hash changes,
-- forces a full refresh and logs a NOTICE — detecting silent ALTER FUNCTION
-- body changes that would otherwise go undetected until a manual refresh.
ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS function_hashes TEXT;

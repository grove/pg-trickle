-- pg_trickle 0.5.0 -> 0.6.0 upgrade script
--
-- v0.6.0 adds:
--   Idempotent DDL: create_or_replace_stream_table() — declarative
--   stream table deployment that creates, no-ops, or alters as needed.

-- Idempotent DDL: create_or_replace_stream_table()
CREATE FUNCTION pgtrickle."create_or_replace_stream_table"(
    "name"                    TEXT,
    "query"                   TEXT,
    "schedule"                TEXT    DEFAULT 'calculated',
    "refresh_mode"            TEXT    DEFAULT 'AUTO',
    "initialize"              bool    DEFAULT true,
    "diamond_consistency"     TEXT    DEFAULT NULL,
    "diamond_schedule_policy" TEXT    DEFAULT NULL,
    "cdc_mode"                TEXT    DEFAULT NULL,
    "append_only"             bool    DEFAULT false
) RETURNS void
LANGUAGE c
AS 'MODULE_PATHNAME', 'create_or_replace_stream_table_wrapper';

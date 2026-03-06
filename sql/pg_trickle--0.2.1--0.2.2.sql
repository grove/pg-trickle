-- pg_trickle 0.2.1 → 0.2.2 upgrade script
--
-- AUTO REFRESH MODE: The default refresh_mode for create_stream_table changes
-- from 'DIFFERENTIAL' to 'AUTO'. AUTO uses differential maintenance when the
-- query supports it and automatically falls back to FULL otherwise.
--
-- The schedule default also changes from '1m' to 'calculated'.
--
-- Since PostgreSQL stores function defaults in pg_proc, we must DROP the old
-- signature and CREATE the new one to update the default values.

-- Drop the old create_stream_table with 'DIFFERENTIAL' / '1m' defaults.
DROP FUNCTION IF EXISTS pgtrickle."create_stream_table"(text, text, text, text, bool, text, text);

-- Create the new signature with 'AUTO' / 'calculated' defaults.
CREATE FUNCTION pgtrickle."create_stream_table"(
	"name" TEXT, /* &str */
	"query" TEXT, /* &str */
	"schedule" TEXT DEFAULT 'calculated', /* core::option::Option<&str> */
	"refresh_mode" TEXT DEFAULT 'AUTO', /* &str */
	"initialize" bool DEFAULT true, /* bool */
	"diamond_consistency" TEXT DEFAULT NULL, /* core::option::Option<&str> */
	"diamond_schedule_policy" TEXT DEFAULT NULL /* core::option::Option<&str> */
) RETURNS void
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'create_stream_table_wrapper';

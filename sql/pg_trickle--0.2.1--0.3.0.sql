-- pg_trickle 0.2.1 → 0.3.0 upgrade script
--
-- ALTER QUERY: Add `query` parameter to alter_stream_table().
-- The function signature changes from 6 to 7 parameters (adding `query`
-- as the second parameter with DEFAULT NULL), so we must DROP the old
-- overload and CREATE the new one.  The C entry point name is unchanged.

-- Drop the old 6-parameter signature.
DROP FUNCTION IF EXISTS pgtrickle."alter_stream_table"(text, text, text, text, text, text);

-- Create the new 7-parameter signature (matches pgrx-generated SQL).
CREATE FUNCTION pgtrickle."alter_stream_table"(
	"name" TEXT, /* &str */
	"query" TEXT DEFAULT NULL, /* core::option::Option<&str> */
	"schedule" TEXT DEFAULT NULL, /* core::option::Option<&str> */
	"refresh_mode" TEXT DEFAULT NULL, /* core::option::Option<&str> */
	"status" TEXT DEFAULT NULL, /* core::option::Option<&str> */
	"diamond_consistency" TEXT DEFAULT NULL, /* core::option::Option<&str> */
	"diamond_schedule_policy" TEXT DEFAULT NULL /* core::option::Option<&str> */
) RETURNS void
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'alter_stream_table_wrapper';

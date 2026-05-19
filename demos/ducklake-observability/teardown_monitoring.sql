-- DuckLake Observability — Teardown Script
-- Removes all stream tables created by init_monitoring.sql.
-- Run this to cleanly remove the monitoring objects from your catalog database.

SELECT pgtrickle.drop_stream_table('ducklake_small_file_counts');
SELECT pgtrickle.drop_stream_table('ducklake_snapshot_rate');
SELECT pgtrickle.drop_stream_table('ducklake_storage_growth');
SELECT pgtrickle.drop_stream_table('ducklake_tenant_activity');
SELECT pgtrickle.drop_stream_table('ducklake_compaction_events');

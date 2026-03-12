-- No stream table managed by this project should have consecutive errors.
-- Returns failing stream tables. Empty result = pass.
SELECT pgt_name, consecutive_errors
FROM pgtrickle.pgt_stream_tables
WHERE consecutive_errors > 0
  AND pgt_name IN (
      'public.department_tree',
      'public.department_stats',
      'public.department_report'
  )

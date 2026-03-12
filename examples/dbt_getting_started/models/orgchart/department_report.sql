{{ config(
    materialized  = 'stream_table',
    schedule      = '1m',
    refresh_mode  = 'DIFFERENTIAL'
) }}

SELECT
    split_part(full_path, ' > ', 2)     AS division,
    SUM(headcount)                      AS total_headcount,
    SUM(total_salary)                   AS total_payroll
FROM {{ ref('department_stats') }}
WHERE depth >= 1
GROUP BY 1
